"""Tests for the stuck-'summarizing' branch of cleanup_stale_background_chats (SC-2633).

When generate_incident_summary_from_chat is consumed from the broker without
executing (worker died after dequeue), the incident stays in aurora_status
'summarizing' forever: sections 1-3 of the cleaner cannot see it (the RCA task
id is cleared before 'summarizing' is set). This branch re-enqueues the
summary task, bumps updated_at so the next sweep does not double-enqueue, and
records a 'summary_requeued' lifecycle event; after max retries (or with no
chat session to summarize from) it marks the incident error.

DB, Celery delay and notifications are all mocked — no I/O.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# In the lightweight CI env celery is stubbed by conftest, so @celery_app.task
# replaced the module functions with MagicMocks — and conftest's import chain
# already pulled this module in before any test can patch the decorator.
# Restore plain functions: patch the decorator to identity and reload.
import importlib

import celery_config

if isinstance(celery_config.celery_app.task, MagicMock):
    _orig_decorator = celery_config.celery_app.task
    celery_config.celery_app.task = lambda *a, **kw: (lambda f: f)
    import chat.background.summarization
    importlib.reload(chat.background.summarization)
    import chat.background.task
    importlib.reload(chat.background.task)
    # Restore the stub decorator: other test modules (tasks/*) rely on the
    # MagicMock celery task objects their fixtures monkeypatch .delay on.
    celery_config.celery_app.task = _orig_decorator

import chat.background.summarization as summarization
import chat.background.task as bg_task

_USER = "user-1"
_INC = "11111111-1111-1111-1111-111111111111"
_SESSION = "22222222-2222-2222-2222-222222222222"


class FakeCursor:
    """Answers each cleanup query by the first matching SQL substring."""

    def __init__(self, results):
        self._results = list(results)
        self.executed = []
        self.rowcount = 0
        self._rows = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        for pattern, rows in self._results:
            if pattern in sql:
                self._rows = rows
                self.rowcount = len(rows)
                return
        self._rows = []
        self.rowcount = 0

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def sql_containing(self, pattern):
        return [(s, p) for s, p in self.executed if pattern in s]


def _run_cleanup(cursor_results):
    cursor = FakeCursor(cursor_results)
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    pool = MagicMock()
    pool.get_admin_connection.return_value.__enter__.return_value = conn

    summary_task = MagicMock()
    with patch.object(bg_task, "db_pool", pool), \
            patch.object(bg_task, "_affected_users", return_value=[_USER]), \
            patch.object(bg_task, "set_rls_context", return_value=True), \
            patch.object(bg_task, "get_org_id_for_user", return_value="org-1"), \
            patch("utils.auth.stateless_auth.get_org_id_for_user", return_value="org-1"), \
            patch.object(summarization, "generate_incident_summary_from_chat", summary_task), \
            patch("utils.notifications.dispatcher.notify_investigation_failed"):
        result = bg_task.cleanup_stale_background_chats()
    return result, cursor, summary_task


# Queries from the untouched sections answer empty so only the summarizing
# branch does observable work.
_BASE_RESULTS = [
    ("cs.status = 'in_progress' AND cs.updated_at", []),
    ("aurora_status = 'running'", []),
    ("status = 'investigating'", []),
    ("UPDATE rca_findings", []),
    ("SELECT DISTINCT org_id FROM users", []),
]


def _summarizing_rows(rows, extra=()):
    # `extra` goes BEFORE the summarizing pattern: the error-branch UPDATE also
    # contains "aurora_status = 'summarizing'" (its WHERE guard), and matching
    # is first-pattern-wins.
    return _BASE_RESULTS + list(extra) + [("aurora_status = 'summarizing'", rows)]


class TestRequeue:
    def test_stuck_incident_is_requeued(self):
        results = _summarizing_rows([(_INC, _SESSION)]) + [
            ("event_type = 'summary_requeued'", [(0,)]),
        ]
        result, cursor, summary_task = _run_cleanup(results)

        summary_task.delay.assert_called_once_with(
            incident_id=_INC, user_id=_USER, session_id=_SESSION,
        )
        assert result["summary_requeued"] == 1
        assert result["summary_errors"] == 0
        # updated_at bumped so the next sweep (5 min) does not double-enqueue.
        assert cursor.sql_containing("SET updated_at = NOW() WHERE id = %s AND aurora_status = 'summarizing'")
        # Retry counter + telemetry: a lifecycle event per re-enqueue (the
        # event type travels in the params — the INSERT statement is generic).
        inserts = cursor.sql_containing("INSERT INTO incident_lifecycle_events")
        assert any(p and "summary_requeued" in p for _, p in inserts)

    def test_select_is_bounded_per_run(self):
        results = _summarizing_rows([(_INC, _SESSION)]) + [
            ("event_type = 'summary_requeued'", [(0,)]),
        ]
        _, cursor, _ = _run_cleanup(results)

        selects = cursor.sql_containing("i.aurora_status = 'summarizing' AND i.status != 'merged'")
        assert len(selects) == 1
        sql, params = selects[0]
        assert "LIMIT" in sql
        assert params[-1] == 5  # re-enqueue budget per cleanup run

    def test_budget_exhausted_stops_querying(self):
        rows = [(f"11111111-1111-1111-1111-11111111111{i}", _SESSION) for i in range(5)]
        results = _summarizing_rows(rows) + [
            ("event_type = 'summary_requeued'", [(0,)]),
        ]
        result, cursor, summary_task = _run_cleanup(results)

        assert summary_task.delay.call_count == 5
        assert result["summary_requeued"] == 5
        # Budget spent: no second SELECT for this user.
        assert len(cursor.sql_containing("i.aurora_status = 'summarizing' AND i.status != 'merged'")) == 1


class TestTerminalError:
    def test_max_requeues_marks_error(self):
        results = _summarizing_rows(
            [(_INC, _SESSION)],
            extra=[("SET aurora_status = 'error'", [(_INC,)])],
        ) + [
            ("event_type = 'summary_requeued'", [(2,)]),
        ]
        result, cursor, summary_task = _run_cleanup(results)

        summary_task.delay.assert_not_called()
        assert result["summary_errors"] == 1
        assert cursor.sql_containing("SET aurora_status = 'error'")
        inserts = cursor.sql_containing("INSERT INTO incident_lifecycle_events")
        assert any(p and "rca_error" in p for _, p in inserts)

    def test_missing_session_marks_error(self):
        results = _summarizing_rows(
            [(_INC, None)],
            extra=[("SET aurora_status = 'error'", [(_INC,)])],
        ) + [
            ("event_type = 'summary_requeued'", [(0,)]),
        ]
        result, cursor, summary_task = _run_cleanup(results)

        summary_task.delay.assert_not_called()
        assert result["summary_errors"] == 1

    def test_update_race_does_not_count(self):
        # Incident already moved on by the task itself (UPDATE returns no row):
        # must not be counted as an error.
        results = _summarizing_rows(
            [(_INC, _SESSION)],
            extra=[("SET aurora_status = 'error'", [])],
        ) + [
            ("event_type = 'summary_requeued'", [(2,)]),
        ]
        result, _, _ = _run_cleanup(results)

        assert result["summary_errors"] == 0
