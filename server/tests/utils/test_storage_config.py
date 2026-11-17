"""StorageConfig.from_env reads STORAGE_REGION (INFRA-1175: OVH Object Storage is region `sbg`)."""

import os
import sys
from unittest.mock import MagicMock

import pytest

_server_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
if os.path.abspath(_server_dir) not in sys.path:
    sys.path.insert(0, os.path.abspath(_server_dir))

# CI's pre-checks job installs no boto3/redis: stub what storage.py imports at module scope.
for _mod in ("boto3", "botocore", "botocore.config", "botocore.exceptions", "utils.cache.redis_client"):
    try:
        __import__(_mod)
    except ImportError:
        sys.modules[_mod] = MagicMock()

from utils.storage.storage import StorageConfig  # noqa: E402


@pytest.fixture(autouse=True)
def _bucket(monkeypatch):
    monkeypatch.setenv("STORAGE_BUCKET", "aurora-test")
    monkeypatch.delenv("STORAGE_REGION", raising=False)


def test_region_defaults_to_us_east_1():
    assert StorageConfig.from_env().region == "us-east-1"


def test_empty_region_keeps_default(monkeypatch):
    monkeypatch.setenv("STORAGE_REGION", "")
    assert StorageConfig.from_env().region == "us-east-1"


def test_region_from_env(monkeypatch):
    monkeypatch.setenv("STORAGE_REGION", "sbg")
    assert StorageConfig.from_env().region == "sbg"
