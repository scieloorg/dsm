import os
import urllib3
from scielo_v3_manager.pid_manager import Manager
from dsm.extdeps import minio


DEFAULT_MINIO_HOST = ''
DEFAULT_MINIO_ACCESS_KEY = ''
DEFAULT_MINIO_SECRET_KEY = ''
DEFAULT_MINIO_SECURE = ''
DEFAULT_MINIO_TIMEOUT = 20000
DEFAULT_PID_DATABASE_DSN = (
    'postgresql+psycopg2://user:password@uri:5432/pid_manager'
)
DEFAULT_PID_DATABASE_TIMEOUT = 20000
DEFAULT_DATABASE_CONNECT_URL = (
    'mongodb://my_user:my_password@127.0.0.1:27017/my_db'
)

MINIO_HOST = os.environ.get("MINIO_HOST", DEFAULT_MINIO_HOST)
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", DEFAULT_MINIO_ACCESS_KEY)
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", DEFAULT_MINIO_SECRET_KEY)
MINIO_SECURE = os.environ.get("MINIO_SECURE", DEFAULT_MINIO_SECURE)
MINIO_TIMEOUT = os.environ.get("MINIO_TIMEOUT", DEFAULT_MINIO_TIMEOUT)
PID_DATABASE_DSN = os.environ.get("PID_DATABASE_DSN", DEFAULT_PID_DATABASE_DSN)
PID_DATABASE_TIMEOUT = os.environ.get("PID_DATABASE_TIMEOUT", DEFAULT_PID_DATABASE_TIMEOUT)
DATABASE_CONNECT_URL = os.environ.get("DATABASE_CONNECT_URL", DEFAULT_DATABASE_CONNECT_URL)


def get_http_client():
    return urllib3.PoolManager(
        timeout=MINIO_TIMEOUT,
        maxsize=10,
        cert_reqs="CERT_REQUIRED",
        retries=urllib3.Retry(
            total=5,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504]
        ))


def get_files_storage():
    return minio.MinioStorage(
        minio_host=MINIO_HOST,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        minio_secure=MINIO_SECURE,
        minio_http_client=get_http_client(),
    )


def get_pid_manager():
    # optional
    if PID_DATABASE_DSN:
        return Manager(
            PID_DATABASE_DSN,
            timeout=PID_DATABASE_TIMEOUT,
        )


def get_db_url():
    return DATABASE_CONNECT_URL
