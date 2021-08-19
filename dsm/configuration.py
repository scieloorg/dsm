import os
import urllib3

# collection
MINIO_SCIELO_COLLECTION = os.environ.get("MINIO_SCIELO_COLLECTION")

# minio
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_SECURE = True if os.environ.get("MINIO_SECURE", "").lower() == 'true' else False
MINIO_TIMEOUT = int(os.environ.get("MINIO_TIMEOUT", "10000"))

# postgresql+psycopg2://user:password@uri:5432/pid_manager
PID_DATABASE_DSN = os.environ.get("PID_DATABASE_DSN")
PID_DATABASE_TIMEOUT = os.environ.get("PID_DATABASE_TIMEOUT")

# mongodb://my_user:my_password@127.0.0.1:27017/my_db
DATABASE_CONNECT_URL = os.environ.get("DATABASE_CONNECT_URL")

# /var/www/scielo/proc/cisis
CISIS_PATH = os.environ.get("CISIS_PATH")


def get_http_client():
    if not MINIO_TIMEOUT:
        raise ValueError(
            "Missing value for environment variable MINIO_TIMEOUT")
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
    from dsm.extdeps import minio
    VARNAME = (
        "MINIO_HOST",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_SECURE",
        "MINIO_TIMEOUT",
        "MINIO_SCIELO_COLLECTION",
    )
    for var_name in VARNAME:
        if not os.environ.get(var_name):
            raise ValueError(
                f"Missing value for environment variable {var_name}"
            )

    return minio.MinioStorage(
        minio_host=MINIO_HOST,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        scielo_collection=MINIO_SCIELO_COLLECTION,
        minio_secure=MINIO_SECURE,
        minio_http_client=get_http_client(),
    )


def get_pid_manager():
    # optional
    if PID_DATABASE_DSN:
        from scielo_v3_manager.pid_manager import Manager

        return Manager(
            PID_DATABASE_DSN,
            timeout=PID_DATABASE_TIMEOUT,
        )


def get_db_url():
    # mongodb://my_user:my_password@127.0.0.1:27017/my_db
    if not DATABASE_CONNECT_URL:
        raise ValueError(
            f"Missing value for environment variable DATABASE_CONNECT_URL. "
            "DATABASE_CONNECT_URL=mongodb://my_user:my_password@127.0.0.1:27017/my_db"
        )
    return DATABASE_CONNECT_URL


def get_cisis_path():
    # mongodb://my_user:my_password@127.0.0.1:27017/my_db
    if not CISIS_PATH:
        raise ValueError(
            f"Missing value for environment variable CISIS_PATH. "
            "CISIS_PATH=/var/www/scielo/proc/cisis"
        )
    return CISIS_PATH
