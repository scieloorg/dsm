import urllib3
from scielo_v3_manager.pid_manager import Manager
from dsm.storage import minio

import configparser


def configuration_from_ini(ini_file_path):
    parser = configparser.ConfigParser()
    parser.read(ini_file_path)
    return parser["config"]


def configuration_from_str(string):
    """
    MINIO_HOST=
    MINIO_ACCESS_KEY=
    MINIO_SECRET_KEY=
    MINIO_SECURE=
    MINIO_TIMEOUT=
    PID_DATABASE_DSN=
    PID_DATABASE_TIMEOUT=
    DATABASE_CONNECT_URL=
    """
    parser = configparser.ConfigParser()
    parser.read_string(string)
    return parser["config"]


def get_http_client(config):
    return urllib3.PoolManager(
        timeout=config["MINIO_TIMEOUT"],
        maxsize=10,
        cert_reqs="CERT_REQUIRED",
        retries=urllib3.Retry(
            total=5,
            backoff_factor=0.2,
            status_forcelist=[500, 502, 503, 504]
        ))


def get_files_storage(config):
    return minio.MinioStorage(
        minio_host=config["MINIO_HOST"],
        minio_access_key=config["MINIO_ACCESS_KEY"],
        minio_secret_key=config["MINIO_SECRET_KEY"],
        minio_secure=config["MINIO_SECURE"],
        minio_http_client=get_http_client(config),
    )


def get_pid_manager(config):
    return Manager(
        config["PID_DATABASE_DSN"],
        timeout=config["PID_DATABASE_TIMEOUT"],
    )


def get_db_url(config):
    return config["DATABASE_CONNECT_URL"]
