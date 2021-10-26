import os
import urllib3
import glob

from dsm import exceptions
from dsm.utils import files

# collection
MINIO_SCIELO_COLLECTION = os.environ.get("MINIO_SCIELO_COLLECTION")

# minio
MINIO_HOST = os.environ.get("MINIO_HOST")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_SECURE = True if os.environ.get("MINIO_SECURE", "").lower() == 'true' else False
MINIO_TIMEOUT = int(os.environ.get("MINIO_TIMEOUT", "10000"))
MINIO_SPF_DIR = os.environ.get("MINIO_SPF_DIR")

# postgresql+psycopg2://user:password@uri:5432/pid_manager
PID_DATABASE_DSN = os.environ.get("PID_DATABASE_DSN")
PID_DATABASE_TIMEOUT = os.environ.get("PID_DATABASE_TIMEOUT")

# mongodb://my_user:my_password@127.0.0.1:27017/my_db
DATABASE_CONNECT_URL = os.environ.get("DATABASE_CONNECT_URL")

# /var/www/scielo/proc/cisis
CISIS_PATH = os.environ.get("CISIS_PATH")

BASES_WORK_PATH = os.environ.get("BASES_WORK_PATH")
BASES_XML_PATH = os.environ.get("BASES_XML_PATH")
BASES_PDF_PATH = os.environ.get("BASES_PDF_PATH")
BASES_TRANSLATION_PATH = os.environ.get("BASES_TRANSLATION_PATH")
HTDOCS_IMG_REVISTAS_PATH = os.environ.get("HTDOCS_IMG_REVISTAS_PATH")
BASES_PATH = os.environ.get("BASES_PATH")


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
        "MINIO_SPF_DIR",
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
    """
    Get CISIS_PATH
    """
    if not CISIS_PATH:
        raise exceptions.MissingCisisPathEnvVarError(
            "Missing value for environment variable CISIS_PATH. "
            "CISIS_PATH=/var/www/scielo/proc/cisis"
        )
    if not os.path.isdir(CISIS_PATH):
        raise exceptions.CisisPathNotFoundMigrationError(
            f"{CISIS_PATH} not found."
        )
    return CISIS_PATH


def check_migration_sources():
    paths = (
        BASES_WORK_PATH,
        BASES_XML_PATH,
        BASES_PDF_PATH,
        BASES_TRANSLATION_PATH,
        HTDOCS_IMG_REVISTAS_PATH,
    )
    names = (
        "BASES_WORK_PATH",
        "BASES_XML_PATH",
        "BASES_PDF_PATH",
        "BASES_TRANSLATION_PATH",
        "HTDOCS_IMG_REVISTAS_PATH",
    )
    for path, name in zip(paths, names):
        if not path:
            raise ValueError(f"Missing configuration: {name}")
        if not os.path.isdir(path):
            raise ValueError(f"{name} must be a directory")


def get_paragraphs_id_file_path(article_pid):
    return os.path.join(
        os.path.dirname(BASES_PDF_PATH), "artigo", "p",
        article_pid[1:10], article_pid[10:14],
        article_pid[14:18], article_pid[-5:] + ".id",
    )


class DocumentFilesAtOldWebsite:

    def __init__(self, subdir_acron_issue, file_name, main_lang):
        self._subdir_acron_issue = subdir_acron_issue
        self._file_name = file_name
        self._main_lang = main_lang
        self._htdocs_img_revistas_files_paths = None
        self._bases_translation_files_paths = None
        self._bases_pdf_files_paths = None
        self._bases_xml_file_path = None

    @property
    def bases_translation_files_paths(self):
        """
        Obtém os arquivos HTML de bases/translation/acron/volnum/*filename*

        Returns
        -------
        dict
        {"en": {"front": "en_a01.html", "back": "en_ba01.html"},
         "es": {"front": "es_a01.html", "back": "es_ba01.html"}}
        """
        if self._bases_translation_files_paths is None:

            files = {}
            patterns = (f"??_{self._file_name}.htm*", f"??_b{self._file_name}.htm*")
            labels = ("front", "back")
            for label, pattern in zip(labels, patterns):
                paths = glob.glob(
                    os.path.join(
                        BASES_TRANSLATION_PATH, self._subdir_acron_issue, pattern)
                )
                if not paths:
                    continue
                # translations
                for path in paths:
                    basename = os.path.basename(path)
                    lang = basename[:2]
                    files.setdefault(lang, {})
                    files[lang][label] = path
            self._bases_translation_files_paths = files
        return self._bases_translation_files_paths

    @property
    def bases_pdf_files_paths(self):
        """
        Obtém os arquivos PDFs de bases/pdf/acron/volnum/*filename*.pdf

        Returns
        -------
        dict
        {"pt": "a01.pdf",
         "en": "en_a01.pdf",
         "es": "es_a01.pdf"}
        """
        if self._bases_pdf_files_paths is None:
            files = {}
            for pattern in (f"{self._file_name}.pdf", f"??_{self._file_name}.pdf"):
                paths = glob.glob(
                    os.path.join(
                        BASES_PDF_PATH,
                        self._subdir_acron_issue,
                        pattern
                    )
                )
                if not paths:
                    continue
                if "_" in pattern:
                    # translations
                    for path in paths:
                        basename = os.path.basename(path)
                        lang = basename[:2]
                        files[lang] = path
                else:
                    # main pdf
                    files[self._main_lang] = paths[0]
            self._bases_pdf_files_paths = files
        return self._bases_pdf_files_paths

    @property
    def htdocs_img_revistas_files_paths(self):
        """
        Obtém os arquivos de imagens de
        htdocs/img/revistas/acron/volnum/*filename*

        Returns
        -------
        list
            ["a01f01.jpg", "a01f02.jpg"],
        """
        if self._htdocs_img_revistas_files_paths is None:
            self._htdocs_img_revistas_files_paths = glob.glob(
                os.path.join(
                    HTDOCS_IMG_REVISTAS_PATH,
                    self._subdir_acron_issue,
                    f"*{self._file_name}*.*"
                )
            )
        return self._htdocs_img_revistas_files_paths

    @property
    def bases_xml_file_path(self):
        if self._bases_xml_file_path is None:
            try:
                xml_file_path = os.path.join(
                    BASES_XML_PATH,
                    self._subdir_acron_issue,
                    f"{self._file_name}.xml"
                )
                self._bases_xml_file_path = glob.glob(xml_file_path)[0]
            except IndexError:
                return None
        return self._bases_xml_file_path


def get_files_storage_folder_for_published_htmls(issn, issue_folder, file_name):
    return os.path.join(
        "migrated", "published", "htmls", issn, issue_folder, file_name)


def get_files_storage_folder_for_published_xmls(issn, issue_folder, file_name):
    return os.path.join(
        "migrated", "published", "xmls", issn, issue_folder, file_name)


def get_files_storage_folder_for_migration(issn, issue_folder, file_name):
    return os.path.join(
        "migrated", "original", issn, issue_folder, file_name)


def get_files_storage_folder_for_document_site_content(issn, scielo_pid_v3):
    """
    Get files storage folder for document's site content (xml, pdf, jpg, tiff, png)

    Parameters
    ----------
    issn : str
        document's issn
    scielo_pid_v3: str
        document's identifier

    Returns
    -------
    str
        folder at files storage
    """
    return os.path.join(
        "documents", issn, scielo_pid_v3
    )


def get_files_storage_folder_for_zipped_packages(issn, scielo_pid_v3):
    """
    Get files storage folder for documents ingressed by ingression module

    Parameters
    ----------
    issn : str
        document's issn
    scielo_pid_v3: str
        document's identifier

    Returns
    -------
    str
        folder at files storage
    """
    date_now_as_folder_name = files.date_now_as_folder_name()
    return os.path.join(
        "ingress", "download", issn, scielo_pid_v3, date_now_as_folder_name)


def get_files_storage_folder_for_received_packages(name, date_now_as_folder_name=None):
    return os.path.join(
        "ingress", "upload", date_now_as_folder_name[:10],
        name, date_now_as_folder_name)


def get_bases_acron(acron):
    return os.path.join(BASES_WORK_PATH, acron, acron)


def get_bases_artigo_path():
    return os.path.join(BASES_PATH, "artigo", "artigo")


def get_htdocs_path():
    return os.path.dirname(os.path.dirname(HTDOCS_IMG_REVISTAS_PATH))

