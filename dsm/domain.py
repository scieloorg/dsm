from dsm.data.document import DocsManager
from dsm.utils import packages
from dsm.storage import db
from dsm.data import document_files as docfiles


class DSM:

    def __init__(self, files_storage, db_url, v3_manager, config=None):
        """
        Instancia objeto da classe DSM

        Parameters
        ----------
        files_storage : dsm.storage.minio.MinioStorage
            files storage object
        db_url : str
            Data to connect to a mongodb. Expected pattern:
                "mongodb://my_user:my_password@127.0.0.1:27017/my_db"
        v3_manager : scielo_v3_manager.pid_manager.Manager
            object to register PID v3
        """
        self._db_url = db_url
        self._config = config
        self._docs_manager = DocsManager(
            files_storage, db_url, v3_manager, config)

    def download_package(self, v3):
        """
        Get uri of zip document package or
        Build the zip document package and return uri

        Parameters
        ----------
        v3 : str
            PID v3

        Returns
        -------
        dict
            {"uri": uri, "name": name} or {"error": error}

        Raises
        ------
            dsm.data.document.DocumentDoesNotExistError
            dsm.storage.db.FetchDocumentError
            dsm.storage.db.DBConnectError
        """
        db.mk_connection(self._db_url)
        return self._docs_manager.get_zip_document_package(v3)

    def upload_package(self, source, pid_v2_items=None, old_filenames=None,
                       issue_id=None):
        """
        Receive the package which is a folder or zip file

        Parameters
        ----------
        source : str
            folder or zip file
        pid_v2_items : dict
            key: XML name without extension
            value: PID v2
        old_filenames : dict
            key: XML name without extension
            value: classic website filename if HTML
        issue_id : str
            id do fascículo

        Returns
        -------
        dict

        Raises
        ------
            dsm.data.document_files.ReceivedPackageRegistrationError
        """
        db.mk_connection(self._db_url)

        # obtém os arquivos de cada documento e registra o pacote recebido
        doc_packages = self._docs_manager.receive_package(source)

        # processa cada documento contido no pacote
        results = []
        for name, doc_pkg in doc_packages.items():
            result = {"name": name}
            try:
                docid = self._docs_manager.register_document(
                    doc_pkg,
                    pid_v2_items.get(name),
                    old_filenames.get(name),
                    issue_id,
                )
                if docid:
                    result.update({"id": docid})
            except Exception as e:
                result.update({"error": str(e)})
            results.append(result)
        return results
