
from dsm.core.document import DocsManager

import configuration


_files_storage = configuration.get_files_storage()
_db_url = configuration.get_db_url()
_v3_manager = configuration.get_pid_manager()


_docs_manager = DocsManager(_files_storage, _db_url, _v3_manager)


def download_package(v3):
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
        dsm.exceptions.DocumentDoesNotExistError
        dsm.exceptions.FetchDocumentError
        dsm.exceptions.DBConnectError
    """
    _docs_manager.db_connect()
    return _docs_manager.get_zip_document_package(v3)


def upload_package(source, pid_v2_items=None, old_filenames=None,
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
        dsm.exceptions.ReceivedPackageRegistrationError
    """
    _docs_manager.db_connect()

    # obtém os arquivos de cada documento e registra o pacote recebido
    doc_packages = _docs_manager.receive_package(source)

    # processa cada documento contido no pacote
    results = []
    for name, doc_pkg in doc_packages.items():
        result = {"name": name}
        try:
            docid = _docs_manager.register_document(
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
