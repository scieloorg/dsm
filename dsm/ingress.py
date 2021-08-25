"""
API to ingress documents
"""
import argparse
from datetime import datetime

from dsm.core.document import DocsManager

from dsm import configuration


_files_storage = configuration.get_files_storage()
_db_url = configuration.get_db_url()
_v3_manager = configuration.get_pid_manager()


_docs_manager = DocsManager(_files_storage, _db_url, _v3_manager)


def get_package_uri_by_pid(scielo_pid_v3):
    """
    Get uri of zip document package or
    Build the zip document package and return uri

    Parameters
    ----------
    scielo_pid_v3 : str
        document's identifier version 3

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
    
    results = {'doc_pkg': [], 'errors': []}
    try:
        doc_pkg = _docs_manager.get_zip_document_package(scielo_pid_v3)
        if doc_pkg:
            results['doc_pkg'].append(doc_pkg)
    except Exception as e:
        raise
        results['errors'].append(str(e))

    return results


def upload_package(source, receipt_id=None, pid_v2_items={}, old_filenames={},
                   issue_id=None, is_new_document=False):
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
    is_new_document: boolean
        é documento novo?

    Returns
    -------
    dict

    Raises
    ------
        dsm.exceptions.ReceivedPackageRegistrationError
    """
    receipt_id = receipt_id or datetime.now().isoformat()
    _docs_manager.db_connect()

    # obtém os arquivos de cada documento
    doc_packages = _docs_manager.get_doc_packages(source)

    # processa cada documento contido no pacote
    results = {'docs': [], 'errors': []}
    for name, doc_pkg in doc_packages.items():
        try:
            docid = _docs_manager.register_document(
                doc_pkg,
                pid_v2_items.get(name),
                old_filenames.get(name),
                issue_id,
            )
            if docid:
                results['docs'].append({"name": name, "id": docid})

        except Exception as e:
            raise
            results['errors'].append(str(e))

    # registra o pacote recebido
    if len(results['errors']) == 0:
        _docs_manager.store_received_package(source, receipt_id)

    return results


def list_documents(**kwargs):
    # TODO
    _docs_manager.db_connect()
    return []


def _download_package(v3):
    print(get_package_uri_by_pid(v3))


def _upload_package(path):
    print(upload_package(path))


def main():
    parser = argparse.ArgumentParser(
        description="Documents ingress tools")
    subparsers = parser.add_subparsers(
        title="Commands", metavar="", dest="command")

    download_package_parser = subparsers.add_parser(
        "download_package",
        help=(
            "Register the content of title.id in MongoDB and"
            " update the website with journals data"
        )
    )
    download_package_parser.add_argument(
        "v3",
        help="Path of ID file that will be imported"
    )

    upload_package_parser = subparsers.add_parser(
        "upload_package",
        help=(
            "Register the content of issue.id in MongoDB and"
            " update the website with issues data"
        )
    )
    upload_package_parser.add_argument(
        "source_path",
        help="Path of ID file that will be imported"
    )

    args = parser.parse_args()

    if args.command == "download_package":
        _download_package(args.v3)
    elif args.command == "upload_package":
        _upload_package(args.source_path)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
