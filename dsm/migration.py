"""
API for the migration
"""
import argparse
import os
from datetime import datetime

from dsm.extdeps.isis_migration import (
    id2json,
    migration_manager,
    friendly_isis,
)
from dsm import configuration
from dsm.core.issue import get_bundle_id
from dsm.core.document import DocsManager
from dsm.utils.files import create_temp_file, size, read_file
from dsm import exceptions


_migration_manager = migration_manager.MigrationManager()
_migration_manager.db_connect()


_MIGRATION_PARAMETERS = {
    "title": dict(
        custom_id_function=id2json.journal_id,
        custom_register_isis_function=_migration_manager.register_isis_journal,
        custom_register_isis_event_name="REGISTERED_ISIS_JOURNAL",
        custom_register_website_function=_migration_manager.update_website_journal_data,
        custom_register_website_event_name="REGISTERED_WEBSITE_JOURNAL",
    ),
    "issue": dict(
        custom_id_function=id2json.issue_id,
        custom_register_isis_function=_migration_manager.register_isis_issue,
        custom_register_isis_event_name="REGISTERED_ISIS_ISSUE",
        custom_register_website_function=_migration_manager.update_website_issue_data,
        custom_register_website_event_name="REGISTERED_WEBSITE_ISSUE",
    )
}

def _select_docs(acron=None, issue_folder=None, pub_year=None, updated_from=None, updated_to=None, pid=None):
    if any((acron, issue_folder, pub_year, updated_from, updated_to, pid)):
        yield from _migration_manager.list_documents(
                    acron, issue_folder, pub_year, updated_from, updated_to, pid)
    else:
        for y in range(1900, datetime.now().year):
            y = str(y).zfill(4)
            yield from _migration_manager.list_documents(
                        acron, issue_folder, pub_year, f"{y}0000", f"{y}9999")


def update_website_with_documents_metadata(acron=None, issue_folder=None, pub_year=None, updated_from=None, updated_to=None):
    for doc in _select_docs(acron, issue_folder, pub_year, updated_from, updated_to):
        _migration_manager.update_website_document_metadata(doc._id)


def register_old_website_files(acron=None, issue_folder=None, pub_year=None, updated_from=None, updated_to=None):
    for doc in _select_docs(acron, issue_folder, pub_year, updated_from, updated_to):
        zip_file_path = _migration_manager.register_old_website_document_files(
            doc._id)


def register_documents(pid=None, acron=None, issue_folder=None, pub_year=None, updated_from=None, updated_to=None):
    _files_storage = configuration.get_files_storage()
    _db_url = configuration.get_db_url()
    _v3_manager = configuration.get_pid_manager()

    _docs_manager = DocsManager(_files_storage, _db_url, _v3_manager)


    registered_xml = 0
    registered_metadata = 0

    for doc in _select_docs(acron, issue_folder, pub_year, updated_from, updated_to, pid):
        zip_file_path = None
        try:
            # obtém os arquivos do site antigo (xml, pdf, html, imagens)
            print("")
            print(doc._id)
            print("type:", doc.file_type)
            zip_file_path = _migration_manager.migrate_document_files(doc._id)

            # registra os metadados do documento a partir do registro isis
            print("update_website_document_metadata")
            _migration_manager.update_website_document_metadata(doc._id)

            # registra os pdfs no website
            _migration_manager.update_website_document_pdfs(doc._id)

            # registra os textos completos provenientes dos arquivos HTML e
            # dos registros do tipo `p`
            if doc.file_type == "html":
                _migration_manager.update_website_document_htmls(doc._id)
            elif doc.file_type == "xml":
                _migration_manager.update_website_document_xmls(doc._id)
            registered_metadata += 1
        except Exception as e:
            print("Error registering %s: %s" % (doc._id, e))
            raise

    print("Published with XML: ", registered_xml)
    print("Published with metadata: ", registered_metadata)


def register_external_p_records(acron=None, issue_folder=None, pub_year=None, updated_from=None, updated_to=None):
    _files_storage = configuration.get_files_storage()
    _db_url = configuration.get_db_url()
    _v3_manager = configuration.get_pid_manager()

    _docs_manager = DocsManager(_files_storage, _db_url, _v3_manager)


    for doc in _select_docs(acron, issue_folder, pub_year, updated_from, updated_to):
        try:
            # obtém os arquivos do site antigo (xml, pdf, html, imagens)
            print("")
            print(doc._id)
            print("type:", doc.file_type)
            if doc.file_type != "xml":
                # registra os registros do tipo `p` externos na base artigo
                print("register_isis_document_external_p_records")
                _migration_manager.register_isis_document_external_p_records(
                    doc._id)
        except Exception as e:
            print("Error registering p_records %s: %s" % (doc._id, e))


def _register_package(_docs_manager, zip_file_path, doc):
    fi_doc = friendly_isis.FriendlyISISDocument(doc._id, doc.records)
    issue_id = get_bundle_id(
        doc._id[1:10],
        doc.pub_year,
        fi_doc.volume,
        fi_doc.number,
        fi_doc.suppl,
    )
    packages = _docs_manager.get_doc_packages(zip_file_path)
    doc_pkg = list(packages.values())[0]
    res = _docs_manager.register_document(
        doc_pkg,
        doc._id,
        doc.file_name,
        issue_id,
    )
    print(res)


def register_artigo_id(id_file_path):
    for _id, records in id2json.get_json_records(
            id_file_path, id2json.article_id):
        try:
            if len(records) == 1:
                if _migration_manager.register_isis_issue(_id, records[0]):
                    _migration_manager.update_website_issue_data(_id)
            else:
                _migration_manager.register_isis_document(_id, records)
        except:
            print(_id)
            print(f"Algum problema com {_id}")
            print(records)
            raise


def migrate_title(id_file_path):

    for _id, records in id2json.get_json_records(
            id_file_path, id2json.journal_id):
        try:
            if _migration_manager.register_isis_journal(_id, records[0]):
                _migration_manager.update_website_journal_data(_id)
        except:
            print(_id)
            print(f"Algum problema com {_id}")
            raise


def migrate_issue(id_file_path):

    for _id, records in id2json.get_json_records(
            id_file_path, id2json.issue_id):
        try:
            if _migration_manager.register_isis_issue(_id, records[0]):
                _migration_manager.update_website_issue_data(_id)
        except:
            print(_id)
            print(f"Algum problema com {_id}")
            raise


def create_id_file(db_file_path, id_file_path=None):
    """
    Generates ID file `id_file_path` of a ISIS database `db_file_path`

    Parameters
    ----------
    db_file_path: str
        path of an ISIS database without extension
    id_file_path: str
        path of the ID file to be created

    Returns
    -------
    str
        id_file_path

    Raises
    ------
    exceptions.MissingCisisPathEnvVarError
    exceptions.CisisPathNotFoundMigrationError
    exceptions.MissingI2IdCommandPathEnvVarError
    exceptions.IsisDBNotFoundError
    PermissionError
    FileNotFoundError
    """
    # obtém CISIS_PATH
    cisis_path = configuration.get_cisis_path()

    # check if the utilitary i2id exists
    i2id_cmd = os.path.join(cisis_path, "i2id")
    if not os.path.isfile(i2id_cmd):
        raise exceptions.MissingI2IdCommandPathEnvVarError(
            f"Not found: {i2id_cmd}"
        )

    # check if the isis database exists
    if not os.path.isfile(db_file_path + ".mst"):
        raise exceptions.IsisDBNotFoundError(f"Not found {db_file_path}.mst")

    if id_file_path is None:
        # create id_file in a temp folder
        id_file_path = create_temp_file(os.path.basename(db_file_path))
    else:
        # create the destination folder
        dirname = os.path.dirname(id_file_path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)

        # delete the id_file_path
        write_file(id_file_path, "")

    # execute i2id db > id_file_path
    os.system(f"{i2id_cmd} {db_file_path} > {id_file_path}")

    # check if id_file_path is valid
    try:
        content = read_file(id_file_path, encoding="iso-8859-1")
    except FileNotFoundError:
        return None
    else:
        return id_file_path


def get_id_file_path(source_file_path):
    """
    Evaluate `source_file_path` and returns `source_file_path` if it is ID file
    or create its ID file

    Parameters
    ----------
    source_file_path: str
        path of a ISIS Database or ID file

    Returns
    -------
    str

    Raises
    ------
        exceptions.IdFileNotFoundError
        exceptions.IsisDBNotFoundError

    """
    name, ext = os.path.splitext(source_file_path)
    if ext == ".id":
        # `source_file_path` is an ID file
        if not os.path.isfile(source_file_path):
            raise exceptions.IdFileNotFoundError(
                f"Unable to `get_id_file_path` from {source_file_path}. "
                f"Not found {source_file_path}"
            )
        return source_file_path
    elif ext == "":
        # `source_file_path` is an ISIS databse, so create its ID file
        if not os.path.isfile(source_file_path + ".mst"):
            raise exceptions.IsisDBNotFoundError(
                f"Unable to `get_id_file_path` from {source_file_path}. "
                f"Not found {source_file_path}.mst"
            )
        return create_id_file(source_file_path)


def register_acron(acron, id_folder_path=None):
    configuration.check_migration_sources()
    if id_folder_path and not os.path.isdir(id_folder_path):
        os.makedirs(id_folder_path)
    if id_folder_path:
        id_file_path = os.path.join(id_folder_path, f"{acron}.id")
    else:
        id_file_path = create_temp_file(f"{acron}.id", '')
    db_path = configuration.get_bases_acron(acron)
    if os.path.isfile(db_path + ".mst"):
        create_id_file(db_path, id_file_path)
        print()
        print(f"{id_file_path} - size: {size(id_file_path)} bytes")
        register_artigo_id(id_file_path)
        print("finished\n")


def main():
    parser = argparse.ArgumentParser(
        description="ISIS database migration tool")
    subparsers = parser.add_subparsers(
        title="Commands", metavar="", dest="command")

    create_id_file_parser = subparsers.add_parser(
        "create_id_file",
        help=(
            "Create the id file of a database"
        )
    )
    create_id_file_parser.add_argument(
        "db_file_path",
        help=(
            "Path of the file master without extension. "
            "E.g.: /home/scielo/bases/title/title"
        )
    )
    create_id_file_parser.add_argument(
        "id_file_path",
        help=(
            "Path of the file master without extension. "
            "E.g.: /tmp/title.id"
        )
    )

    migrate_title_parser = subparsers.add_parser(
        "migrate_title",
        help=(
            "Migrate journal data from ISIS database to MongoDB."
        )
    )
    migrate_title_parser.add_argument(
        "source_file_path",
        help=(
            "/path/title/title (ISIS database path) or "
            "/path/title/title.id (ID file path)"
        )
    )

    migrate_issue_parser = subparsers.add_parser(
        "migrate_issue",
        help=(
            "Migrate issue data from ISIS database to MongoDB."
        )
    )
    migrate_issue_parser.add_argument(
        "source_file_path",
        help=(
            "/path/issue/issue (ISIS database path) or "
            "/path/issue/issue.id (ID file path)"
        )
    )

    register_acron_parser = subparsers.add_parser(
        "register_acron",
        help=(
            "Register the content of acron.id in MongoDB and"
            " update the website with acrons data"
        )
    )
    register_acron_parser.add_argument(
        "acron",
        help="Journal acronym"
    )
    register_acron_parser.add_argument(
        "--id_folder_path",
        help="Output folder"
    )

    register_artigo_id_parser = subparsers.add_parser(
        "register_artigo_id",
        help="Register the content of artigo.id in MongoDB",
    )
    register_artigo_id_parser.add_argument(
        "id_file_path",
        help="Path of ID file that will be imported"
    )

    register_documents_parser = subparsers.add_parser(
        "register_documents",
        help=(
            "Update the website with documents (text available only for XML)"
        ),
    )
    register_documents_parser.add_argument(
        "--pid",
        help="pid",
    )
    register_documents_parser.add_argument(
        "--acron",
        help="Journal acronym",
    )
    register_documents_parser.add_argument(
        "--issue_folder",
        help="Issue folder (e.g.: v20n1)",
    )
    register_documents_parser.add_argument(
        "--pub_year",
        help="Publication year",
    )
    register_documents_parser.add_argument(
        "--updated_from",
        help="Updated from"
    )
    register_documents_parser.add_argument(
        "--updated_to",
        help="Updated to"
    )

    register_external_p_records_parser = subparsers.add_parser(
        "register_external_p_records",
        help=(
            "Update the p records using external records"
        ),
    )
    register_external_p_records_parser.add_argument(
        "--pub_year",
        help="Publication year",
    )
    register_external_p_records_parser.add_argument(
        "--acron",
        help="Journal acronym",
    )
    register_external_p_records_parser.add_argument(
        "--issue_folder",
        help="Issue folder (e.g.: v20n1)",
    )
    register_external_p_records_parser.add_argument(
        "--updated_from",
        help="Updated from"
    )
    register_external_p_records_parser.add_argument(
        "--updated_to",
        help="Updated to"
    )

    args = parser.parse_args()
    result = None
    if args.command == "migrate_title":
        result = migrate_title(args.source_file_path)
    elif args.command == "migrate_issue":
        result = migrate_issue(args.source_file_path)
    elif args.command == "register_artigo_id":
        register_artigo_id(args.id_file_path)
    elif args.command == "register_documents":
        register_documents(
            args.pid, args.acron, args.issue_folder, args.pub_year,
            args.updated_from, args.updated_to)
    elif args.command == "register_external_p_records":
        register_external_p_records(
            args.acron, args.issue_folder, args.pub_year,
            args.updated_from, args.updated_to)
    elif args.command == "create_id_file":
        create_id_file(args.db_file_path, args.id_file_path)
    elif args.command == "register_acron":
        register_acron(args.acron, args.id_folder_path)
    else:
        parser.print_help()

    print(result)


if __name__ == '__main__':
    main()
