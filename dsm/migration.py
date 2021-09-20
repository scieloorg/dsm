"""
API for the migration
"""
import argparse
import os
from datetime import datetime

from dsm.extdeps.isis_migration import (
    id2json,
    migration_manager,
)
from dsm import configuration
from dsm.core.document import DocsManager
from dsm.utils.files import create_temp_file, size, read_file, write_file

from dsm import exceptions


_migration_manager = migration_manager.MigrationManager()
_migration_manager.db_connect()


_MIGRATION_PARAMETERS = {
    "title": dict(
        custom_id_function=id2json.journal_id,
        operations_sequence=[
            dict(
                name="REGISTER_ISIS",
                result="REGISTERED_ISIS_JOURNAL",
                action=_migration_manager.register_isis_journal,
            ),
            dict(
                name="PUBLISH",
                result="PUBLISHED_JOURNAL",
                action=_migration_manager.publish_journal_data,
            )
        ]
    ),
    "issue": dict(
        custom_id_function=id2json.issue_id,
        operations_sequence=[
            dict(
                name="REGISTER_ISIS",
                result="REGISTERED_ISIS_ISSUE",
                action=_migration_manager.register_isis_issue,
            ),
            dict(
                name="PUBLISH",
                result="PUBLISHED_ISSUE",
                action=_migration_manager.publish_issue_data,
            )
        ]
    ),
    "artigo": dict(
        custom_id_function=id2json.article_id,
        operations_sequence=[
            dict(
                name="REGISTER_ISIS",
                result="REGISTERED_ISIS_DOCUMENT",
                action=_migration_manager.register_isis_document,
            ),
            dict(
                name="MIGRATE_DOCUMENT_FILES",
                result="MIGRATED_DOCUMENT_FILES",
                action=_migration_manager.migrate_document_files,
            ),
            dict(
                name="PUBLISH",
                result="PUBLISHED_DOCUMENT",
                action=_migration_manager.publish_document_metadata,
            ),
            dict(
                name="PUBLISH_PDFS",
                result="PUBLISHED_PDFS",
                action=_migration_manager.publish_document_pdfs,
            ),
            dict(
                name="PUBLISH_XMLS",
                result="PUBLISHED_XMLS",
                action=_migration_manager.publish_document_xmls,
            ),
            dict(
                name="PUBLISH_HTMLS",
                result="PUBLISHED_HTMLS",
                action=_migration_manager.publish_document_htmls,
            ),
        ]
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
            print("publish_document_metadata")
            _migration_manager.publish_document_metadata(doc._id)

            # registra os pdfs no website
            _migration_manager.publish_document_pdfs(doc._id)

            # registra os textos completos provenientes dos arquivos HTML e
            # dos registros do tipo `p`
            if doc.file_type == "html":
                _migration_manager.publish_document_htmls(doc._id)
            elif doc.file_type == "xml":
                _migration_manager.publish_document_xmls(doc._id)
            registered_metadata += 1
        except Exception as e:
            print("Error registering %s: %s" % (doc._id, e))
            raise

    print("Published with XML: ", registered_xml)
    print("Published with metadata: ", registered_metadata)


def register_artigo_id(id_file_path):
    for _id, records in id2json.get_json_records(
            id_file_path, id2json.article_id):
        try:
            if len(records) == 1:
                if _migration_manager.register_isis_issue(_id, records[0]):
                    _migration_manager.publish_issue_data(_id)
            else:
                _migration_manager.register_isis_document(_id, records)
        except:
            print(_id)
            print(f"Algum problema com {_id}")
            print(records)
            raise


def migrate_isis_db(db_type, source_file_path=None, records_content=None):
    """
    Migrate ISIS database content from `source_file_path` or `records_content`
    which is ISIS database or ID file

    Parameters
    ----------
    db_type: str
        "title" or "issue" or "artigo"
    source_file_path: str
        ISIS database or ID file path
    records_content: str
        ID records

    Returns
    -------
    generator
        results of the migration
    """
    if source_file_path:
        # get id_file_path
        id_file_path = get_id_file_path(source_file_path)

        # get id file rows
        rows = id2json.get_id_file_rows(id_file_path)
    elif records_content:
        rows = records_content.splitlines()
    else:
        raise ValueError(
            "Unable to migrate ISIS DB. "
            "Expected `source_file_path` or `records_content`."
        )

    # migrate
    for result in _migrate_isis_records(
            id2json.join_id_file_rows_and_return_records(rows), db_type):
        yield result


def _migrate_isis_records(id_file_records, db_type):
    """
    Migrate data from `source_file_path` which is ISIS database or ID file

    Parameters
    ----------
    id_file_records: generator or list of strings
        list of ID records
    db_type: str
        "title" or "issue" or "artigo"

    Returns
    -------
    generator

    ```
        {
            "pid": "pid",
            "events": [
                {
                    "_id": "",
                    "event": "",
                    "isis_created": "",
                    "isis_updated": "",
                    "created": "",
                    "updated": "",
                },
                {
                    "_id": "",
                    "event": "",
                    "created": "",
                    "updated": "",
                }
            ]
        }
        ``` 
        or 
    ```
        {
            "pid": "pid",
            "error": ""
        }
    ```

    Raises
    ------
        ValueError

    """
    # get the migration parameters according to db_type:
    # title or issue or artigo
    migration_parameters = _MIGRATION_PARAMETERS.get(db_type)
    if not migration_parameters:
        raise ValueError(
            "Invalid value for `db_type`. "
            "Expected values: title, issue, artigo"
        )

    for pid, records in id2json.get_id_and_json_records(
            id_file_records, migration_parameters["custom_id_function"]):
        item_result = {"pid": pid}
        try:
            isis_data = records[0]
            operations_sequence = migration_parameters["operations_sequence"]
            if db_type == "artigo":
                # base artigo
                if len(records) == 1:
                    # registro de issue na base artigo
                    operations_sequence = (
                        _MIGRATION_PARAMETERS["issue"]["operations_sequence"]
                    )
                else:
                    # registros do artigo na base artigo
                    isis_data = records
            _result = _migrate_one_isis_item(
                pid, isis_data, operations_sequence,
            )
            item_result.update(_result)
        except Exception as e:
            item_result["error"] = str(e)
        yield item_result


def _migrate_one_isis_item(pid, isis_data, operations):
    """
    Migrate one ISIS item (title or issue or artigo)

    Parameters
    ----------
    pid: str
    isis_data: str

    Returns
    -------
    dict
        {
            "pid": "",
            "events": [],
        }
    """
    result = {
        "pid": pid,
    }
    events = []
    try:
        saved = operations[0]['action'](pid, isis_data)
        events.append(
            _get_event(operations[0], saved,
                       saved[0].isis_created_date, saved[0].isis_updated_date)
        )
        for op in operations[1:]:
            saved = op['action'](pid)
            events.append(_get_event(op, saved))
    except Exception as e:
        events.append({
            "op": op,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        })
    result["events"] = events
    return result


def _get_event(operation, saved, isis_created_date=None, isis_updated_date=None):
    if not saved:
        return {
            "event_name": operation["name"],
            "event_result": operation["result"],
        }

    record_data, tracker = saved
    event = {
        "_id": record_data._id,
        "event_name": operation["name"],
        "event_result": operation["result"],
        "created": record_data.created,
        "updated": record_data.updated,
    }
    if tracker:
        event.update({
            "detail": tracker.detail,
            "total errors": tracker.total_errors,
        })
        event.update(tracker.status)
    if isis_created_date and isis_updated_date:
        event.update({
            "isis_created": isis_created_date,
            "isis_updated": isis_updated_date,
        })
    return event


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


def migrate_acron(acron, id_folder_path=None):
    configuration.check_migration_sources()

    db_path = configuration.get_bases_acron(acron)
    print("db:", db_path)
    if id_folder_path:
        id_file_path = os.path.join(id_folder_path, f"{acron}.id")
        id_file_path = create_id_file(db_path, id_file_path)
        db_path = id_file_path
        print(f"{id_file_path} - size: {size(id_file_path)} bytes")
    for res in migrate_isis_db("artigo", db_path):
        yield res


def identify_documents_to_migrate(from_date=None, to_date=None):
    for doc in migration_manager.get_document_pids_to_migrate(from_date, to_date):
        yield _migration_manager.create_mininum_record_in_isis_doc(
            doc["pid"], doc["updated"]
        )


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

    migrate_artigo_parser = subparsers.add_parser(
        "migrate_artigo",
        help=(
            "Migrate artigo data from ISIS database to MongoDB."
        )
    )
    migrate_artigo_parser.add_argument(
        "source_file_path",
        help=(
            "/path/artigo/artigo (ISIS database path) or "
            "/path/artigo/artigo.id (ID file path)"
        )
    )

    migrate_acron_parser = subparsers.add_parser(
        "migrate_acron",
        help=(
            "Register the content of acron.id in MongoDB and"
            " update the website with acrons data"
        )
    )
    migrate_acron_parser.add_argument(
        "acron",
        help="Journal acronym"
    )
    migrate_acron_parser.add_argument(
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

    identify_documents_to_migrate_parser = subparsers.add_parser(
        "identify_documents_to_migrate",
        help="Register the pid and isis_updated_date in isis_doc",
    )
    identify_documents_to_migrate_parser.add_argument(
        "--from_date",
        help="from date",
    )
    identify_documents_to_migrate_parser.add_argument(
        "--to_date",
        help="to date",
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

    args = parser.parse_args()
    result = None
    if args.command == "migrate_title":
        result = migrate_isis_db(
            "title", args.source_file_path
        )
    elif args.command == "migrate_issue":
        result = migrate_isis_db(
            "issue", args.source_file_path
        )
    elif args.command == "migrate_artigo":
        result = migrate_isis_db(
            "artigo", args.source_file_path
        )
    elif args.command == "register_artigo_id":
        register_artigo_id(args.id_file_path)
    elif args.command == "register_documents":
        register_documents(
            args.pid, args.acron, args.issue_folder, args.pub_year,
            args.updated_from, args.updated_to)
    elif args.command == "create_id_file":
        create_id_file(args.db_file_path, args.id_file_path)
    elif args.command == "migrate_acron":
        result = migrate_acron(args.acron, args.id_folder_path)
    elif args.command == "identify_documents_to_migrate":
        result = identify_documents_to_migrate(args.from_date, args.to_date)
    else:
        parser.print_help()

    if result:
        for res in result:
            print("")
            print(res)


if __name__ == '__main__':
    main()
