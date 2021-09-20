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
from dsm.utils.files import (
    create_temp_file, size, read_file, write_file,
    date_now_as_folder_name,
)
from dsm.extdeps.isis_migration.isis_cmds import (
    create_id_file,
    get_id_file_path,
    get_document_pids_to_migrate,
)

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


def list_documents_to_migrate(
        acron, issue_folder, pub_year, isis_updated_from, isis_updated_to,
        status=None,
        descending=None,
        page_number=None,
        items_per_page=None,
        ):
    return _migration_manager.list_documents_to_migrate(
        acron, issue_folder, pub_year, isis_updated_from, isis_updated_to,
        status=status,
        descending=descending,
        page_number=page_number,
        items_per_page=items_per_page,
    )


def migrate_document(pid):
    """
    Migrate ISIS records of a document

    Parameters
    ----------
    pid: str
        identifier in ISIS database

    Returns
    -------
    generator
        results of the migration
    """
    _document_isis_db_file_path = get_document_isis_db(pid)
    return migrate_isis_db("artigo", _document_isis_db_file_path)


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
    return _migrate_isis_records(
        id2json.join_id_file_rows_and_return_records(rows), db_type)


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
    print(id_file_records)
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
        print(pid, len(records))
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
        print(".......")
        op = operations[0]
        saved = op['action'](pid, isis_data)
        print(saved)
        events.append(
            _get_event(op, saved,
                       saved[0].isis_created_date, saved[0].isis_updated_date)
        )
    except Exception as e:
        events.append(_get_error(op, e))

    for op in operations[1:]:
        try:
            saved = op['action'](pid)
            events.append(_get_event(op, saved))
        except Exception as e:
            events.append(_get_error(op, e))
    result["events"] = events
    return result


def _get_error(operation, error):
    return {
        "op": operation,
        "error": str(error),
        "timestamp": datetime.utcnow().isoformat(),
    }


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


def migrate_acron(acron, id_folder_path=None):
    configuration.check_migration_sources()

    db_path = configuration.get_bases_acron(acron)
    print("db:", db_path)
    if id_folder_path:
        id_file_path = os.path.join(id_folder_path, f"{acron}.id")
        id_file_path = create_id_file(db_path, id_file_path)
        db_path = id_file_path
        print(f"{id_file_path} - size: {size(id_file_path)} bytes")
    return migrate_isis_db("artigo", db_path)


def identify_documents_to_migrate(from_date=None, to_date=None):
    for doc in migration_manager.get_document_pids_to_migrate(
            from_date, to_date):
        yield _migration_manager.create_mininum_record_in_isis_doc(
            doc["pid"], doc["updated"]
        )


def main():
    parser = argparse.ArgumentParser(
        description="ISIS database migration tool")
    subparsers = parser.add_subparsers(
        title="Commands", metavar="", dest="command")

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

    migrate_document_parser = subparsers.add_parser(
        "migrate_document",
        help=(
            "Migrate document data from ISIS database to MongoDB."
        )
    )
    migrate_document_parser.add_argument(
        "pid",
        help=(
            "PID v2"
        )
    )

    migrate_acron_parser = subparsers.add_parser(
        "migrate_acron",
        help=(
            "Migrate `acron` data from ISIS database to MongoDB."
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

    list_documents_to_migrate_parser = subparsers.add_parser(
        "list_documents_to_migrate",
        help=(
            "Update the website with documents (text available only for XML)"
        ),
    )
    list_documents_to_migrate_parser.add_argument(
        "--acron",
        help="Journal acronym",
    )
    list_documents_to_migrate_parser.add_argument(
        "--issue_folder",
        help="Issue folder (e.g.: v20n1)",
    )
    list_documents_to_migrate_parser.add_argument(
        "--pub_year",
        help="Publication year",
    )
    list_documents_to_migrate_parser.add_argument(
        "--isis_updated_from",
        help="Updated from"
    )
    list_documents_to_migrate_parser.add_argument(
        "--isis_updated_to",
        help="Updated to"
    )
    list_documents_to_migrate_parser.add_argument(
        "--status",
        help="status",
    )
    list_documents_to_migrate_parser.add_argument(
        "--descending",
        help="descending",
    )
    list_documents_to_migrate_parser.add_argument(
        "--page_number",
        help="page_number",
        type=int,
    )
    list_documents_to_migrate_parser.add_argument(
        "--items_per_page",
        help="items_per_page",
        type=int,
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
    elif args.command == "migrate_document":
        result = migrate_document(
            args.pid
        )
    elif args.command == "migrate_acron":
        result = migrate_acron(args.acron, args.id_folder_path)
    elif args.command == "identify_documents_to_migrate":
        result = identify_documents_to_migrate(args.from_date, args.to_date)
    elif args.command == "list_documents_to_migrate":
        result = list_documents_to_migrate(
            args.acron, args.issue_folder, args.pub_year,
            args.isis_updated_from, args.isis_updated_to,
            args.status,
            args.descending,
            args.page_number,
            args.items_per_page,
        )
        for i in result:
            print(i.isis_updated_date, i._id)
    else:
        parser.print_help()

    if result and args.command != "list_documents_to_migrate":
        for res in result:
            print("")
            print(res)


if __name__ == '__main__':
    main()
