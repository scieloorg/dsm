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


_db_url = configuration.get_db_url()
print(_db_url)
_migration_manager = migration_manager.MigrationManager(_db_url)


def migrate_documents(pub_year=None, updated_from=None, updated_to=None):
    _migration_manager.db_connect()
    if any((pub_year, updated_from, updated_to)):
        for doc in _migration_manager.list_documents(
                    pub_year, updated_from, updated_to):
            _migration_manager.migrate_document(doc._id)
    else:
        for y in range(1900, datetime.now().year):
            y = str(y).zfill(4)
            for doc in _migration_manager.list_documents(
                        pub_year, f"{y}0000", f"{y}9999"):
                _migration_manager.migrate_document(doc._id)


def migrate_documents_files(pub_year=None, updated_from=None, updated_to=None):
    _migration_manager.db_connect()
    if any((pub_year, updated_from, updated_to)):
        for doc in _migration_manager.list_documents(
                    pub_year, updated_from, updated_to):
            _migration_manager.migrate_document_files(doc._id)
    else:
        for y in range(1900, datetime.now().year):
            y = str(y).zfill(4)
            for doc in _migration_manager.list_documents(
                        pub_year, f"{y}0000", f"{y}9999"):
                _migration_manager.migrate_document_files(doc._id)


def migrate_artigo_id(id_file_path):
    _migration_manager.db_connect()
    for _id, records in id2json.get_json_records(
            id_file_path, id2json.article_id):
        try:
            if len(records) == 1:
                if _migration_manager.register_issue(_id, records[0]):
                    _migration_manager.migrate_issue(_id)
            else:
                _migration_manager.register_document(_id, records)
        except:
            print(_id)
            print(f"Algum problema com {_id}")
            print(records)
            raise


def migrate_title_id(id_file_path):
    _migration_manager.db_connect()

    for _id, records in id2json.get_json_records(
            id_file_path, id2json.journal_id):
        try:
            if _migration_manager.register_journal(_id, records[0]):
                _migration_manager.migrate_journal(_id)
        except:
            print(_id)
            print(f"Algum problema com {_id}")
            raise


def migrate_issue_id(id_file_path):
    _migration_manager.db_connect()

    for _id, records in id2json.get_json_records(
            id_file_path, id2json.issue_id):
        try:
            if _migration_manager.register_issue(_id, records[0]):
                _migration_manager.migrate_issue(_id)
        except:
            print(_id)
            print(f"Algum problema com {_id}")
            raise


def create_id_file_path(db_file_path, id_file_path):
    dirname = os.path.dirname(id_file_path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)
    if os.path.isfile(id_file_path):
        try:
            os.unlink(id_file_path)
        except:
            raise OSError(f"Unable to delete {id_file_path}")
    cisis_path = configuration.get_cisis_path()
    i2id_cmd = os.path.join(cisis_path, "i2id")
    os.system(f"{i2id_cmd} {db_file_path} > {id_file_path}")
    return os.path.isfile(id_file_path)


def main():
    parser = argparse.ArgumentParser(
        description="ISIS database migration tool")
    subparsers = parser.add_subparsers(
        title="Commands", metavar="", dest="command")

    migrate_title_parser = subparsers.add_parser(
        "migrate_title",
        help="Migrate ISIS DB to MongoDB")
    migrate_title_parser.add_argument(
        "id_file_path",
        # metavar="file",
        help="Path of ID file that will be migrated"
    )

    migrate_issue_parser = subparsers.add_parser(
        "migrate_issue",
        help="Migrate ISIS DB to MongoDB")
    migrate_issue_parser.add_argument(
        "id_file_path",
        # metavar="file",
        help="Path of ID file that will be migrated"
    )

    migrate_artigo_parser = subparsers.add_parser(
        "migrate_artigo",
        help="Migrate ISIS DB to MongoDB",
    )
    migrate_artigo_parser.add_argument(
        "id_file_path",
        # metavar="file",
        help="Path of ID file that will be migrated"
    )

    migrate_documents_parser = subparsers.add_parser(
        "migrate_documents",
        help="Migrate JSON to MongoDB (site)",
    )
    migrate_documents_parser.add_argument(
        "--pub_year",
        help="Publication year",
    )
    migrate_documents_parser.add_argument(
        "--updated_from",
        help="Updated from"
    )
    migrate_documents_parser.add_argument(
        "--updated_to",
        help="Updated to"
    )

    migrate_documents_files_parser = subparsers.add_parser(
        "migrate_documents_files",
        help="Migrate XML packages",
    )
    migrate_documents_files_parser.add_argument(
        "--pub_year",
        help="Publication year",
    )
    migrate_documents_files_parser.add_argument(
        "--updated_from",
        help="Updated from"
    )
    migrate_documents_files_parser.add_argument(
        "--updated_to",
        help="Updated to"
    )

    args = parser.parse_args()

    if args.command == "migrate_artigo":
        migrate_artigo_id(args.id_file_path)
    elif args.command == "migrate_title":
        migrate_title_id(args.id_file_path)
    elif args.command == "migrate_issue":
        migrate_issue_id(args.id_file_path)
    elif args.command == "migrate_documents":
        migrate_documents(args.pub_year, args.updated_from, args.updated_to)
    elif args.command == "migrate_documents_files":
        migrate_documents_files(args.pub_year, args.updated_from, args.updated_to)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
