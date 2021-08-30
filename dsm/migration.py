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

_migration_manager = migration_manager.MigrationManager()


def _select_docs(pub_year=None, updated_from=None, updated_to=None):
    if any((pub_year, updated_from, updated_to)):
        yield from _migration_manager.list_documents(
                    pub_year, updated_from, updated_to)
    else:
        for y in range(1900, datetime.now().year):
            y = str(y).zfill(4)
            yield from _migration_manager.list_documents(
                        pub_year, f"{y}0000", f"{y}9999")


def update_website_with_documents_metadata(pub_year=None, updated_from=None, updated_to=None):
    _migration_manager.db_connect()
    for doc in _select_docs(pub_year, updated_from, updated_to):
        _migration_manager.update_website_document_metadata(doc._id)


def register_old_website_files(pub_year=None, updated_from=None, updated_to=None):
    _migration_manager.db_connect()
    for doc in _select_docs(pub_year, updated_from, updated_to):
        zip_file_path = _migration_manager.register_old_website_document_files(
            doc._id)


def register_documents(pub_year=None, updated_from=None, updated_to=None):
    _files_storage = configuration.get_files_storage()
    _db_url = configuration.get_db_url()
    _v3_manager = configuration.get_pid_manager()

    _docs_manager = DocsManager(_files_storage, _db_url, _v3_manager)

    _migration_manager.db_connect()

    registered_xml = 0
    registered_metadata = 0

    for doc in _select_docs(pub_year, updated_from, updated_to):
        zip_file_path = None
        try:
            # obtém os arquivos do site antigo (xml, pdf, html, imagens)
            zip_file_path = _migration_manager.register_old_website_document_files(
                doc._id)
            if doc.file_type != "xml":
                # registra os textos completos marcados em HTML
                # que provém de registros do tipo `p` e
                # de arquivos HTML localizados na pasta `bases/translation`
                _migration_manager.register_isis_document_html_paragraphs(doc._id)
            if doc.file_type == "xml" and zip_file_path and os.path.isfile(zip_file_path):
                # em caso de marcação XML, faz o registro do documento
                # usando o pacote XML
                _register_package(_docs_manager, zip_file_path, doc)
                registered_xml += 1
            else:
                # registra os metadados do documento a partir do registro isis
                _migration_manager.update_website_document_metadata(doc._id)
                # registra os textos completos provenientes dos arquivos HTML e
                # dos registros do tipo `p`
                _migration_manager.update_website_document_htmls(doc._id)
                registered_metadata += 1
        except Exception as e:
            print("Error registering %s: %s" % (doc._id, e))

    print("Published with XML: ", registered_xml)
    print("Published with metadata: ", registered_metadata)


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


def register_artigo_id_file_data(id_file_path):
    _migration_manager.db_connect()
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
    _migration_manager.db_connect()

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
    _migration_manager.db_connect()

    for _id, records in id2json.get_json_records(
            id_file_path, id2json.issue_id):
        try:
            if _migration_manager.register_isis_issue(_id, records[0]):
                _migration_manager.update_website_issue_data(_id)
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
        help=(
            "Register the content of title.id in MongoDB and"
            " update the website with journals data"
        )
    )
    migrate_title_parser.add_argument(
        "id_file_path",
        help="Path of ID file that will be imported"
    )

    migrate_issue_parser = subparsers.add_parser(
        "migrate_issue",
        help=(
            "Register the content of issue.id in MongoDB and"
            " update the website with issues data"
        )
    )
    migrate_issue_parser.add_argument(
        "id_file_path",
        help="Path of ID file that will be imported"
    )

    register_artigo_id_file_data_parser = subparsers.add_parser(
        "register_artigo_id_file_data",
        help="Register the content of artigo.id in MongoDB",
    )
    register_artigo_id_file_data_parser.add_argument(
        "id_file_path",
        help="Path of ID file that will be imported"
    )

    # update_website_with_documents_metadata_parser = subparsers.add_parser(
    #     "update_website_with_documents_metadata",
    #     help="Migrate JSON to MongoDB (site)",
    # )
    # update_website_with_documents_metadata_parser.add_argument(
    #     "--pub_year",
    #     help="Publication year",
    # )
    # update_website_with_documents_metadata_parser.add_argument(
    #     "--updated_from",
    #     help="Updated from"
    # )
    # update_website_with_documents_metadata_parser.add_argument(
    #     "--updated_to",
    #     help="Updated to"
    # )

    # register_old_website_files_parser = subparsers.add_parser(
    #     "register_old_website_files",
    #     help="Migrate XML packages",
    # )
    # register_old_website_files_parser.add_argument(
    #     "--pub_year",
    #     help="Publication year",
    # )
    # register_old_website_files_parser.add_argument(
    #     "--updated_from",
    #     help="Updated from"
    # )
    # register_old_website_files_parser.add_argument(
    #     "--updated_to",
    #     help="Updated to"
    # )

    register_documents_parser = subparsers.add_parser(
        "register_documents",
        help=(
            "Update the website with documents (text available only for XML)"
        ),
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

    if args.command == "migrate_title":
        migrate_title(args.id_file_path)
    elif args.command == "migrate_issue":
        migrate_issue(args.id_file_path)
    elif args.command == "register_artigo_id_file_data":
        register_artigo_id_file_data(args.id_file_path)
    elif args.command == "register_documents":
        register_documents(args.pub_year, args.updated_from, args.updated_to)
    # elif args.command == "update_website_with_documents_metadata":
    #     update_website_with_documents_metadata(
    #         args.pub_year, args.updated_from, args.updated_to)
    # elif args.command == "register_old_website_files":
    #     register_old_website_files(
    #         args.pub_year, args.updated_from, args.updated_to)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
