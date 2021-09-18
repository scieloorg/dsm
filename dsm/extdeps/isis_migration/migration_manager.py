import os
import glob
from datetime import datetime

from scielo_v3_manager.v3_gen import generates

from dsm.utils.html_utils import adapt_html_text_to_website, get_assets_locations
from dsm.utils.xml_utils import get_xml_tree, tostring

from dsm.utils.async_download import download_files
from dsm.utils.reqs import (
    requests_get,
    requests_get_content,
)
from dsm.utils.files import (
    create_zip_file,
    create_temp_file,
    read_from_zipfile,
    read_file,
)
from dsm.configuration import (
    check_migration_sources,
    get_files_storage,
    get_db_url,
    get_paragraphs_id_file_path,
    DocumentFilesAtOldWebsite,
    get_files_storage_folder_for_htmls,
    get_files_storage_folder_for_xmls,
    get_files_storage_folder_for_migration,
    get_htdocs_path,
)
from dsm.core.issue import get_bundle_id
from dsm.core.document import (
    set_translate_titles,
    set_translated_sections,
    set_abstracts,
    set_keywords,
    set_authors,
    set_authors_meta,
)
from dsm.core.document_files import (
    files_storage_register,
)
from dsm.core.sps_package import SPS_Package
from dsm.extdeps.isis_migration import friendly_isis
from dsm.extdeps import db
from dsm.extdeps.isis_migration.id2json import get_paragraphs_records
from dsm import exceptions


class Tracker:

    def __init__(self, tracked_operation):
        self._tracked_operation = tracked_operation
        self._tracks = []

    def info(self, _info):
        self._tracks.append({
            "datetime": datetime.utcnow(),
            "operation": self._tracked_operation,
            "info": _info
        })

    def error(self, _error):
        self._tracks.append({
            "datetime": datetime.utcnow(),
            "operation": self._tracked_operation,
            "error": _error
        })

    @property
    def detail(self):
        return self._tracks


class MigrationManager:
    """
    Obtém os dados das bases ISIS: artigo, title e issue.
    Registra-os, respectivamente nas coleções do mongodb:
    isis_doc, isis_journal, isis_issue

    Obtém os arquivos de:
    - bases/pdf
    - bases/xml
    - htdocs/img/revistas
    Faz um pacote zip com os arquivos de cada documento e os registra no minio

    # TODO
    # Obtém os dados das bases artigo/p/ (registros de parágrafos)

    # TODO
    #    Obtém os arquivos de:
    #     - bases/translation
    """
    def __init__(self):
        """
        Instancia objeto da classe MigrationManager
        """
        self._db_url = get_db_url()
        self._files_storage = get_files_storage()
        check_migration_sources()

    def db_connect(self):
        db.mk_connection(self._db_url)

    def register_isis_document(self, _id, records):
        """
        Register migrated document data

        Parameters
        ----------
        _id: str
        records : list of dict

        Returns
        -------
        str
            _id

        Raises
        ------
            dsm.storage.db.DBSaveDataError
            dsm.storage.db.DBCreateDocumentError
        """
        # recupera `isis_document` ou cria se não existir

        # se existirem osregistros de parágrafos que estejam externos à
        # base artigo, ou seja, em artigo/p/ISSN/ANO/ISSUE_ORDER/...,
        # os recupera e os ingressa junto aos registros da base artigo
        p_records = (
            get_paragraphs_records(get_paragraphs_id_file_path(_id)) or []
        )
        tracker = Tracker("register_isis_document")
        tracker.info("total of external p records: {len(p_records)}")

        doc = friendly_isis.FriendlyISISDocument(_id, records + p_records)
        isis_document = (
                db.fetch_isis_document(_id) or
                db.create_isis_document()
        )
        isis_document._id = doc._id
        isis_document.doi = doc.doi
        isis_document.pub_year = doc.collection_pubdate[:4]

        isis_document.isis_updated_date = doc.isis_updated_date
        isis_document.isis_created_date = doc.isis_created_date
        isis_document.records = doc.records
        isis_document.status = "1"

        isis_document.file_name = doc.file_name
        isis_document.file_type = doc.file_type
        isis_document.issue_folder = doc.issue_folder

        journal = db.fetch_isis_journal(doc.journal_pid)
        _journal = friendly_isis.FriendlyISISJournal(
            journal._id, journal.record)
        isis_document.acron = _journal.acronym

        # salva o documento
        db.save_data(isis_document)
        return isis_document, tracker

    def register_isis_document_external_p_records(self, _id):
        """
        Register migrated document data

        Parameters
        ----------
        _id: str
        records : list of dict

        Returns
        -------
        str
            _id

        Raises
        ------
            dsm.storage.db.DBSaveDataError
            dsm.storage.db.DBCreateDocumentError
        """
        # recupera `isis_document` ou cria se não existir

        # se existirem os registros de parágrafos que estejam externos à
        # base artigo, ou seja, em artigo/p/ISSN/ANO/ISSUE_ORDER/...,
        # os recupera e os ingressa junto aos registros da base artigo
        p_records = (
            get_paragraphs_records(get_paragraphs_id_file_path(_id)) or []
        )
        if not p_records:
            return
        isis_document = db.fetch_isis_document(_id)
        if not isis_document:
            raise exceptions.DocumentDoesNotExistError(
                "isis_document %s does not exist" % _id
            )
        doc = friendly_isis.FriendlyISISDocument(_id, isis_document.records)

        # atualiza p_records
        doc.p_records = p_records

        # atualiza registros
        isis_document.records = doc.records

        # salva o documento
        db.save_data(isis_document)
        return isis_document, None

    def register_isis_journal(self, _id, record):
        """
        Register migrated journal data

        Parameters
        ----------
        _id: str
        record : JSON

        Returns
        -------
        str
            _id

        Raises
        ------
            dsm.storage.db.DBSaveDataError
            dsm.storage.db.DBCreateDocumentError
        """
        # recupera isis_journal ou cria se não existir

        journal = friendly_isis.FriendlyISISJournal(_id, record)
        isis_journal = (
                db.fetch_isis_journal(_id) or
                db.create_isis_journal()
        )
        isis_journal._id = journal._id
        isis_journal.isis_updated_date = journal.isis_updated_date
        isis_journal.isis_created_date = journal.isis_created_date
        isis_journal.record = journal.record

        # salva o journal
        db.save_data(isis_journal)
        return isis_journal, None

    def register_isis_issue(self, _id, record):
        """
        Register migrated issue data

        Parameters
        ----------
        _id: str
        record : dict

        Returns
        -------
        str
            _id

        Raises
        ------
            dsm.storage.db.DBSaveDataError
            dsm.storage.db.DBCreateDocumentError
        """
        # recupera isis_issue ou cria se não existir

        issue = friendly_isis.FriendlyISISIssue(_id, record)
        isis_issue = (
                db.fetch_isis_issue(issue._id) or
                db.create_isis_issue()
        )
        isis_issue._id = issue._id
        isis_issue.isis_updated_date = issue.isis_updated_date
        isis_issue.isis_created_date = issue.isis_created_date
        isis_issue.record = issue.record

        # salva o issue
        db.save_data(isis_issue)
        return isis_issue, None

    def update_website_journal_data(self, journal_id):
        """
        Migrate isis journal data to website

        Parameters
        ----------
        journal_id : str

        Returns
        -------
        dict
        """
        # registro migrado formato json
        i_journal = db.fetch_isis_journal(journal_id)

        # interface mais amigável para obter os dados
        fi_journal = friendly_isis.FriendlyISISJournal(
            i_journal._id, i_journal.record)

        # cria ou recupera o registro do website
        journal = (
            db.fetch_journal(journal_id) or db.create_journal()
        )

        # atualiza os dados
        _update_journal_with_isis_data(journal, fi_journal)

        # salva os dados
        db.save_data(journal)
        return journal, None

    def update_website_issue_data(self, issue_id):
        """
        Migrate isis issue data to website

        Parameters
        ----------
        issue_id : str

        Returns
        -------
        dict
        """
        # registro migrado formato json
        i_issue = db.fetch_isis_issue(issue_id)

        # interface mais amigável para obter os dados
        fi_issue = friendly_isis.FriendlyISISIssue(
            i_issue._id, i_issue.record)

        # cria ou recupera o registro do website
        issue = db.fetch_issue(issue_id) or db.create_issue()

        # atualiza os dados
        _update_issue_with_isis_data(issue, fi_issue)

        # salva os dados
        db.save_data(issue)
        return issue, None

    def update_website_document_metadata(self, article_id):
        """
        Update the website document

        Parameters
        ----------
        article_id : str

        Returns
        -------
        dict
        """
        # obtém os dados de artigo migrado
        migrated_document = MigratedDocument(article_id)

        # cria ou recupera o registro de documento do website
        document = db.fetch_document(article_id) or db.create_document()

        # cria ou recupera o registro de issue do website
        bundle_id = get_bundle_id(
            migrated_document.journal_pid,
            migrated_document.pub_year,
            migrated_document.volume,
            migrated_document.number,
            migrated_document.suppl,
        )
        issue = db.fetch_issue(bundle_id) or db.create_issue()

        # atualiza os dados
        _update_document_with_isis_data(document, migrated_document, issue)

        # salva os dados
        db.save_data(document)
        return document, None

    def update_website_document_pdfs(self, article_pid):
        """
        Update the website document pdfs
        Get texts from paragraphs records and from translations files
            registered in `isis_doc`
        Build the HTML files and register them in the files storage
        Update the `document.pdfs` with lang and uri
        Parameters
        ----------
        article_pid : str
        Returns
        -------
        dict
        """
        # obtém os dados de artigo
        migrated = MigratedDocument(article_pid)

        tracker = Tracker("update_website_document_pdfs")

        # cria ou recupera o registro de documento do website
        document = db.fetch_document(article_pid)
        if not document:
            raise exceptions.DocumentDoesNotExistError(
                "Document %s does not exist" % article_pid
            )

        # obtém os dados de `isis_doc.pdfs` e `isis_doc.pdf_files`
        # e os organiza para registrar em `document.pdfs`
        document.pdfs = migrated.migrated_pdfs

        # rastreia pdfs migrados
        tracker.info(f"migrated pdfs: {document.pdfs}")

        # salva os dados
        db.save_data(document)
        return document, tracker

    def update_website_document_htmls(self, article_pid):
        """
        Update the website document htmls

        Get texts from paragraphs records and from translations files
            registered in `isis_doc`
        Build the HTML files and register them in the files storage
        Update the `document.htmls` with lang and uri

        Parameters
        ----------
        article_pid : str

        Returns
        -------
        dict
        """
        # obtém os dados de artigo
        migrated = MigratedDocument(article_pid)

        if migrated.isis_doc.file_type != "html":
            return

        # cria ou recupera o registro de documento do website
        document = db.fetch_document(article_pid)
        if not document:
            raise exceptions.DocumentDoesNotExistError(
                "Document %s does not exist" % article_pid
            )

        tracker = Tracker("update_website_document_htmls")

        # cria arquivos html com o conteúdo obtido dos arquivos html e
        # dos registros de parágrafos
        htmls = []
        for text in migrated.html_texts_adapted_for_the_website:
            # obtém os conteúdos de html registrados em `isis_doc`
            file_path = create_temp_file(text["filename"], text["text"])

            # obtém a localização do arquivo no `files storage`
            folder = get_files_storage_folder_for_htmls(
                migrated.journal_pid, migrated.issue_folder
            )
            html = {"lang": text["lang"]}

            try:
                tracker.info(f"publish {file_path}")

                # registra no files storage
                uri = self._files_storage.register(
                    file_path, folder, text["filename"], preserve_name=True
                )
            except Exception as e:
                tracker.error(f"Unable to register {file_path} ({e})")

            # atualiza com a uri o valor de htmls
            html.update({"url": uri})

            htmls.append(html)
        document.htmls = htmls
        tracker.info(f"published: {htmls}")

        # salva os dados
        db.save_data(document)
        return document, tracker

    def update_website_document_xmls(self, article_pid):
        """
        Update the website document xmls

        Get texts from paragraphs records and from translations files
            registered in `isis_doc`
        Build the HTML files and register them in the files storage
        Update the `document.xmls` with lang and uri

        Parameters
        ----------
        article_pid : str

        Returns
        -------
        dict
        """
        # obtém os dados de artigo
        migrated = MigratedDocument(article_pid)

        if migrated.isis_doc.file_type != "xml":
            return

        # cria ou recupera o registro de documento do website
        document = db.fetch_document(article_pid)
        if not document:
            raise exceptions.DocumentDoesNotExistError(
                "Document %s does not exist" % article_pid
            )

        tracker = Tracker("update_website_document_xmls")

        # publica arquivos xml com o conteúdo obtido dos arquivos xml
        for text in migrated.xml_texts:

            try:
                # inclui v3 no XML
                sps_pkg = SPS_Package(text["text"])
                sps_pkg.scielo_pid_v3 = document._id
                sps_pkg.scielo_pid_v2 = document.pid
                if document.aop_pid:
                    sps_pkg.aop_pid = document.aop_pid

                # obtém os idiomas do texto completo
                document.htmls = [{"lang": lang} for lang in sps_pkg.languages]

                # obtém os conteúdos de xml registrados em `isis_doc`
                file_path = create_temp_file(
                    text["filename"], sps_pkg.xml_content)

                # obtém a localização do arquivo no `files storage`
                folder = get_files_storage_folder_for_xmls(
                    migrated.journal_pid, migrated.issue_folder
                )

                tracker.info(f"publish {file_path}")

                # registra no files storage
                document.xml = self._files_storage.register(
                    file_path, folder, text["filename"], preserve_name=True
                )
            except Exception as e:
                tracker.error(e)

        # salva os dados
        db.save_data(document)
        tracker.info(f"published {document.htmls}")
        return document, tracker

    def migrate_document_files(self, article_pid):
        """
        Migrate document files

        Recover the files from
            BASES_XML_PATH,
            BASES_PDF_PATH,
            BASES_TRANSLATION_PATH,
            HTDOCS_IMG_REVISTAS_PATH
        Register them in Files Storage
        Register a zip of these files in Files Storage
        Register the locations in `isis_doc` collection

        Parameters
        ----------
        article_pid : str

        Returns
        -------
        dict
        """
        migrated_document = MigratedDocument(article_pid)

        migrated_document.tracker = Tracker("migrate_document_files")
        migrated_document.files_to_zip = []

        migrated_document.migrate_pdfs(self._files_storage)
        migrated_document.migrate_text_files(self._files_storage)
        migrated_document.migrate_images(self._files_storage)
        migrated_document.migrate_images_from_html(self._files_storage)
        migrated_document.register_migrated_document_files_zipfile(
            self._files_storage
        )
        # salva os dados
        db.save_data(migrated_document.isis_doc)
        return migrated_document.isis_doc, migrated_document.tracker

    def list_documents(self, acron, issue_folder, pub_year, updated_from, updated_to, pid):
        """
        Migrate isis document data to website

        Parameters
        ----------
        pub_year: str
        updated_from: str
        update_to: str

        Returns
        -------
        dict
        """
        # registro migrado formato json
        print(f"{acron}, {issue_folder}, {pub_year}, {updated_from}, {updated_to}")
        docs = []
        if pid:
            docs = db.fetch_isis_document(pid)
            if docs:
                return [docs]
        if acron or issue_folder or pub_year:
            docs = db.get_isis_documents(acron, issue_folder, pub_year)
        elif updated_from or updated_to:
            docs = db.get_isis_documents_by_date_range(
                updated_from, updated_to)
        return docs


def _update_document_with_isis_data(document, migrated_document, issue):
    """
    Update the `document` attributes with `f_document` attributes

    Parameters
    ----------
    document : opac_schema.v1.models.Document
    f_document : dsm.extdeps.isis_migration.friendly_isis.FriendlyISISDocument
    """
    # usa a data de criação do registro no isis como data de criação
    # do registro no site
    # TODO save_data sobrescreve estes comandos, refatorar
    f_document = migrated_document._f_doc

    document.created = db.convert_date(f_document.isis_created_date)
    document.updated = db.convert_date(f_document.isis_updated_date)

    _set_issue_data(document, issue)
    _set_order(document, f_document)
    _set_renditions(document, f_document)
    _set_xml_url(document, migrated_document.isis_doc)
    _set_ids(document, f_document)
    _set_is_public(document, is_public=True)
    _set_languages(document, f_document, migrated_document.isis_doc)
    _set_article_abstracts(document, f_document)
    _set_article_authors(document, f_document)
    _set_article_pages(document, f_document)
    _set_article_publication_date(document, f_document)
    _set_article_sections(document, f_document)
    _set_article_titles(document, f_document)
    _set_article_type(document, f_document)


def _set_order(article, f_document):
    article.order = int(f_document.order)


def _set_renditions(article, f_document):
    article.pdfs = f_document.pdfs


def _set_xml_url(article, migrated_document):
    if migrated_document.xml_files:
        article.xml = migrated_document.xml_files[0].uri


def _set_issue_data(article, issue):
    # TODO verificar se faz sentido neste local
    # if article.issue is not None and article.issue.number == "ahead":
    #     if article.aop_url_segs is None:
    #         url_segs = {
    #             "url_seg_article": article.url_segment,
    #             "url_seg_issue": article.issue.url_segment,
    #         }
    #         article.aop_url_segs = models.AOPUrlSegments(**url_segs)
    article.issue = issue
    article.journal = issue.journal


def _set_is_public(article, is_public=True):
    article.is_public = is_public


def _set_ids(article, f_document):
    article._id = (
        article._id or f_document.scielo_pid_v3 or generates()
    )
    article.aid = article._id

    article_pids = {}
    if f_document.scielo_pid_v1:
        article_pids["v1"] = f_document.scielo_pid_v1
    if f_document.scielo_pid_v2:
        article_pids["v2"] = f_document.scielo_pid_v2
    if f_document.scielo_pid_v3:
        article_pids["v3"] = f_document.scielo_pid_v3
    article.scielo_pids = article_pids

    article.aop_pid = f_document.ahead_of_print_pid
    article.pid = f_document.scielo_pid_v2
    article.doi = f_document.doi

    # TODO
    # doi por idioma / mudanca no opac_schema


def _set_article_type(article, f_document):
    article.type = f_document.article_type


def _set_languages(article, f_document, isis_doc):
    article.original_language = f_document.language
    article.languages = list(set(
        f_document.languages + list(isis_doc.translations.keys())
    ))


def _set_article_titles(article, f_document):
    article.title = f_document.original_title
    set_translate_titles(article, f_document.translated_titles)


def _set_article_sections(article, f_document):
    article.section = f_document.original_section
    set_translated_sections(article, f_document.translated_sections)


def _set_article_abstracts(article, f_document):
    article.abstract = f_document.abstract
    set_abstracts(article, f_document.abstracts)
    set_keywords(article, f_document.keywords_groups)


def _set_article_publication_date(article, f_document):
    article.publication_date = f_document.document_pubdate


def _set_article_pages(article, f_document):
    article.elocation = f_document.elocation_id
    article.fpage = f_document.fpage
    article.fpage_sequence = f_document.fpage_seq
    article.lpage = f_document.lpage


def _set_article_authors(article, f_document):
    set_authors(article, f_document.contrib_group)
    set_authors_meta(article, f_document.contrib_group)


def _update_issue_with_isis_data(issue, f_issue):
    """
    Update the `issue` attributes with `f_issue` attributes
    Parameters
    ----------
    issue : opac_schema.v1.models.Issue
    f_issue : dsm.extdeps.isis_migration.friendly_isis.FriendlyISISIssue
    """
    # TODO issue._id deve ter o mesmo padrão usado no site novo
    # e não o pid de fascículo do site antigo

    issue.journal = db.fetch_journal(f_issue.journal_pid)
    # ReferenceField(Journal, reverse_delete_rule=CASCADE)

    # not available in isis
    # TODO: verificar o uso no site
    # issue.cover_url = f_issue.cover_url

    if f_issue.number == "ahead":
        issue.volume = None
        issue.number = None
    else:
        issue.volume = f_issue.volume
        # TODO: verificar o uso no site | números especiais spe_text
        issue.number = f_issue.number

    # nao presente na base isis
    # TODO: verificar o uso no site
    issue.type = _get_issue_type(f_issue)

    # supplement
    issue.suppl_text = f_issue.suppl

    # nao presente na base isis // tem uso no site?
    # TODO: verificar o uso no site
    issue.spe_text = f_issue.spe_text

    # nao presente na base isis // tem uso no site?
    issue.start_month = f_issue.start_month

    # nao presente na base isis // tem uso no site?
    issue.end_month = f_issue.end_month

    issue.year = int(f_issue.year)
    issue.label = f_issue.issue_folder

    # TODO: no banco do site 20103 como int e isso está incorreto
    # TODO: verificar o uso no site
    # ou fica como str 20130003 ou como int 3
    issue.order = int(f_issue.order)

    issue.is_public = True

    # nao presente na base isis // tem uso no site?
    # issue.unpublish_reason = f_issue.unpublish_reason

    # PID no site antigo
    issue.pid = f_issue.pid

    # isso é atribuído no pre_save do modelo
    # issue.url_segment = f_issue.url_segment

    # nao presente na base isis // tem uso no site?
    # issue.assets_code = f_issue.assets_code

    # ID no site
    issue._id = get_bundle_id(
        issue.journal._id,
        issue.year,
        issue.volume,
        issue.number,
        issue.suppl_text,
    )
    issue.iid = issue._id


def _get_issue_type(f_issue):
    """
    https://github.com/scieloorg/opac-airflow/blob/1064b818fda91f73414a6393d364663bdefa9665/airflow/dags/sync_kernel_to_website.py#L511
    https://github.com/scieloorg/opac_proc/blob/3c6bd66040de596e1af86a99cca6d205bfb79a68/opac_proc/transformers/tr_issues.py#L76
    'ahead', 'regular', 'special', 'supplement', 'volume_issue'
    """
    if f_issue.suppl:
        return "supplement"
    if f_issue.number:
        if f_issue.number == "ahead":
            return "ahead"
        if "spe" in f_issue.number:
            return "special"
        return "regular"
    return "volume_issue"


def _update_journal_with_isis_data(journal, f_journal):
    """
    Update the `journal` attributes with `f_journal` attributes
    Parameters
    ----------
    journal : opac_schema.v1.models.Journal
    f_journal : dsm.extdeps.isis_migration.friendly_isis.FriendlyISISJournal
    """
    journal._id = f_journal._id
    journal.jid = f_journal._id

    # TODO
    # journal.collection = f_journal.attr
    # ReferenceField(Collection, reverse_delete_rule=CASCADE)

    # TODO
    # journal.timeline = f_journal.attr
    # EmbeddedDocumentListField(Timeline)

    journal.subject_categories = f_journal.subject_categories

    journal.study_areas = f_journal.study_areas

    # TODO
    # journal.social_networks = f_journal.attr
    # EmbeddedDocumentListField(SocialNetwork)

    journal.title = f_journal.title

    journal.title_iso = f_journal.iso_abbreviated_title

    # TODO verificar conteúdo
    journal.next_title = f_journal.new_title
    journal.short_title = f_journal.abbreviated_title

    journal.acronym = f_journal.acronym

    journal.scielo_issn = f_journal._id
    journal.print_issn = f_journal.print_issn
    journal.eletronic_issn = f_journal.electronic_issn

    journal.subject_descriptors = f_journal.subject_descriptors
    journal.copyrighter = f_journal.copyright_holder
    journal.online_submission_url = f_journal.online_submission_url

    # ausente na base isis
    # journal.title_slug = f_journal.attr
    # journal.logo_url = f_journal.attr

    # TODO
    if f_journal.current_status == "C":
        journal.current_status = "current"
    journal.editor_email = f_journal.email

    journal.index_at = f_journal.index_at

    journal.is_public = f_journal.publication_status == 'C'

    journal.mission = [
        db.models.Mission(**{"language": lang, "description": text})
        for lang, text in f_journal.mission.items()
    ]

    # TODO EmbeddedDocumentListField(OtherTitle)
    # nao usa no site?
    # journal.other_titles = f_journal.other_titles

    # TODO ID or title
    # journal.previous_journal_ref = f_journal.previous_journal_title

    journal.publisher_address = f_journal.publisher_address
    journal.publisher_city = f_journal.publisher_city
    journal.publisher_state = f_journal.publisher_state
    journal.publisher_country = f_journal.publisher_country

    # TODO ver na base de dados do site
    journal.publisher_name = f_journal.get_publisher_names()

    # journal.scimago_id = f_journal.scimago_id

    journal.sponsors = f_journal.sponsors

    journal.study_areas = f_journal.study_areas

    # journal.timeline = f_journal.timeline

    journal.unpublish_reason = f_journal.unpublish_reason

    # journal.url_segment = f_journal.url_segment


class MigratedDocument:

    def __init__(self, _id):
        self._id = _id
        self._isis_document = db.fetch_isis_document(_id)
        self._isis_issue = db.fetch_isis_issue(_id[1:18])

        if not self.isis_doc:
            raise exceptions.DBFetchDocumentError("%s is not migrated" % _id)
        self._f_doc = friendly_isis.FriendlyISISDocument(
            _id, self.isis_doc.records)
        self._f_doc.issue = friendly_isis.FriendlyISISIssue(
            self._isis_issue._id, self._isis_issue.record)

        self._document_files = DocumentFilesAtOldWebsite(
            os.path.join(
                self.isis_doc.acron, self.isis_doc.issue_folder),
            self.isis_doc.file_name, self._f_doc.language)
        self._files_storage_folder = get_files_storage_folder_for_migration(
            self.journal_pid, self.isis_doc.issue_folder
        )
        self.tracker = None
        self.files_to_zip = []

    @property
    def isis_doc(self):
        return self._isis_document

    @property
    def pub_year(self):
        return self._f_doc.collection_pubdate[:4]

    @property
    def volume(self):
        return self._f_doc.volume

    @property
    def number(self):
        return self._f_doc.number

    @property
    def suppl(self):
        return self._f_doc.suppl

    @property
    def file_name(self):
        return self.isis_doc.file_name

    @property
    def issue_folder(self):
        return self.isis_doc.issue_folder

    @property
    def acron(self):
        return self.isis_doc.acron

    @property
    def journal_pid(self):
        return self.isis_doc.journal_pid

    @property
    def translations(self):
        return self.isis_doc.translations

    @property
    def html_texts(self):
        if self.isis_doc.file_type == "xml":
            return []
        texts = []
        paragraphs = self._f_doc.paragraphs
        text = {
            "lang": self._f_doc.language,
            "filename": self.isis_doc.file_name + ".html",
            "text": paragraphs.text or "",
        }
        texts.append(text)
        for transl_text in self.translated_texts:
            text = {
                "lang": transl_text["lang"],
                "filename": transl_text["filename"],
            }
            text["text"] = transl_text["text"][0]
            text["text"] += paragraphs.references
            if len(transl_text["text"]) > 1:
                text["text"] += transl_text["text"][1]
            texts.append(text)
        return texts

    @property
    def html_texts_adapted_for_the_website(self):
        if self.isis_doc.file_type == "xml":
            return []
        assets_by_lang = {}
        for asset in self.isis_doc.asset_files:
            lang = asset["annotation"]["lang"]
            assets_by_lang.setdefault(lang, [])
            assets_by_lang[lang].append(
                {
                    "elem": asset["annotation"]["elem"],
                    "attr": asset["annotation"]["attr"],
                    "original": asset["annotation"]["original"],
                    "new": asset["uri"],
                }
            )

        for html_text in self.html_texts:
            assets = assets_by_lang.get(html_text["lang"])
            yield {
                "lang": html_text["lang"],
                "text": adapt_html_text_to_website(
                    html_text["text"],
                    assets
                ),
                "filename": html_text["filename"],
            }

    @property
    def xml_texts(self):
        if self.isis_doc.file_type == "html":
            return []
        texts = []
        try:
            content = requests_get_content(self.isis_doc.xml_files[0].uri)
            sps_pkg = SPS_Package(content)
            sps_pkg.local_to_remote(self.isis_doc.asset_files)
            content = sps_pkg.xml_content
        except IndexError as e:
            pass
        except Exception as e:
            # TODO melhorar o tratamento de excecao
            raise
        else:
            text = {
                "lang": self._f_doc.language,
                "filename": self.isis_doc.file_name + ".xml",
                "text": content,
            }
            texts.append(text)
        return texts

    @property
    def translated_texts(self):
        texts = []
        translations_files = self._document_files.bases_translation_files_paths.items()
        for lang, paths in translations_files:
            text = {}
            text["lang"] = lang
            text["text"] = []
            text["filename"] = paths[0]
            for path in paths:
                print("path", path)
                content = read_file(path, encoding="iso-8859-1")
                text["text"].append(content or '')
            texts.append(text)
        return texts

    def save(self):
        # salva o documento
        return db.save_data(self.isis_doc)

    @property
    def original_pdf_paths(self):
        """
        Obtém os arquivos PDFs do documento da pasta BASES_PDF_PATH
        """
        for lang, pdf_path in self._document_files.bases_pdf_files_paths.items():
            yield {
                "path": pdf_path,
                "lang": lang,
                "basename": os.path.basename(pdf_path)
            }

    def _migrate_document_file(self, files_storage, file_path):
        basename = os.path.basename(file_path)

        self.tracker.info(f"migrate {file_path}")

        # identificar para inserir no zip do pacote
        self.files_to_zip.append(file_path)

        try:
            # registra o arquivo na nuvem
            remote = files_storage.register(
                file_path, self._files_storage_folder,
                basename, preserve_name=True)

            self.tracker.info(f"migrated {remote}")
        except Exception as e:
            self.tracker.error(e)
        else:
            return db.create_remote_and_local_file(remote, basename)

    def migrate_pdfs(self, files_storage):
        """
        Obtém os arquivos PDFs do documento da pasta BASES_PDF_PATH
        Registra os arquivos na nuvem
        Atualiza os dados de PDF de `isis_document`
        """
        pdfs = {}
        _uris_and_names = []

        for pdf in self.original_pdf_paths:
            file_path = pdf["path"]
            lang = pdf["lang"]

            pdfs[lang] = pdf["basename"]
            migrated = self._migrate_document_file(files_storage, file_path)
            if migrated:
                _uris_and_names.append(migrated)

        self.isis_doc.pdfs = pdfs
        self.isis_doc.pdf_files = _uris_and_names

    @property
    def migrated_pdfs(self):
        # url, filename, type, lang
        _pdfs = []
        uris = {
            item.name: item.uri
            for item in self.isis_doc.pdf_files
        }
        for lang, name in self.isis_doc.pdfs.items():
            _pdfs.append(
                {
                    "lang": lang,
                    "filename": name,
                    "url": uris.get(name),
                    "type": "pdf",
                }
            )
        return _pdfs

    def migrate_images(self, files_storage):
        """
        Obtém os arquivos de imagens do documento da pasta HTDOCS_IMG_REVISTAS_PATH
        Registra os arquivos na nuvem
        Atualiza os dados de imagens de `isis_document`
        """
        if self.isis_doc.file_type != "xml":
            return
        _files = []
        _uris_and_names = []
        images = self._document_files.htdocs_img_revistas_files_paths
        for file_path in images:
            self.tracker.info(f"migrate {file_path}")

            name = os.path.basename(file_path)
            _files.append(name)

            # identificar para inserir no zip do pacote
            self.files_to_zip.append(file_path)

            # registra no files storage
            remote = files_storage.register(
                file_path, self._files_storage_folder,
                name, preserve_name=True)
            _uris_and_names.append(
                db.create_remote_and_local_file(remote, name)
            )
            self.tracker.info(f"migrated {remote}")
        self.isis_doc.assets = _files
        self.isis_doc.asset_files = _uris_and_names

    def migrate_images_from_html(self, files_storage):
        """
        Parse HTML content to get src / href
        Obtém os arquivos de imagens do documento da pasta HTDOCS_IMG_REVISTAS_PATH
        Registra os arquivos na nuvem
        Atualiza os dados de imagens de `isis_document`

        Some images in html content might be located in an unexpected path,
        such as, /img/revista/acron/nahead/, althouth the document is not aop
        anymore.
        Some images might have not same preffix name as the main html file,
        for instance, main file name is a01.htm, their images can be named as
        a1f1.
        This attribute have to parse the HTML and recover the images from
        /img/revista/ located in unexpected folders and with unexpected file
        names.

        Returns
        -------
        dict
        """
        if self.isis_doc.file_type != "html":
            return
        htmls = []
        _files = []
        _uris_and_names = []

        expected_file_paths = []
        not_found_file_paths = []

        HTDOCS_PATH = get_htdocs_path()
        for text in self.html_texts:
            if not text["text"]:
                self.tracker.error(
                    f"html {text['filename']} ({text['lang']}) is empty")
                continue

            assets_in_html = get_assets_locations(text["text"])
            for asset in assets_in_html:
                # fullpath
                subdir = asset["path"]
                if subdir.startswith("/"):
                    subdir = subdir[1:]
                file_path = os.path.join(HTDOCS_PATH, subdir)

                self.tracker.info(f"migrate {file_path}")

                expected_file_paths.append(file_path)
                if not os.path.isfile(file_path):
                    not_found_file_paths.append(file_path)
                    self.tracker.error(f"Unable to find {file_path}")
                    continue

                # basename
                name = os.path.basename(file_path)
                _files.append(name)

                # identificar para inserir no zip do pacote
                self.files_to_zip.append(file_path)

                # registra no files storage
                remote = files_storage.register(
                    file_path, self._files_storage_folder,
                    name, preserve_name=True)
                annotation = {
                    "original": asset["link"],
                    "elem": asset["elem"].tag,
                    "attr": asset["attr"],
                    "lang": text["lang"],
                }
                _uris_and_names.append(
                    db.create_remote_and_local_file(remote, name, annotation)
                )
                self.tracker.info(f"migrated {remote}")

        self.isis_doc.assets = _files
        self.isis_doc.asset_files = _uris_and_names

    def migrate_text_files(self, files_storage):
        """
        Obtém os arquivos que correspondem aos textos completos das pastas
        BASES_XML_PATH,
        BASES_TRANSLATION_PATH,
        Registra os arquivos na nuvem
        Atualiza os dados de texto completo de `isis_document`
        """
        translations_locations = []
        xml_location = []
        _uris_and_names = []

        self.isis_doc.translations = {}
        self.isis_doc.xml_files = []
        self.isis_doc.html_files = []

        if self.isis_doc.file_type == "xml":
            xml = self._document_files.bases_xml_file_path
            self.tracker.info(f"migrate {xml}")
            if xml:
                xml_location.append(xml)
                name = os.path.basename(xml)

                # identificar para inserir no zip do pacote
                self.files_to_zip.append(xml)

                # registra no files storage
                remote = files_storage.register(
                    xml, self._files_storage_folder,
                    name, preserve_name=True)
                _uris_and_names.append(
                    db.create_remote_and_local_file(remote, name)
                )
                self.isis_doc.xml_files = _uris_and_names
                self.tracker.info(f"migrated {xml}")
            else:
                self.tracker.error(f"Not found {xml}")
        else:
            # HTML Traduções
            translations_locations = []
            _translations = {}
            translations_files = self._document_files.bases_translation_files_paths.items()
            for lang, paths in translations_files:
                translations_locations.extend(paths)
                _translations[lang] = []
                for path in paths:
                    self.tracker.info(f"migrate {path} ({lang})")
                    name = os.path.basename(path)
                    _translations[lang].append(name)

                    # identificar para inserir no zip do pacote
                    self.files_to_zip.append(path)

                    # registra no files storage
                    remote = files_storage.register(
                        path, self._files_storage_folder,
                        name, preserve_name=True)
                    _uris_and_names.append(
                        db.create_remote_and_local_file(remote, name)
                    )
                    self.tracker.info(f"migrated {path} ({lang})")
            self.isis_doc.html_files = _uris_and_names
            self.isis_doc.translations = _translations

    def register_migrated_document_files_zipfile(self, files_storage):
        # create zip file with document files
        self.tracker.info(f"total of files to zip: {len(self.files_to_zip)}")
        zip_file_path = create_zip_file(
            self.files_to_zip, self.isis_doc.file_name + ".zip")
        remote = files_storage.register(
            zip_file_path,
            self._files_storage_folder,
            os.path.basename(zip_file_path),
            preserve_name=True)
        self.isis_doc.zipfile = db.create_remote_and_local_file(
            remote=remote, local=os.path.basename(zip_file_path))
        self.tracker.info(f"total of zipped files: {len(self.files_to_zip)}")
        return zip_file_path
