from dsm.core.issue import get_bundle_id
from dsm.extdeps.isis_migration import json2doc
from dsm.extdeps import db


class MigrationManager:

    def __init__(self, db_url):
        """
        Instancia objeto da classe MigrationManager

        Parameters
        ----------
        db_url : str
            Data to connect to a mongodb. Expected pattern:
                "mongodb://my_user:my_password@127.0.0.1:27017/my_db"
        """
        self._db_url = db_url

    def db_connect(self):
        db.mk_connection(self._db_url)

    def register_document(self, _id, records):
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
        # recupera migrated_documento ou cria se não existir

        doc = json2doc.Document(_id, records)
        migrated_document = (
            db.fetch_migrated_document(_id) or
            db.create_migrated_document()
        )
        migrated_document._id = doc._id
        migrated_document.doi = doc.doi
        migrated_document.isis_updated_date = doc.isis_updated_date
        migrated_document.isis_created_date = doc.isis_created_date
        migrated_document.records = doc.records

        # salva o documento
        return db.save_data(migrated_document)

    def register_journal(self, _id, record):
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
        # recupera migrated_journal ou cria se não existir

        journal = json2doc.Journal(_id, record)
        migrated_journal = (
            db.fetch_migrated_journal(_id) or
            db.create_migrated_journal()
        )
        migrated_journal._id = journal._id
        migrated_journal.isis_updated_date = journal.isis_updated_date
        migrated_journal.isis_created_date = journal.isis_created_date
        migrated_journal.record = journal.record

        # salva o journal
        return db.save_data(migrated_journal)

    def register_issue(self, _id, record):
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
        # recupera migrated_issue ou cria se não existir

        issue = json2doc.Issue(_id, record)
        migrated_issue = (
            db.fetch_migrated_issue(issue._id) or
            db.create_migrated_issue()
        )
        migrated_issue._id = issue._id
        migrated_issue.isis_updated_date = issue.isis_updated_date
        migrated_issue.isis_created_date = issue.isis_created_date
        migrated_issue.record = issue.record

        # salva o issue
        return db.save_data(migrated_issue)

    def migrate_journal(self, journal_id):
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
        migrated_journal = db.fetch_migrated_journal(journal_id)

        # interface mais amigável para obter os dados
        _migrated_journal = json2doc.Journal(
            migrated_journal._id, migrated_journal.record)

        # cria ou recupera o registro do website
        journal = (
            db.fetch_journal(journal_id) or db.create_journal()
        )

        # atualiza os dados
        _migrate_journal_data(journal, _migrated_journal)

        # salva os dados
        return db.save_data(journal)

    def migrate_issue(self, issue_id):
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
        migrated_issue = db.fetch_migrated_issue(issue_id)

        # interface mais amigável para obter os dados
        _migrated_issue = json2doc.Issue(
            migrated_issue.id, migrated_issue.record)

        # cria ou recupera o registro do website
        issue = db.fetch_issue(issue_id) or db.create_issue()

        # atualiza os dados
        _migrate_issue_data(issue, _migrated_issue)

        # salva os dados
        return db.save_data(issue)


def _migrate_issue_data(issue, migrated_issue):
    """
    Update the `issue` attributes with `migrated_issue` attributes
    Parameters
    ----------
    issue : opac_schema.v1.models.Issue
    migrated_issue : dsm.extdeps.isis_migration.json2doc.Issue
    """
    # TODO issue._id deve ter o mesmo padrão usado no site novo
    # e não o pid de fascículo do site antigo
    issue.iid = migrated_issue.iid

    issue.journal = db.fetch_journal(migrated_issue.pid[:9])
    # ReferenceField(Journal, reverse_delete_rule=CASCADE)

    # not available in isis
    # TODO: verificar o uso no site
    # issue.cover_url = migrated_issue.cover_url

    if migrated_issue.number == "ahead":
        issue.volume = None
        issue.number = None
    else:
        issue.volume = migrated_issue.volume
        # TODO: verificar o uso no site | números especiais spe_text
        issue.number = migrated_issue.number

    # nao presente na base isis
    # TODO: verificar o uso no site
    # issue.type = migrated_issue.type

    # supplement
    issue.suppl_text = migrated_issue.suppl

    # nao presente na base isis // tem uso no site?
    # TODO: verificar o uso no site
    issue.spe_text = migrated_issue.spe_text

    # nao presente na base isis // tem uso no site?
    issue.start_month = migrated_issue.start_month

    # nao presente na base isis // tem uso no site?
    issue.end_month = migrated_issue.end_month

    issue.year = int(migrated_issue.year)
    issue.label = migrated_issue.label

    # TODO: no banco do site 20103 como int e isso está incorreto
    # TODO: verificar o uso no site
    # ou fica como str 20130003 ou como int 3
    issue.order = int(migrated_issue.order)

    issue.is_public = migrated_issue.is_public

    # nao presente na base isis // tem uso no site?
    # issue.unpublish_reason = migrated_issue.unpublish_reason

    # PID no site antigo
    issue.pid = migrated_issue.pid

    # isso é atribuído no pre_save do modelo
    # issue.url_segment = migrated_issue.url_segment

    # nao presente na base isis // tem uso no site?
    # issue.assets_code = migrated_issue.assets_code

    # ID no site
    issue._id = get_bundle_id(
        issue.journal,
        issue.year,
        issue.volume,
        issue.number,
        issue.suppl,
    )


def _migrate_journal_data(journal, migrated_journal):
    """
    Update the `journal` attributes with `migrated_journal` attributes
    Parameters
    ----------
    journal : opac_schema.v1.models.Journal
    migrated_journal : dsm.extdeps.isis_migration.json2doc.Journal
    """
    journal._id = migrated_journal._id
    journal.jid = migrated_journal._id

    # TODO
    # journal.collection = migrated_journal.attr
    # ReferenceField(Collection, reverse_delete_rule=CASCADE)

    # TODO
    # journal.timeline = migrated_journal.attr
    # EmbeddedDocumentListField(Timeline)

    journal.subject_categories = migrated_journal.subject_categories

    journal.study_areas = migrated_journal.study_areas

    # TODO
    # journal.social_networks = migrated_journal.attr
    # EmbeddedDocumentListField(SocialNetwork)

    journal.title = migrated_journal.title

    journal.title_iso = migrated_journal.iso_abbreviated_title

    # TODO verificar conteúdo
    journal.next_title = migrated_journal.new_title
    journal.short_title = migrated_journal.abbreviated_title

    journal.acronym = migrated_journal.acronym

    journal.scielo_issn = migrated_journal._id
    journal.print_issn = migrated_journal.print_issn
    journal.eletronic_issn = migrated_journal.electronic_issn

    journal.subject_descriptors = migrated_journal.subject_descriptors
    journal.copyrighter = migrated_journal.copyright_holder
    journal.online_submission_url = migrated_journal.online_submission_url

    # ausente na base isis
    # journal.title_slug = migrated_journal.attr
    # journal.logo_url = migrated_journal.attr

    # TODO
    # journal.current_status = migrated_journal.current_status
    journal.editor_email = migrated_journal.email

    journal.index_at = migrated_journal.index_at

    journal.is_public = migrated_journal.publication_status == 'C'

    journal.mission = [
        db.models.Mission(**{"language": lang, "description": text})
        for lang, text in migrated_journal.mission.items()
    ]

    # TODO EmbeddedDocumentListField(OtherTitle)
    # nao usa no site?
    # journal.other_titles = migrated_journal.other_titles

    # TODO ID or title
    #journal.previous_journal_ref = migrated_journal.previous_journal_title

    journal.publisher_address = migrated_journal.publisher_address
    journal.publisher_city = migrated_journal.publisher_city
    journal.publisher_state = migrated_journal.publisher_state
    journal.publisher_country = migrated_journal.publisher_country

    # TODO ver na base de dados do site
    journal.publisher_name = migrated_journal.get_publisher_names()

    # journal.scimago_id = migrated_journal.scimago_id

    journal.sponsors = migrated_journal.sponsors

    journal.study_areas = migrated_journal.study_areas

    # journal.timeline = migrated_journal.timeline

    journal.unpublish_reason = migrated_journal.unpublish_reason

    # journal.url_segment = migrated_journal.url_segment
