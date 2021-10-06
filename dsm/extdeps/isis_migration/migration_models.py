# coding: utf-8
from datetime import datetime
from mongoengine import (
    Document,
    # campos:
    StringField,
    DateTimeField,
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    DictField,
    ListField,
    Q,
    DecimalField,
)
from opac_schema.v2.models import RemoteAndLocalFile


_MIGRATION_STATUS = dict(
    PENDING_MIGRATION={
        "value": 'pending_migration',
        'help': 'only pid was registered',
    },
    ISIS_METADATA_MIGRATED={
        "value": 'isis_metadata_migrated',
        'help': 'isis metadata registered',
    },
    PUBLISHED_INCOMPLETE={
        "value": 'published_incomplete',
        'help': 'published but missing any files',
    },
    PUBLISHED_COMPLETE={
        "value": 'published_complete',
        'help': 'totally migrated and published',
    },
)


def get_migration_status(STATUS):
    try:
        return _MIGRATION_STATUS[STATUS]['value']
    except KeyError:
        raise ValueError("Invalid value for MIGRATION STATUS")


def get_list_documents_status_arg_help():
    help = " | ".join([
        f"{item['value']} ({item['help']})"
        for item in _MIGRATION_STATUS.values()
    ])
    return f'status: {help}'


def _get_value(data, tag):
    """
    Returns first value of field `tag`
    """
    # data['v880'][0]['_']
    try:
        return data[tag][0]['_']
    except (KeyError, IndexError):
        return None


class ISISDocument(Document):
    """
    Armazena documento (artigo) migrado
    """
    _id = StringField(max_length=32, primary_key=True, required=True)
    doi = StringField()
    pub_year = StringField()

    # datas no registro da base isis para identificar
    # se houve mudança durante a migração
    isis_updated_date = StringField()
    isis_created_date = StringField()

    # registro no formato json correspondente ao conteúdo da base isis
    records = ListField(DictField())

    # data de criação e atualização da migração
    created = DateTimeField()
    updated = DateTimeField()

    # status do registro quanto aos metadados
    status = StringField()

    # status do documento quanto aos arquivos
    tracked_files_to_migrate = DictField()
    tracked_files_migration_progress = DecimalField()
    tracked_files_to_publish = DictField()
    tracked_files_publication_progress = DecimalField()

    # status do documento quanto ao registro parágrafos, se aplicável
    tracked_p_records = DictField()
    tracked_refs_in_p_records = DecimalField()

    # dados dos arquivos do documento
    file_name = StringField()
    file_type = StringField()
    acron = StringField()
    issue_folder = StringField()

    assets = ListField()
    translations = DictField()
    pdfs = DictField()

    asset_files = EmbeddedDocumentListField(RemoteAndLocalFile)
    html_files = EmbeddedDocumentListField(RemoteAndLocalFile)
    pdf_files = EmbeddedDocumentListField(RemoteAndLocalFile)
    xml_files = EmbeddedDocumentListField(RemoteAndLocalFile)

    zipfile = EmbeddedDocumentField(RemoteAndLocalFile)

    meta = {
        'collection': 'isis_doc',
        'indexes': [
            'updated',
            'isis_updated_date',
            'doi',
            'records',
            'pub_year',
            'file_name',
            'file_type',
            'pdfs',
            'acron',
            'issue_folder',
            'assets',
            'translations',
            'status',
            'tracked_files_migration_progress',
            'tracked_files_publication_progress',
            'tracked_p_records',
            'tracked_refs_in_p_records',
        ]
    }

    @property
    def journal_pid(self):
        return self.id[1:10]

    def update_status(self, STATUS):
        self.status = get_migration_status(STATUS)

    def update_tracked_files_to_migrate(self):
        status = {}
        status.update(self.pdfs_status)
        status.update(self.assets_status)
        status.update(self.xml_status or {})
        status.update(self.translations_status or {})
        self.tracked_files_to_migrate = status

        # percentagem migrada
        self.tracked_files_migration_progress = 0
        if self.tracked_files_to_migrate:
            self.tracked_files_migration_progress = (
                sum(self.tracked_files_to_migrate.values()) /
                len(self.tracked_files_to_migrate)
            )

    def update_tracked_files_to_publish(self):
        # 1 pelo menos porque todos os documentos deveriam ter o XML
        published_files_expected = 1

        if self.file_type == "html":
            # 1 para o idioma do texto original em registro tipo p
            # mais quantidade de traduções
            published_files_expected += 1 + len(self.translations)

        # percentagem publicada
        self.tracked_files_publication_progress = (
            sum(self.tracked_files_to_publish.values()) /
            published_files_expected
        )

    @property
    def xml_status(self):
        """
        Status of XML file to migrate
        """
        if self.file_type != "xml":
            return None
        if self.xml_files and self.xml_files[0].uri:
            found = 1
        else:
            found = 0
        return {self.file_name + ".xml": found}

    @property
    def pdfs_status(self):
        """
        Status of pdfs files to migrate
        """
        items = {}
        for lang, filename in self.pdfs.items():
            items[filename] = False
        for pdf in self.pdf_files:
            if pdf["uri"]:
                items[pdf["name"]] = 1
        return items

    @property
    def assets_status(self):
        """
        Status of assets files to migrate
        """
        items = {
            k: 0
            for k in self.assets
        }
        for asset in self.asset_files:
            if asset["uri"]:
                items[asset["name"]] = 1
        return items

    @property
    def translations_status(self):
        """
        Status of translations files to migrate
        """
        if self.file_type != "html":
            return None
        if not self.translations:
            return None
        items = {}
        for lang, filename in self.translations.items():
            for part in filename:
                items[part] = 0
        for html in self.html_files:
            if html["uri"]:
                items[html["name"]] = 1
        return items

    def update_tracked_p_records(self):
        """
        Status of translations files to migrate
        """
        if self.file_type != "html":
            self.tracked_refs_in_p_records = 1.0
            self.tracked_p_records = {}
            return

        total_p = 0
        total_ref_in_p = 0
        total_c = 0
        items = {}
        for record in self.records:
            rec_type = _get_value(record, "v706")
            if rec_type == "p":
                total_p += 1
                if _get_value(record, "v888"):
                    total_ref_in_p += 1
            elif rec_type == "c":
                total_c += 1

        self.tracked_refs_in_p_records = 0
        if total_c:
            self.tracked_refs_in_p_records = total_ref_in_p / total_c

        items["p_records"] = total_p
        items["c_records"] = total_c
        items["ref_in_p_records"] = total_ref_in_p
        self.tracked_p_records = items

    def get_language(self):
        for record in self.records:
            rec_type = _get_value(record, "v706")
            if rec_type == "h":
                return _get_value(record, "v040")

    def mark_as_published(self, file_type, data):
        """
        Documentos de texto completo que devem ser publicados
        """
        # xml
        if not self.tracked_files_to_publish:
            self.tracked_files_to_publish = {}
        if file_type == "xml":
            self.tracked_files_to_publish.update({"published xml": 1 if data else 0})
        elif file_type == "html":
            published_htmls = {}
            for lang in list(self.translations.keys()) + [self.get_language()]:
                if lang:
                    published_htmls[f"published html ({lang})"] = 0
            for item in data:
                if item["url"]:
                    published_htmls[f"published html ({item['lang']})"] = 1
            self.tracked_files_to_publish.update(published_htmls)

    def save(self, *args, **kwargs):
        # update files migration status
        self.update_tracked_files_to_migrate()

        # update files migration status
        self.update_tracked_files_to_publish()

        # update p records status
        self.update_tracked_p_records()

        # update migration status
        if (self.tracked_files_migration_progress == 1.0 and
                self.tracked_files_publication_progress == 1.0):
            self.update_status("PUBLISHED_COMPLETE")

        # dates
        self.updated = datetime.utcnow()
        if not self.created:
            self.created = self.updated
        return super(ISISDocument, self).save(*args, **kwargs)

    def __unicode__(self):
        return '%s' % self._id


class ISISJournal(Document):
    """
    Armazena journal migrado
    """
    _id = StringField(max_length=32, primary_key=True, required=True)

    # datas no registro da base isis para identificar
    # se houve mudança durante a migração
    isis_updated_date = StringField()
    isis_created_date = StringField()

    # registro no formato json correspondente ao conteúdo da base isis
    record = DictField()

    # data de criação e atualização da migração
    created = DateTimeField()
    updated = DateTimeField()

    meta = {
        'collection': 'isis_journal',
        'indexes': [
            'updated',
            'isis_updated_date',
        ],
    }

    def save(self, *args, **kwargs):
        self.updated = datetime.utcnow()
        if not self.created:
            self.created = self.updated
        return super(ISISJournal, self).save(*args, **kwargs)

    def __unicode__(self):
        return '%s' % self._id


class ISISIssue(Document):
    """
    Armazena issue migrado
    """
    _id = StringField(max_length=32, primary_key=True, required=True)

    # datas no registro da base isis para identificar
    # se houve mudança durante a migração
    isis_updated_date = StringField()
    isis_created_date = StringField()

    # registro no formato json correspondente ao conteúdo da base isis
    record = DictField()

    # data de criação e atualização da migração
    created = DateTimeField()
    updated = DateTimeField()

    meta = {
        'collection': 'isis_issue',
        'indexes': [
            'updated',
            'isis_updated_date',
        ],
    }

    def save(self, *args, **kwargs):
        self.updated = datetime.utcnow()
        if not self.created:
            self.created = self.updated
        return super(ISISIssue, self).save(*args, **kwargs)

    def __unicode__(self):
        return '%s' % self._id


def get_isis_documents_to_migrate(
        acron, issue_folder, pub_year, isis_updated_from, isis_updated_to,
        status=None, descending=None, page_number=None, items_per_page=None
        ):

    names = ('acron', 'issue_folder', 'pub_year', 'status')
    values = (acron, issue_folder, pub_year, status)

    descending = descending or True
    items_per_page = items_per_page or 50
    page_number = page_number or 1

    skip = ((page_number - 1) * items_per_page)
    limit = items_per_page

    order_by = '-isis_updated_date' if descending else '+isis_updated_date'

    q = {
        name: value
        for name, value in zip(names, values)
        if value
    }

    if isis_updated_from and isis_updated_to:
        return ISISDocument.objects(
            Q(isis_updated_date__gte=isis_updated_from) &
            Q(isis_updated_date__lte=isis_updated_to),
            **q
        ).order_by(order_by).skip(skip).limit(limit)
    if isis_updated_from:
        return ISISDocument.objects(
            Q(isis_updated_date__gte=isis_updated_from),
            **q
        ).order_by(order_by).skip(skip).limit(limit)
    if isis_updated_to:
        return ISISDocument.objects(
            Q(isis_updated_date__lte=isis_updated_to),
            **q
        ).order_by(order_by).skip(skip).limit(limit)

    return ISISDocument.objects(
            **q
        ).order_by(order_by).skip(skip).limit(limit)
