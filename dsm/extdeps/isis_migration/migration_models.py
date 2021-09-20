# coding: utf-8
from datetime import datetime
from mongoengine import (
    Document,
    EmbeddedDocument,
    # campos:
    StringField,
    DateTimeField,
    EmbeddedDocumentField,
    EmbeddedDocumentListField,
    URLField,
    DictField,
    ListField,
    Q,
    DecimalField,
)
from opac_schema.v2.models import RemoteAndLocalFile


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
    files_to_migrate = DictField()
    files_migration_progress = DecimalField()
    files_to_publish = DictField()
    published_files_progress = DecimalField()

    # status do documento quanto ao registro parágrafos, se aplicável
    p_records_status = DictField()
    refs_in_p_records = DecimalField()

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
            'files_migration_progress',
            'published_files_progress',
            'p_records_status',
            'refs_in_p_records',
        ]
    }

    @property
    def journal_pid(self):
        return self.id[1:10]

    def update_files_to_migrate(self):
        status = {}
        status.update(self.pdfs_status)
        status.update(self.assets_status)
        status.update(self.xml_status or {})
        status.update(self.translations_status or {})
        self.files_to_migrate = status

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

    def update_p_records_status(self):
        """
        Status of translations files to migrate
        """
        if self.file_type != "html":
            self.refs_in_p_records = 1.0
            self.p_records_status = {}
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

        self.refs_in_p_records = 0
        if total_c:
            self.refs_in_p_records = total_ref_in_p / total_c

        items["p_records"] = total_p
        items["c_records"] = total_c
        items["ref_in_p_records"] = total_ref_in_p
        self.p_records_status = items

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
        if not self.files_to_publish:
            self.files_to_publish = {}
        if file_type == "xml":
            self.files_to_publish.update({"published xml": 1 if data else 0})
        elif file_type == "html":
            published_htmls = {}
            for lang in list(self.translations.keys()) + [self.get_language()]:
                if lang:
                    published_htmls[f"published html ({lang})"] = 0
            for item in data:
                if item["url"]:
                    published_htmls[f"published html ({item['lang']})"] = 1
            self.files_to_publish.update(published_htmls)

    def save(self, *args, **kwargs):
        # arquivos para migrar
        self.update_files_to_migrate()
        # percentagem migrada
        self.files_migration_progress = 0
        if self.files_to_migrate:
            self.files_migration_progress = (
                sum(self.files_to_migrate.values()) /
                len(self.files_to_migrate)
            )
        # arquivos XML e HTML publicados contendo textos completos
        published_files_expected = 1
        if self.file_type == "html":
            published_files_expected += 1 + len(self.translations)
        # percentagem publicada
        self.published_files_progress = (
            sum(self.files_to_publish.values()) / published_files_expected
        )

        # update record status
        self.update_p_records_status()

        if (self.files_migration_progress == 1.0 and
                self.published_files_progress == 1.0):
            self.status = "published_complete"

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
