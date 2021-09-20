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
    files_quality = DecimalField()
    # status do documento quanto ao registro parágrafos, se aplicável
    p_records_quality = DecimalField()
    # detailed status
    detailed_status = DictField()

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
        ],
    }

    @property
    def journal_pid(self):
        return self.id[1:10]

    def set_detailed_status(self):
        status = {"metadata": self.status}
        status.update(self.files_status)
        status.update(self.records_status)
        self.detailed_status = status

    @property
    def files_status(self):
        status = {}
        status.update(self.pdfs_status)
        status.update(self.assets_status)
        status.update(self.xml_status or {})
        status.update(self.translations_status or {})
        return status

    @property
    def xml_status(self):
        if self.file_type != "xml":
            return None
        if self.xml_files and self.xml_files[0].uri:
            found = 1
        else:
            found = 0
        return {self.file_name + ".xml": found}

    @property
    def pdfs_status(self):
        items = {}
        for lang, filename in self.pdfs.items():
            items[filename] = False
        for pdf in self.pdf_files:
            if pdf["uri"]:
                items[pdf["name"]] = 1
        return items

    @property
    def assets_status(self):
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

    @property
    def records_status(self):
        total_p = 0
        total_ref_in_p = 0
        total_c = 0
        items = {}
        for record in self.records:
            rec_type = record.get("v706")
            if rec_type == "p":
                total_p += 1
                if record.get("v888"):
                    total_ref_in_p += 1
            elif rec_type == "c":
                total_c += 1
        items["p_records"] = total_p
        if total_c:
            items["ref_in_p_records"] = total_ref_in_p
            items["c_records"] = total_c
            items["ref_in_p_records / c_records"] = total_ref_in_p / total_c
        return items

    def eval_status(self):
        expected = len(self.files_status)
        done = sum(self.files_status.values())
        self.files_quality = done / expected

        if self.file_type == "html":
            records_status = self.records_status
            expected = 1
            done = 1 if records_status["p_records"] else 0
            if records_status.get("c_records"):
                expected += records_status.get("c_records")
                done += records_status.get("ref_in_p_records")
            self.p_records_quality = done / expected

    def save(self, *args, **kwargs):
        self.eval_status()
        self.set_detailed_status()
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
