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
    status = StringField()

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

    def save(self, *args, **kwargs):
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
