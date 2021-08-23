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
)
from opac_schema.v2.models import RemoteAndLocalFile


class MigratedDoc(Document):
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
    zipfile = EmbeddedDocumentField(RemoteAndLocalFile)

    meta = {
        'collection': 'migrated_doc',
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

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = datetime.now()
        self.updated = datetime.now()
        return super(MigratedDoc, self).save(*args, **kwargs)

    def __unicode__(self):
        return '%s' % self._id


class MigratedJournal(Document):
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
        'collection': 'migrated_journal',
        'indexes': [
            'updated',
            'isis_updated_date',
        ],
    }

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = datetime.now()
        self.updated = datetime.now()
        return super(MigratedJournal, self).save(*args, **kwargs)

    def __unicode__(self):
        return '%s' % self._id


class MigratedIssue(Document):
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
        'collection': 'migrated_issue',
        'indexes': [
            'updated',
            'isis_updated_date',
        ],
    }

    def save(self, *args, **kwargs):
        if not self.created:
            self.created = datetime.now()
        self.updated = datetime.now()
        return super(MigratedIssue, self).save(*args, **kwargs)

    def __unicode__(self):
        return '%s' % self._id
