from datetime import datetime
from mongoengine import connect
from opac_schema.v1 import models
from opac_schema.v2 import models as v2_models
from dsm import exceptions
from dsm.extdeps.isis_migration.migration_models import (
    MigratedDoc,
    MigratedJournal,
    MigratedIssue,
)


def convert_date(date):
    """Traduz datas em formato YYYYMMDD ou ano-mes-dia, ano-mes para
    o formato usado no banco de dados"""

    _date = None
    if "-" not in date:
        date = "-".join((date[:4], date[4:6], date[6:]))

    def limit_month_range(date):
        """Remove o segundo mês da data limitando o intervalo de criaçã
        >>> limit_month_range("2018-Oct-Dec")
        >>> "2018-Dec"
        """
        parts = [part for part in date.split("-") if len(part.strip()) > 0]
        return "-".join([parts[0], parts[-1]])

    def remove_invalid_date_parts(date):
        """Remove partes inválidas de datas e retorna uma data válida
        >>> remove_invalid_date_parts("2019-12-100")
        >>> "2019-12"
        >>> remove_invalid_date_parts("2019-20-01")
        >>> "2019" # Não faz sentido utilizar o dia válido após um mês inválido
        """
        date = date.split("-")
        _date = []

        for index, part in enumerate(date):
            if len(part) == 0 or part == "00" or part == "0":
                break
            elif index == 1 and part.isnumeric() and int(part) > 12:
                break
            elif index == 2 and part.isnumeric() and int(part) > 31:
                break
            elif part.isdigit():
                part = str(int(part))
            _date.append(part)

        return "-".join(_date)

    formats = [
        ("%Y-%m-%d", lambda x: x),
        ("%Y-%m", lambda x: x),
        ("%Y", lambda x: x),
        ("%Y-%b-%d", lambda x: x),
        ("%Y-%b", lambda x: x),
        ("%Y-%B", lambda x: x),
        ("%Y-%B-%d", lambda x: x),
        ("%Y-%B", remove_invalid_date_parts),
        ("%Y-%b", limit_month_range),
        ("%Y-%m-%d", remove_invalid_date_parts),
        ("%Y-%m", remove_invalid_date_parts),
        ("%Y", remove_invalid_date_parts),
    ]

    for template, func in formats:
        print(template, date)
        try:
            _date = (
                datetime.strptime(func(date.strip()), template).isoformat(
                    timespec="microseconds"
                )
                + "Z"
            )
        except ValueError:
            continue
        else:
            return _date

    raise ValueError("Could not transform date '%s' to ISO format" % date) from None    


def mk_connection(host):
    try:
        connect(host=host)
    except Exception as e:
        raise exceptions.DBConnectError(e)


def fetch_document(any_doc_id, **kwargs):
    try:
        articles = models.Article.objects(
            pk=any_doc_id, **kwargs)
        if not articles:
            articles = models.Article.objects(
                pid=any_doc_id, **kwargs)
        if not articles:
            articles = models.Article.objects(
                aop_pid=any_doc_id, **kwargs)
        if not articles:
            articles = models.Article.objects(
                scielo_pids__other=any_doc_id, **kwargs)
        if not articles:
            articles = models.Article.objects(
                doi=any_doc_id, **kwargs)
        return articles and articles[0]
    except Exception as e:
        raise exceptions.DBFetchDocumentError(e)


def _fetch_record(_id, model, **kwargs):
    try:
        obj = model.objects(_id=_id, **kwargs)[0]
    except IndexError:
        return None
    except Exception as e:
        raise exceptions.DBFetchMigratedDocError(e)
    else:
        return obj


def fetch_journal(_id, **kwargs):
    return _fetch_record(_id, models.Journal, **kwargs)


def fetch_issue(_id, **kwargs):
    return _fetch_record(_id, models.Issue, **kwargs)


def create_document():
    try:
        return models.Article()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def create_journal():
    try:
        return models.Journal()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def create_issue():
    try:
        return models.Issue()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def fetch_migrated_document(_id, **kwargs):
    return _fetch_record(_id, MigratedDoc, **kwargs)


def create_migrated_document():
    try:
        return MigratedDoc()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def fetch_migrated_journal(_id, **kwargs):
    return _fetch_record(_id, MigratedJournal, **kwargs)


def create_migrated_journal():
    try:
        return MigratedJournal()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def fetch_migrated_issue(_id, **kwargs):
    return _fetch_record(_id, MigratedIssue, **kwargs)


def create_migrated_issue():
    try:
        return MigratedIssue()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def save_data(data):
    if not hasattr(data, 'created'):
        data.created = None
    try:
        data.created = data.created or datetime.utcnow().isoformat()
        data.updated = datetime.utcnow().isoformat()
        return data.save()
    except Exception as e:
        raise
        # exceptions.DBSaveDataError(e)


def create_remote_and_local_file(local, remote, annotation=None):
    try:
        file = {}
        if local:
            file["name"] = local
        if remote:
            file["uri"] = remote
        if annotation:
            file["annotation"] = annotation
        return models.RemoteAndLocalFile(**file)
    except Exception as e:
        raise exceptions.RemoteAndLocalFileError(
            "Unable to create RemoteAndLocalFile(%s, %s): %s" %
            (local, remote, e)
        )


def register_received_package(uri, name, annotation=None):
    received = v2_models.ReceivedPackage()
    received.file = create_remote_and_local_file(uri, name, annotation)
    return save_data(received)


def fetch_document_package(v3):
    return v2_models.ArticleFiles(_id=v3)


def register_document_package(v3, data):
    """
    data = {}
    data['xml'] = xml_uri_and_name
    data['assets'] = assets
    data['renditions'] = renditions
    data['file'] = file
    """
    article_files = v2_models.ArticleFiles()
    article_files._id = v3
    article_files.xml = create_remote_and_local_file(**data['xml'])
    article_files.file = create_remote_and_local_file(**data['file'])

    assets = []
    for item in data["assets"]:
        assets.append(
            create_remote_and_local_file(**item)
        )
    article_files.assets = assets

    renditions = []
    for item in data["renditions"]:
        renditions.append(
            create_remote_and_local_file(**item)
        )
    article_files.renditions = renditions
    save_data(article_files)
    return article_files
