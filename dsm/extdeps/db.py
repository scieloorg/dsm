from datetime import datetime
from mongoengine import connect, Q
from opac_schema.v1 import models
from opac_schema.v2 import models as v2_models
from dsm import exceptions
from dsm.extdeps.isis_migration.migration_models import (
    ISISDocument,
    ISISJournal,
    ISISIssue,
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


def _fetch_records(model, **kwargs):
    try:
        objs = model.objects(**kwargs)
    except IndexError:
        return None
    except Exception as e:
        raise exceptions.DBFetchMigratedDocError(e)
    else:
        return objs


def fetch_articles_files(**kwargs):
    return v2_models.ArticleFiles.objects(**kwargs)


def fetch_journals(**kwargs):
    return _fetch_records(models.Journal, **kwargs)


def fetch_issues(**kwargs):
    return _fetch_records(models.Issue, **kwargs)


def fetch_documents(**kwargs):
    return _fetch_records(models.Article, **kwargs)


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


def fetch_isis_document(_id, **kwargs):
    return _fetch_record(_id, ISISDocument, **kwargs)


def create_isis_document():
    try:
        return ISISDocument()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def fetch_isis_journal(_id, **kwargs):
    return _fetch_record(_id, ISISJournal, **kwargs)


def create_isis_journal():
    try:
        return ISISJournal()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def fetch_isis_issue(_id, **kwargs):
    return _fetch_record(_id, ISISIssue, **kwargs)


def create_isis_issue():
    try:
        return ISISIssue()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def save_data(data):
    if not hasattr(data, 'created'):
        data.created = None
    try:
        data.updated = datetime.utcnow()
        if not data.created:
            data.created = data.updated
        data.save()
        return data
    except Exception as e:
        raise
        # exceptions.DBSaveDataError(e)


def create_remote_and_local_file(remote, local, annotation=None):
    try:
        file = {}
        if local:
            file["name"] = local
        if remote:
            file["uri"] = remote
        if annotation:
            file["annotation"] = annotation
        return v2_models.RemoteAndLocalFile(**file)
    except Exception as e:
        raise exceptions.RemoteAndLocalFileError(
            "Unable to create RemoteAndLocalFile(%s, %s): %s" %
            (remote, local, e)
        )


def register_received_package(_id, uri, name, annotation=None):
    received = v2_models.ReceivedPackage()
    received._id = _id
    received.file = create_remote_and_local_file(uri, name, annotation)
    return save_data(received)


def fetch_document_packages(v3):
    return v2_models.ArticleFiles.objects(aid=v3).order_by('-updated')


def fetch_document_package_by_pid_and_version(pid, version):
    return v2_models.ArticleFiles.objects.get(
        aid = pid,
        version = version
    )


def register_document_package(v3, data):
    """
    data = {}
    data['xml'] = xml_uri_and_name
    data['assets'] = assets
    data['renditions'] = renditions
    data['file'] = file
    """
    article_files = v2_models.ArticleFiles()
    article_files.aid = v3
    article_files.version = _get_article_files_new_version(v3)
    article_files.scielo_pids = {'v3': v3}

    _set_document_package_file_paths(article_files, data)
    save_data(article_files)

    return article_files


def _get_article_files_new_version(v3):
    current_version = fetch_document_packages(v3).count() or 0
    return current_version + 1


def _set_document_package_file_paths(article_files, data):
    article_files.xml = create_remote_and_local_file(
        data['xml']['uri'],
        data['xml']['name']
    )

    article_files.file = create_remote_and_local_file(
        data['file']['uri'],
        data['file']['name']
    )

    assets = []
    for item in data["assets"]:
        assets.append(
            create_remote_and_local_file(
                item['uri'],
                item['name']
            )
        )
    article_files.assets = assets

    renditions = []
    for item in data["renditions"]:
        renditions.append(
            create_remote_and_local_file(
                item['uri'],
                item['name']
            )
        )
    article_files.renditions = renditions


def get_isis_documents_by_date_range(
        isis_updated_from=None, isis_updated_to=None):
    if isis_updated_from and isis_updated_to:
        return ISISDocument.objects(
            Q(isis_updated_date__gte=isis_updated_from) &
            Q(isis_updated_date__lte=isis_updated_to)
        )
    if isis_updated_from:
        return ISISDocument.objects(
            Q(isis_updated_date__gte=isis_updated_from)
        )
    if isis_updated_to:
        return ISISDocument.objects(
            Q(isis_updated_date__lte=isis_updated_to)
        )


def get_isis_documents_by_publication_year(year):
    return ISISDocument.objects(pub_year=year)


def get_isis_documents(acron=None, issue_folder=None, pub_year=None):
    params = {}
    if acron:
        params['acron'] = acron
    if pub_year:
        params['pub_year'] = pub_year
    if issue_folder:
        params['issue_folder'] = issue_folder

    if params:
        return ISISDocument.objects(**params)
    return []
