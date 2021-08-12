from datetime import datetime
from mongoengine import connect
from opac_schema.v1 import models
from opac_schema.v2 import models as v2_models
from dsm import exceptions


def mk_connection(host):
    try:
        connect(host=host)
    except Exception as e:
        raise exceptions.DBConnectError(e)


def fetch_document(any_doc_id, is_public=True, **kwargs):
    try:
        articles = models.Article.objects(
            pk=any_doc_id, is_public=is_public, **kwargs)
        if not articles:
            articles = models.Article.objects(
                scielo_pids__other=any_doc_id, is_public=is_public, **kwargs)
        if not articles:
            articles = models.Article.objects(
                doi=any_doc_id, is_public=is_public, **kwargs)
        return articles and articles[0]
    except Exception as e:
        raise exceptions.DBFetchDocumentError(e)


def create_document():
    try:
        return models.Article()
    except Exception as e:
        raise exceptions.DBCreateDocumentError(e)


def save_data(data):
    try:
        data.created = data.created or datetime.utcnow().isoformat()
        data.updated = datetime.utcnow().isoformat()
        r = data.save()
        return r or data
    except Exception as e:
        raise exceptions.DBSaveDataError(e)


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
