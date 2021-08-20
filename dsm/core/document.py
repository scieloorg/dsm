
# Module does the same as ArticleFactory from opac-airflow

import logging

from opac_schema.v1 import models

from dsm.utils import packages

from dsm.core.sps_package import SPS_Package
from dsm.core import document_files as docfiles

from dsm.extdeps.doc_ids_manager import add_pids_to_xml
from dsm.extdeps import db
from dsm import exceptions


class DocsManager:

    def __init__(self, files_storage, db_url, v3_manager, config=None):
        """
        Instancia objeto da classe DocsManager

        Parameters
        ----------
        files_storage : dsm.storage.minio.MinioStorage
            files storage object
        db_url : str
            Data to connect to a mongodb. Expected pattern:
                "mongodb://my_user:my_password@127.0.0.1:27017/my_db"
        v3_manager : scielo_v3_manager.pid_manager.Manager
            object to register PID v3
        """
        self._files_storage = files_storage
        self._db_url = db_url
        self._v3_manager = v3_manager
        self._config = config

    def db_connect(self):
        db.mk_connection(self._db_url)


    def get_zip_document_package(self, v3):
        """
        Get uri of zip document package or
        Build the zip document package and return uri

        Parameters
        ----------
        v3 : str
            PID v3

        Returns
        -------
        str
            URI of the zip file

        Raises
        ------
            dsm.exceptions.DocumentDoesNotExistError
            dsm.exceptions.FetchDocumentError
            dsm.exceptions.DBConnectError
        """
        document_package = db.fetch_document_package(v3)

        if not document_package:
            doc = db.fetch_document(v3)
            if not doc:
                raise exceptions.DocumentDoesNotExistError(
                    f"Document {v3} does not exist"
                )
            data = docfiles.build_zip_package(self._files_storage, doc)
            document_package = db.register_document_package(v3, data)

        if document_package:
            if document_package.file:
                return {
                    "uri": document_package.file.uri,
                    "name": document_package.file.name,
                }


    def get_doc_packages(self, source):
        """
        Get documents' package

        Parameters
        ----------
        source: str
            zip file

        Returns
        -------
        dict
        """
        # obtém do pacote os arquivos de cada documento
        return packages.explore_source(source)


    def receive_package(self, source):
        """
        Receive package

        Parameters
        ----------
        source : str
            zip file

        Returns
        -------
        dict
        """
        # armazena o zip
        docfiles.register_received_package(self._files_storage, source)


    def _fetch_document(self, xml_sps, pid_v2):
        _ids = (
            xml_sps.doi,
            xml_sps.aop_pid,
            xml_sps.scielo_pid_v3,
            xml_sps.scielo_pid_v2,
            pid_v2,
        )
        for _id in _ids:
            if not _id:
                continue
            doc = db.fetch_document(_id)
            if doc:
                return doc
        

    def register_document(self, doc_pkg, pid_v2, old_name, issue_id, is_new_document=False):
        """
        Register one package of XML documents + PDFs + images.

        Registra um pacote de XML + PDFs + imagens.

        Parameters
        ----------
        doc_pkg : dsm.utils.package.Package
            dados do pacote XML + PDFs + imagens
        pid_v2 : str
            pid v2, required if absent in XML
        old_name : str
            nome do arquivo no site clássico, se era html ou se era migrado
        issue_id : str
            issue id in the website database
            (it is required for new documents)
        is_new_document: boolean
            é documento novo?
        Returns
        -------
        str
            document id

        Raises
        ------
            exceptions.MissingPidV3Error
            dsm.storage.db.DBSaveDataError
            dsm.storage.db.DBCreateDocumentError
            dsm.data.document_files.FilesStorageRegisterError
        """
        # obtém o XML
        xml_sps = SPS_Package(doc_pkg.xml_content)

        if is_new_document:
            document = db.create_document()
            add_pids_to_xml(xml_sps, document, doc_pkg.xml, pid_v2, self._v3_manager)
        else:
            document = self._fetch_document(xml_sps, pid_v2)

        if not xml_sps.scielo_pid_v3:
            raise exceptions.MissingPidV3Error(
                f"{doc_pkg.xml} missing v3"
            )

        # registra os arquivos do documento (XML, PDFs, imagens) no MINIO
        registered_xml, registered_renditions, assets_registration = docfiles.register_document_files(self._files_storage, doc_pkg, xml_sps, old_name)

        # atualiza os dados de document com os dados do XML e demais dados
        update_document_data(
            document, xml_sps, registered_xml, registered_renditions,
            issue_id,
            document_order=None,
            other_pids=None,
            is_public=True,
        )

        # salva o documento
        db.save_data(document)

        return document._id


def update_document_data(
        document, xml_sps, registered_xml, registered_renditions,
        issue_id,
        document_order=None,
        other_pids=None,
        is_public=True,
        ):
    """
    Update the `document` attributes with `xml_sps` attributes,
    with `registered_xml` and with `registered_renditions`
    Also, update `issue_id`, `document_order`, `other_pids`, and `is_public`.

    Parameters
    ----------
    document : opac_schema.v1.models.Article
        Article model
    xml_sps : dsm.data.sps_package.SPS_Package
        object to handle XML
    registered_xml : str
        XML URI at Minio
    registered_renditions : list
        list of dict (renditions data)
    issue_id : str
        ID of the issue
    document_order : str
        order of the document in the table of contents
    other_pids : list
        other pids of the document
    is_public : bool
        is available at the website
    """
    set_issue_data(document, issue_id)
    set_order(document, xml_sps, document_order)
    set_renditions(document, registered_renditions)
    set_xml_url(document, registered_xml)
    set_ids(document, xml_sps)
    set_is_public(document, is_public=is_public)
    set_languages(document, xml_sps)
    set_article_abstracts(document, xml_sps)
    set_article_authors(document, xml_sps)
    set_article_pages(document, xml_sps)
    set_article_publication_date(document, xml_sps)
    set_article_sections(document, xml_sps)
    set_article_titles(document, xml_sps)
    set_article_type(document, xml_sps)
    add_other_pids(document, other_pids)


def add_other_pids(article, other_pids):
    if other_pids:
        article.scielo_pids.update(
            {
                "other":
                list(set(list(article.scielo_pids.get("other") or []) +
                     list(other_pids)))
            }
        )


def set_order(article, xml_sps, document_order):
    article.order = _get_order(xml_sps, document_order)


def set_renditions(article, renditions):
    article.pdfs = renditions


def set_xml_url(article, xml_url):
    article.xml = xml_url


def set_issue_data(article, issue_id):
    if issue_id is None:
        issue_id = article.issue._id

    if article.issue is not None and article.issue.number == "ahead":
        if article.aop_url_segs is None:
            url_segs = {
                "url_seg_article": article.url_segment,
                "url_seg_issue": article.issue.url_segment,
            }
            article.aop_url_segs = models.AOPUrlSegments(**url_segs)

    # Issue vinculada
    issue = models.Issue.objects.get(_id=issue_id)

    logging.info("ISSUE %s" % str(issue))
    logging.info("ARTICLE.ISSUE %s" % str(article.issue))
    logging.info("ARTICLE.AOP_PID %s" % str(article.aop_pid))
    logging.info("ARTICLE.PID %s" % str(article.pid))

    article.issue = issue
    article.journal = issue.journal


def set_is_public(article, is_public=True):
    article.is_public = is_public


def set_ids(article, xml_sps):
    article._id = xml_sps.scielo_pid_v3
    article.aid = xml_sps.scielo_pid_v3
    article.scielo_pids = xml_sps.article_ids
    article.aop_pid = xml_sps.aop_pid
    article.pid = xml_sps.scielo_pid_v2
    article.doi = xml_sps.doi


def set_article_type(article, xml_sps):
    article.type = xml_sps.article_type


def set_languages(article, xml_sps):
    article.original_language = xml_sps.lang
    article.languages = xml_sps.languages
    article.htmls = [{"lang": lang} for lang in xml_sps.languages]


def set_article_titles(article, xml_sps):
    article.title = xml_sps.article_title
    langs_and_titles = {
        lang: title
        for lang, title in xml_sps.article_titles.items()
        if lang != xml_sps.lang
    }
    set_translate_titles(article, langs_and_titles)


def set_article_sections(article, xml_sps):
    article.section = xml_sps.subject
    set_translated_sections(article, xml_sps.subjects)


def set_article_abstracts(article, xml_sps):
    article.abstract = xml_sps.abstract
    set_abstracts(article, xml_sps.abstracts)
    set_keywords(article, xml_sps.keywords_groups)


def set_article_publication_date(article, xml_sps):
    article.publication_date = xml_sps.document_pubdate


def set_article_pages(article, xml_sps):
    article.elocation = xml_sps.elocation_id
    article.fpage = xml_sps.fpage
    article.fpage_sequence = xml_sps.fpage_seq
    article.lpage = xml_sps.lpage


def set_article_authors(article, xml_sps):
    set_authors(article, xml_sps.authors)
    set_authors_meta(article, xml_sps.authors)


def set_authors(article, authors):
    """
    Update `opac_schema.v1.models.Article.authors`
    with `authors`

    Parameters
    ----------
    article: opac_schema.v1.models.Article
    authors: dict which keys are ("surname", "given_names")
    """
    article.authors = [
        f'{a.get("surname")}, {a.get("given_names")}'
        for a in authors
    ]


def set_authors_meta(article, authors):
    """
    Update `opac_schema.v1.models.Article.authors_meta`
    with `authors`

    Parameters
    ----------
    article: opac_schema.v1.models.Article
    authors: dict which keys are (
        "surname", "given_names", "orcid", "affiliation", "suffix",
    )
    """
    _authors = []
    for a in authors:
        if not a.get("orcid") and not a.get("affiliation"):
            continue
        author = {}
        if a.get("orcid"):
            author["orcid"] = a.get("orcid")
        if a.get("aff"):
            author["affiliation"] = a.get("aff")
        if a.get("prefix"):
            author["name"] = (
                f'{a.get("surname")} {a.get("prefix")}, {a.get("given_names")}'
            )
        else:
            author["name"] = (
                f'{a.get("surname")}, {a.get("given_names")}'
            )
        _authors.append(models.AuthorMeta(**author))
    article.authors_meta = _authors


def set_translate_titles(article, langs_and_titles):
    """
    Update `opac_schema.v1.models.Article.translated_titles`
    with `langs_and_titles`

    Parameters
    ----------
    article: opac_schema.v1.models.Article
    langs_and_titles: dict
    """
    translated_titles = []
    for lang, title in langs_and_titles.items():
        translated_titles.append(
            models.TranslatedTitle(
                **{
                    "name": title,
                    "language": lang,
                }
            )
        )
    article.translated_titles = translated_titles


def set_translated_sections(article, langs_and_sections):
    """
    Update `opac_schema.v1.models.Article.trans_sections`
    with `langs_and_sections`

    Parameters
    ----------
    article: opac_schema.v1.models.Article
    langs_and_sections: dict
    """
    sections = []
    for lang, section in langs_and_sections.items():
        sections.append(
            models.TranslatedSection(
                **{
                    "name": section,
                    "language": lang,
                }
            )
        )
    article.trans_sections = sections


def set_abstracts(article, langs_and_abstracts):
    """
    Update `opac_schema.v1.models.Article.abstracts` with `langs_and_abstracts`

    Parameters
    ----------
    article: opac_schema.v1.models.Article
    langs_and_abstracts: dict
    """
    abstracts = []
    for lang, abstr in langs_and_abstracts.items():
        abstracts.append(
            models.Abstract(
                **{
                    "text": abstr,
                    "language": lang,
                }
            )
        )
    article.abstracts = abstracts
    article.abstract_languages = list(langs_and_abstracts.keys())


def set_keywords(article, langs_and_keywords):
    """
    Update `opac_schema.v1.models.Article.keywords` with `langs_and_keywords`

    Parameters
    ----------
    article: opac_schema.v1.models.Article
    langs_and_keywords: dict
    """
    keywords = []
    for lang, kwds in langs_and_keywords.items():
        keywords.append(
            models.ArticleKeyword(
                **{
                    "keywords": kwds,
                    "language": lang,
                }
            )
        )
    article.keywords = keywords


def _get_order(xml_sps, document_order):
    try:
        return int(document_order)
    except (ValueError, TypeError):
        order_err_msg = (
            "'{}' is not a valid value for "
            "'article.order'".format(document_order)
        )
        logging.info(
            "{}. It was set '{} (the last 5 digits of PID v2)' to "
            "'article.order'".format(order_err_msg, xml_sps.order))
        return xml_sps.order


# def is_document_pid_v2(document, pid):
#     if pid == document.pid:
#         return True
#     if pid == document.aop_pid:
#         return True
#     if document.scielo_pids:
#         pids = document.scielo_pids.get("other") or []
#         if pid in pids:
#             return True
#         pids = [v for k, v in document.scielo_pids.items() if k != 'other']
#         if pid in pids:
#             return True
#     return False


# def is_document_pid_v3(document, pid):
#     if pid == document._id:
#         return True
#     if pid == document.aid:
#         return True
#     if document.scielo_pids.get("other"):
#         if pid in document.scielo_pids.get("other"):
#             return True
#     return False

