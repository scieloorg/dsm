from unittest import TestCase

from dsm.extdeps.isis_migration import (
    sps_exporter,
    json2doc,
)
from dsm.extdeps.isis_migration.sps_exporter import (
    SetupArticlePipe,
    XMLArticleAttributesPipe,
    XMLFrontPipe,
    XMLJournalMetaJournalIdPipe,
    XMLJournalMetaJournalTitleGroupPipe,
    XMLJournalMetaISSNPipe,
    XMLJournalMetaPublisherPipe,
    XMLArticleMetaArticleIdPublisherPipe,
    XMLArticleMetaArticleIdDOIPipe,
    XMLArticleMetaArticleCategoriesPipe,
    XMLArticleMetaTitleGroupPipe,
    XMLArticleMetaTranslatedTitleGroupPipe,
    XMLArticleMetaContribGroupPipe,
    # XMLArticleMetaAffiliationPipe,
    # XMLArticleMetaDatesInfoPipe,
    # XMLArticleMetaIssueInfoPipe,
    # XMLArticleMetaElocationInfoPipe,
    # XMLArticleMetaPagesInfoPipe,
    # XMLArticleMetaHistoryPipe,
    # XMLArticleMetaPermissionPipe,
    # XMLArticleMetaSelfUriPipe,
    # XMLArticleMetaAbstractsPipe,
    # XMLArticleMetaKeywordsPipe,
    # XMLArticleMetaCountsPipe,
    # XMLBodyPipe,
    # XMLArticleMetaCitationsPipe,
    # XMLSubArticlePipe,
    XMLClosePipe,
)
from dsm.utils import files
from dsm.utils import xml_utils
import json


def get_document():
    content = files.read_file("./tests/fixtures/json_files/artigo.json")
    return json2doc.Document("id", json.loads(content))


def get_journal():
    journal_json = {
        "id": "issn",
        "records": [{
            "v069": [{"_": "acron"}],
            "v320": [{"_": "SP"}],
            "v490": [{"_": "São Paulo"}],
            "v100": [{"_": "Journal Title"}],
            "v150": [{"_": "Abbrev journal title"}],
            "v435": [
                {"_": "12345-0988", "t": "PRINT"},
                {"_": "12345-9988", "t": "ONLIN"},
            ],
            "v480": [
                {"_": "Sociedade ..."},
                {"_": "Universidade ..."},
            ],
            "v150": [{"_": "Abbrev journal title"}],
        }]
    }
    return json2doc.Journal("id", journal_json["records"][0])


def get_issue():
    issue_json = {
        "id": "issn",
        "records": [{
            "v049": [
                {"c": "AABC050", "t": "Section A", "l": "en"},
                {"c": "AABC050", "t": "Seção A", "l": "pt"},
            ],
        }]
    }
    return json2doc.Issue("id", issue_json["records"][0])


class TestSPSExporter(TestCase):
    def setUp(self):
        print()
        self._document = get_document()

    def get_xml(self, xml_str):
        return xml_utils.etree.fromstring(xml_str)

    # def test_get_xml_rsps(self):
    #     result = self.get_result()
    #     expected = (
    #         """<!DOCTYPE article PUBLIC "-//NLM//DTD J[177 chars]0"/>"""
    #         """<article/>"""
    #     ).encode("utf-8")
    #     self.assertEqual(expected, result)

    def test_SetupArticlePipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = SetupArticlePipe().transform(data)
        self.assertEqual('sps-1.4', _xml.get("specific-use"))
        self.assertEqual('1.0', _xml.get("dtd-version"))

    def test_XMLClosePipe(self):
        data = self._document, self.get_xml("<article/>")
        _xml = XMLClosePipe().transform(data)
        expected = (
            '<!DOCTYPE article PUBLIC "-//NLM//DTD JATS (Z39.96) '
            'Journal Publishing DTD v1.0 20120330//EN" '
            '"JATS-journalpublishing1.dtd">\n'
            '<article/>'
        ).encode("utf-8")
        self.assertEqual(expected, _xml)

    def test_XMLArticleAttributesPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleAttributesPipe().transform(data)
        self.assertEqual(
            self._document.document_type, _xml.get("article-type"))
        self.assertEqual(
            'en', _xml.get("{http://www.w3.org/XML/1998/namespace}lang"))

    def test_XMLFrontPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLFrontPipe(
            ).transform(data)
        expected = (
            "<?xml version='1.0' encoding='utf-8'?>"
            "\n<article>"
            "<front><journal-meta/>"
            "<article-meta/>"
            "</front>"
            "</article>"
        )
        result = xml_utils.tostring(_xml)
        self.assertEqual(expected, result)

    def test_XMLJournalMetaJournalIdPipe(self):
        """
        """
        self._document.journal = get_journal()

        doc = self._document
        xml = self.get_xml(
            "<article><front><journal-meta/></front></article>"
        )
        data = doc, xml
        _raw, _xml = XMLJournalMetaJournalIdPipe(
            ).transform(data)
        expected = (
            b'<journal-id journal-id-type="publisher-id">'
            b'acron</journal-id>'
        )
        result = xml_utils.etree.tostring(_xml.find(".//journal-id"))
        self.assertEqual(expected, result)

    def test_XMLJournalMetaJournalTitleGroupPipe(self):
        """
        """
        self._document.journal = get_journal()

        doc = self._document
        xml = self.get_xml(
            "<article><front><journal-meta/></front></article>"
        )
        data = doc, xml
        _raw, _xml = XMLJournalMetaJournalTitleGroupPipe(
            ).transform(data)
        expected = (
            '<journal-title-group>'
            '<journal-title>Journal Title</journal-title>'
            '<abbrev-journal-title abbrev-type="publisher">'
            'Abbrev journal title'
            '</abbrev-journal-title>'
            '</journal-title-group>'
        ).encode("utf-8")
        result = xml_utils.etree.tostring(_xml.find(".//journal-title-group"))
        self.assertEqual(expected, result)

    def test_XMLJournalMetaISSNPipe(self):
        self._document.journal = get_journal()

        doc = self._document
        xml = self.get_xml(
            "<article><front><journal-meta/></front></article>"
        )
        data = doc, xml
        _raw, _xml = XMLJournalMetaISSNPipe(
            ).transform(data)
        expected = (
            '<journal-meta>'
            '<issn pub-type="ppub">12345-0988</issn>'
            '<issn pub-type="epub">12345-9988</issn>'
            '</journal-meta>'
        ).encode("utf-8")
        result = xml_utils.etree.tostring(_xml.find(".//journal-meta"))
        self.assertEqual(expected, result)

    def test_XMLJournalMetaPublisherPipe(self):
        self._document.journal = get_journal()

        doc = self._document
        xml = self.get_xml(
            "<article><front><journal-meta/></front></article>"
        )
        data = doc, xml
        _raw, _xml = XMLJournalMetaPublisherPipe(
            ).transform(data)
        expected = (
            '<publisher>'
            '<publisher-name>Sociedade ...; Universidade ...'
            '</publisher-name>'
            '<publisher-loc>São Paulo, SP'
            '</publisher-loc>'
            '</publisher>'
        )
        result = xml_utils.node_text(_xml.find(".//journal-meta"))
        print(result)
        print(expected)
        self.assertEqual(expected, result)

    def test_XMLArticleMetaArticleIdPublisherPipe(self):
        doc = self._document
        xml = self.get_xml(
            "<article><front><article-meta/></front></article>"
        )
        data = doc, xml
        _raw, _xml = XMLArticleMetaArticleIdPublisherPipe(
            ).transform(data)
        expected = (
            '<article-id pub-id-type="other">'
            '00515'
            '</article-id>'
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v1">'
            'S0001-3765(19)09100000515'
            '</article-id>'
            '<article-id pub-id-type="publisher-id" specific-use="scielo-v2">'
            'S0001-37652019000400515'
            '</article-id>'
        )
        result = xml_utils.node_text(_xml.find(".//article-meta"))
        self.assertEqual(expected, result)

    def test_XMLArticleMetaArticleIdDOIPipe(self):
        doc = self._document
        xml = self.get_xml(
            "<article><front><article-meta/></front></article>"
        )
        data = doc, xml
        _raw, _xml = XMLArticleMetaArticleIdDOIPipe(
            ).transform(data)
        expected = (
            '<article-id pub-id-type="doi">10.1590/0001-3765201920180750</article-id>'
        )
        result = xml_utils.node_text(_xml.find(".//article-meta"))
        self.assertEqual(expected, result)

    def test_XMLArticleMetaArticleCategoriesPipe(self):
        doc = self._document
        doc.issue = get_issue()
        xml = self.get_xml(
            '<article><front><article-meta/></front></article>'
        )
        data = doc, xml
        _raw, _xml = XMLArticleMetaArticleCategoriesPipe(
            ).transform(data)
        expected = (
            '<article-meta>'
            '<article-categories>'
            '<subj-group subj-group-type="heading"><subject>'
            'Section A</subject></subj-group>'
            '</article-categories>'
            '</article-meta>'
        ).encode("utf-8")
        result = xml_utils.etree.tostring(_xml.find(".//article-meta"))
        self.assertEqual(expected, result)

    def test_XMLArticleMetaTitleGroupPipe(self):
        doc = self._document
        xml = self.get_xml(
            "<article><front><article-meta/></front></article>"
        )
        data = doc, xml
        _raw, _xml = XMLArticleMetaTitleGroupPipe().transform(data)
        expected = (
            '<title-group>'
            '<article-title>'
            'Morphology of the megaspore &lt;em&gt;Lagenoisporites magnus'
            '&lt;/em&gt; (Chi and Hills 1976) Candilier et al. (1982), '
            'from the Carboniferous (lower Mississippian: mid-upper '
            'Tournaisian) of Bolivia'
            '</article-title>'
            '</title-group>'
        )
        result = xml_utils.node_text(_xml.find(".//article-meta"))
        print(result)
        print(expected)
        self.assertEqual(expected, result)

    def test_XMLArticleMetaTranslatedTitleGroupPipe(self):
        doc = self._document
        xml = self.get_xml(
            '<article><front><article-meta>'
            '<title-group/>'
            '</article-meta></front></article>'
        )
        data = doc, xml
        _raw, _xml = XMLArticleMetaTranslatedTitleGroupPipe().transform(data)
        expected = (
            '<title-group>'
            '<trans-title-group xml:lang="es">'
            '<trans-title>'
            'Título en español'
            '</trans-title>'
            '</trans-title-group>'
            '</title-group>'
        )
        result = xml_utils.node_text(_xml.find(".//article-meta"))
        print(result)
        print(expected)
        self.assertEqual(expected, result)

    def test_XMLArticleMetaContribGroupPipe(self):
        doc = self._document
        xml = self.get_xml(
            '<article><front><article-meta>'
            '</article-meta></front></article>'
        )
        data = doc, xml
        _raw, _xml = XMLArticleMetaContribGroupPipe().transform(data)
        expected = (
            '<contrib-group>'
            '<contrib contrib-type="author">'
            '<name>'
            '<surname>QUETGLAS</surname>'
            '<given-names>MARCELA</given-names>'
            '</name>'
            '<xref ref-type="aff" rid="aff1"/>'
            '</contrib>'
            '<contrib contrib-type="author">'
            '<name>'
            '<surname>MACLUF</surname>'
            '<given-names>CECILIA</given-names>'
            '</name>'
            '<xref ref-type="aff" rid="aff1"/>'
            '</contrib>'
            '<contrib contrib-type="author">'
            '<name>'
            '<surname>PASQUO</surname>'
            '<given-names>MERCEDES DI</given-names>'
            '</name>'
            '<xref ref-type="aff" rid="aff2"/></contrib>'
            '</contrib-group>'
        )
        result = xml_utils.node_text(_xml.find(".//article-meta"))
        print(result)
        print(expected)
        self.assertEqual(expected, result)

    def test_XMLArticleMetaAffiliationPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaAffiliationPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaDatesInfoPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaDatesInfoPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaIssueInfoPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaIssueInfoPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaElocationInfoPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaElocationInfoPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaPagesInfoPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaPagesInfoPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaHistoryPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaHistoryPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaPermissionPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaPermissionPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaSelfUriPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaSelfUriPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaAbstractsPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaAbstractsPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaKeywordsPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaKeywordsPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaCountsPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaCountsPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLBodyPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLBodyPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLArticleMetaCitationsPipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLArticleMetaCitationsPipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)

    def test_XMLSubArticlePipe(self):
        data = self._document, self.get_xml("<article/>")
        _raw, _xml = XMLSubArticlePipe(
            ).transform(data)
        expected = ""
        result = ""
        self.assertEqual(expected, result)
