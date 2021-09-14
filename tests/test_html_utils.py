from unittest import TestCase

from dsm.utils import html_utils


class TestHTMLUtils(TestCase):

    def test_replace_link_abc_jpg(self):
        html_text = ("""
            <img src="abc.jpg">
        """)
        expected = ("""<img src="https://minio.scielo.br/abc.jpg">
        """)
        asset_uris_and_names = [
            {"name": "abc.jpg", "uri": "https://minio.scielo.br/abc.jpg"}
        ]
        result = html_utils.change_images_location(
            html_text, asset_uris_and_names)
        self.assertEqual(expected, result)

    def test_replace_link_img_revistas_abcd_v1n1_abc_jpg(self):
        html_text = ("""
            <img src="/img/revistas/abcd/v1n1/abc.jpg">
        """)
        expected = ("""<img src="https://minio.scielo.br/abc.jpg">
        """)
        asset_uris_and_names = [
            {"name": "abc.jpg", "uri": "https://minio.scielo.br/abc.jpg"}
        ]
        result = html_utils.change_images_location(
            html_text, asset_uris_and_names)
        self.assertEqual(expected, result)

    def test_replace_link_img_revistas_abcd_v1n1_abc_png(self):
        html_text = ("""
            <img src="http://www.scielo.br/img/revistas/abcd/v1n1/abc.png">
        """)
        expected = ("""<img src="https://minio.scielo.br/abc.png">
        """)
        asset_uris_and_names = [
            {"name": "abc.png", "uri": "https://minio.scielo.br/abc.png"}
        ]
        result = html_utils.change_images_location(
            html_text, asset_uris_and_names)
        self.assertEqual(expected, result)

    def test_replace_some_links(self):
        html_text = (
            '    <html><body>'
            '    <img src="/img/revistas/abcd/v1n1/abc_f01.png">'
            '    <img src="/img/revistas/abcd/v1n1/abc-987.jpg">'
            '    <img src="/img/revistas/abcd/v1n1/abc-987.tiff">'
            '    <img src="/img/revistas/abcd/v1n1/abc.jpg">'
            '    <img src="/img/revistas/abcd/v1n1/abc.png">'
            '    </body></html>'
        )
        expected = (
            '    <html><body>'
            '    <img src="https://minio.scielo.br/abc_f01.png">'
            '    <img src="https://minio.scielo.br/abc-987.jpg">'
            '    <img src="https://minio.scielo.br/abc-987.tiff">'
            '    <img src="/img/revistas/abcd/v1n1/abc.jpg">'
            '    <img src="/img/revistas/abcd/v1n1/abc.png">'
            '    </body></html>'
        )
        asset_uris_and_names = [
            {
                "name": "abc_f01.png",
                "uri": "https://minio.scielo.br/abc_f01.png"
            },
            {
                "name": "abc_f01.jpg",
                "uri": "https://minio.scielo.br/abc_f01.jpg"
            },
            {
                "name": "abc_f01.tiff",
                "uri": "https://minio.scielo.br/abc_f01.tiff"
            },
            {
                "name": "abc-987.png",
                "uri": "https://minio.scielo.br/abc-987.png"
            },
            {
                "name": "abc-987.jpg",
                "uri": "https://minio.scielo.br/abc-987.jpg"
            },
            {
                "name": "abc-987.tiff",
                "uri": "https://minio.scielo.br/abc-987.tiff"
            },
        ]
        result = html_utils.change_images_location(
            html_text, asset_uris_and_names)
        self.assertEqual(expected.strip(), result.strip())


class TestGetPath(TestCase):

    def setUp(self):
        self.url = "www.scielo.br"

    def test__get_path_returns_none_if_link_is_website_url(self):
        result = html_utils._get_path("www.scielo.br", self.url)
        self.assertIsNone(result)

    def test__get_path_returns_none_if_link_is_external(self):
        result = html_utils._get_path("http://www.othersite.br/home", self.url)
        self.assertIsNone(result)

    def test__get_path_returns_none_if_link_is_not_http(self):
        result = html_utils._get_path("mailto:a@scielo.org", self.url)
        self.assertIsNone(result)

    def test__get_path_returns_home_if_link_startswith_website_url(self):
        expected = "/home"
        result = html_utils._get_path("www.scielo.br/home", self.url)
        self.assertEqual(expected, result)

    def test__get_path_returns_path_if_link_is_correct(self):
        expected = "/img/revistas/acron/volun/a1.gif"
        result = html_utils._get_path(
            "/img/revistas/acron/volun/a1.gif", self.url)
        self.assertEqual(expected, result)

    def test__get_path_returns_path_if_link_has_no_slash_at_the_begin(self):
        expected = "top.gif"
        result = html_utils._get_path("top.gif", self.url)
        self.assertEqual(expected, result)

