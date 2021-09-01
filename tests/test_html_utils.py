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

