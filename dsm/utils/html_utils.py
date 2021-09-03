import os
from urllib.parse import urlparse

from lxml.html import rewrite_links


class URI_Changer:

    def __init__(self, uris_and_names):
        self._image_files = {
            item["name"]: item["uri"]
            for item in uris_and_names
        }

    def replace_link(self, link):
        parsed = urlparse(link)
        basename = os.path.basename(parsed.path)
        uri = self._image_files.get(basename)
        if uri:
            return uri
        else:
            return link


def change_images_location(html_text, asset_uris_and_names):
    # https://lxml.de/lxmlhtml.html
    changer = URI_Changer(asset_uris_and_names)
    return rewrite_links(html_text, changer.replace_link)

