import os
from urllib.parse import urlparse

from lxml.html import rewrite_links, iterlinks, fromstring, tostring


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


def get_assets_locations(html_text, website_url='www.scielo.br'):
    """
    Find links in html content using iterlinks

    iterlinks(
        '<img src="abc.jpg"/>'
        '<a href="bcd.jpg"/>'
        '<a href="www.scielo.br"/>'
        '<a href="http://www.scielo.br"/>'
    )
    (<Element img at 0x10d313810>, 'src', 'abc.jpg', 0)
    (<Element a at 0x10d99c590>, 'href', 'bcd.jpg', 0)
    (<Element a at 0x10da5c590>, 'href', 'www.scielo.br', 0)
    (<Element a at 0x10d313810>, 'href', 'http://www.scielo.br', 0)

    Returns the ones which were considered document assets
    """
    links = []
    for item in iterlinks(html_text):
        elem, attr, link, pos = item
        path = _get_path(link, website_url)
        if path:
            links.append(
                {"elem": elem, "attr": attr, "link": link, "path": path}
            )
    return links


def _get_path(link, website_url):
    parsed = urlparse(link)
    # ParseResult(
    #       scheme='http', netloc='www.cwi.nl:80',
    #       path='/%7Eguido/Python.html', params='', query='', fragment='')
    if website_url == parsed.netloc:
        return parsed.path
    if website_url == parsed.path:
        return None
    if parsed.path.startswith(website_url):
        return parsed.path[parsed.path.find(website_url)+len(website_url):]
    if parsed.path and not parsed.netloc and not parsed.scheme:
        return parsed.path


def adapt_html_text_to_website(html_text, assets):
    html = fromstring(html_text)

    for asset in assets:
        elem = asset["elem"]
        attr = asset["attr"]
        original = asset["original"]

        for node in html.xpath(f"//{elem}[@{attr}='{original}']"):
            new = asset["new"]
            if "#" in original:
                new += original[original.find("#"):]
            node.set(attr, new)
    return tostring(html).decode("utf-8")

