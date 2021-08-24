import os
import shutil
import logging
import tempfile
from datetime import datetime
from zipfile import ZipFile

from .requests import requests_get


logger = logging.getLogger(__name__)


def is_folder(source):
    return os.path.isdir(source)


def is_zipfile(source):
    return os.path.isfile(source) and source.endswith(".zip")


def xml_files_list(path):
    """
    Return the XML files found in `path`
    """
    return (f for f in os.listdir(path) if f.endswith(".xml"))


def files_list(path):
    """
    Return the files in `path`
    """
    return os.listdir(path)


def read_file(path, encoding="utf-8"):
    with open(path, "r", encoding=encoding) as f:
        text = f.read()
    return text


def xml_files_list_from_zipfile(zip_path):
    with ZipFile(zip_path) as zf:
        xmls_filenames = [
            xml_filename
            for xml_filename in zf.namelist()
            if os.path.splitext(xml_filename)[-1] == ".xml"
        ]
    return xmls_filenames


def files_list_from_zipfile(zip_path):
    """
    Return the files in `zip_path`

    Example:

    ```
    [
        '2318-0889-tinf-33-0421/2318-0889-tinf-33-e200069.pdf',
        '2318-0889-tinf-33-0421/2318-0889-tinf-33-e200069.xml',
        '2318-0889-tinf-33-0421/2318-0889-tinf-33-e200071.pdf',
        '2318-0889-tinf-33-0421/2318-0889-tinf-33-e200071.xml',
        '2318-0889-tinf-33-0421/2318-0889-tinf-33-e200071-gf01.tif',
        '2318-0889-tinf-33-0421/2318-0889-tinf-33-e200071-gf02.tif',
        '2318-0889-tinf-33-0421/2318-0889-tinf-33-e200071-gf03.tif',
        '2318-0889-tinf-33-0421/2318-0889-tinf-33-e200071-gf04.tif',
    ]
    ```
    """
    with ZipFile(zip_path) as zf:
        return zf.namelist()


def write_file(path, source, mode="w"):
    dirname = os.path.dirname(path)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)
    logger.debug("Gravando arquivo: %s", path)
    if "b" in mode:
        with open(path, mode) as f:
            f.write(source)
        return

    with open(path, mode, encoding="utf-8") as f:
        f.write(source)


def download_files_and_create_zip_file(
        zip_path, uri_and_file_items, timeout=10):
    results = []
    zip_folder = os.path.dirname(zip_path)
    with ZipFile(zip_path, 'w') as myzip:
        for uri_and_name in uri_and_file_items:
            # get uri content
            local_path = os.path.join(zip_folder, uri_and_name["name"])
            try:
                result = uri_and_name
                content = requests_get(uri_and_name["uri"], timeout=timeout)
                write_file(local_path, content, "wb")
                # add file to zip
                myzip.write(local_path, uri_and_name["name"])
            except Exception as e:
                result.update({"error": str(e)})
            results.append(result)
    return results


def create_zip_file(files, zip_name, zip_folder=None):
    zip_folder = zip_folder or tempfile.mkdtemp()

    zip_path = os.path.join(zip_folder, zip_name)
    with ZipFile(zip_path, 'w') as myzip:
        for f in files:
            myzip.write(f, os.path.basename(f))
    return zip_path


def delete_folder(path):
    try:
        shutil.rmtree(path)
    except:
        pass


def date_now_as_folder_name():
    # >>> datetime.now().isoformat()
    # >>> '2021-08-11T17:54:50.556715'
    return datetime.now().isoformat().replace(":", "")
