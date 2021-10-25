import os
import tempfile
from mimetypes import MimeTypes
from zipfile import ZipFile

from dsm.utils import (
    files,
    reqs,
    async_download,
)
from dsm.core.sps_package import (
    SPS_Package,
)
from dsm.extdeps import db
from dsm import exceptions
from dsm import configuration


def build_zip_package(files_storage, record):
    # get XML file
    xml_sps = _get_xml_to_zip(record)

    # get uri and filename of assets and renditions
    assets = _get_assets_to_zip(xml_sps)

    renditions = _get_renditions_to_zip(record)

    files_storage_folder = configuration.get_files_storage_folder_for_zipped_packages(
        issn=xml_sps.issn,
        scielo_pid_v3=xml_sps.scielo_pid_v3,
    )

    xml_uri_and_name = register_xml(
        files_storage,
        files_storage_folder,
        xml_sps)

    uris_and_names = (
        [xml_uri_and_name] + assets + renditions
    )

    # create zip file
    zip_file_path = _zip_files(f"{xml_sps.package_name}.zip", uris_and_names)

    # publish zip file in the files storage
    uri_and_name = files_storage_register(
        files_storage,
        files_storage_folder,
        zip_file_path,
        os.path.basename(zip_file_path))

    # delete temp_dir
    files.delete_folder(os.path.dirname(zip_file_path))

    data = {}
    data['xml'] = xml_uri_and_name

    # FIXME - alguns valores que deveriam ser uri são nome de arquivos
    # isso dá erro ao salvar o registro
    assets = [a for a in assets if a["uri"].startswith("http")]
    data['assets'] = assets

    data['renditions'] = renditions
    data['file'] = uri_and_name

    return data


def _get_xml_to_zip(document):
    """
    Get document files, build zip file and publish it at files storage

    Parameters
    ----------
    document : opac_schema.v1.models.Article

    Returns
    -------
    dsm.data.sps_package.SPS_Package
    """
    # get XML file
    content = reqs.requests_get_content(document.xml)

    xml_sps = SPS_Package(content)

    # change assets uri
    xml_sps.remote_to_local(xml_sps.package_name)
    return xml_sps


def _get_assets_to_zip(xml_sps):
    """
    Get assets uris and filenames to create zip file

    Parameters
    ----------
    xml_sps : dsm.data.sps_package.SPS_Package

    Returns
    -------
    list
        list of uris and files
    """
    # get uri and filename of assets and renditions
    uris_and_filenames = []
    for asset in xml_sps.assets.items:
        uris_and_filenames.append({
            "uri": asset.uri,
            "name": asset.filename
        })
    return uris_and_filenames


def _get_renditions_to_zip(document):
    """
    Get renditions uris and filenames to create zip file

    Parameters
    ----------
    document : opac_schema.v1.models.Article

    Returns
    -------
    list
        list of uris and files
    """
    uris_and_filenames = []
    for rendition in document.pdfs:
        uris_and_filenames.append({
            "uri": rendition['url'],
            "name": rendition["filename"],
        })
    return uris_and_filenames


def _zip_files(zip_name, uris_and_names):
    """
    Create zip file

    Parameters
    ----------
    xml_sps : dsm.data.sps_package.SPS_Package
    uri_and_file_items : list of tuples

    Returns
    -------
    zip file path
    """

    # create zip file
    uris_and_names = [
        item
        for item in uris_and_names
        if item["uri"].startswith("http")
    ]
    downloaded_files = async_download.download_files(uris_and_names)
    return files.create_zip_file(downloaded_files, zip_name)


def register_document_files(files_storage, doc_package, xml_sps,
                            classic_website_filename=None):
    """
    Registra os arquivos dos documentos.

    Parameters
    ----------
    files_storage : dsm.storage.minio.MinioStorage
        serviço de armazenagem dos arquivos
    doc_package : dsm.utils.package.Package
        dados dos arquivos do pacote: XML, PDFs, imagens
    xml_sps : dsm.data.sps_package.SPS_Package
        object to handle XML

    Returns
    -------
    tuple (registered_xml, registered_renditions, assets_registration_result)
        registered_xml
            URI do documento XML
        registered_renditions
            dados das manifestações
        assets_registration_result
            resultado do registro dos ativos digitais
    """
    files_storage_folder = configuration.get_files_storage_folder_for_ingress(
        issn=xml_sps.issn,
        scielo_pid_v3=xml_sps.scielo_pid_v3,
    )

    assets_registration_result = register_assets(
        files_storage, files_storage_folder,
        doc_package, xml_sps.assets.items,
    )
    registered_xml = register_xml(
            files_storage, files_storage_folder, xml_sps)

    registered_renditions = register_renditions(
        files_storage, files_storage_folder,
        doc_package,
        classic_website_filename
    )
    return registered_xml, registered_renditions, assets_registration_result


def register_xml(files_storage, files_storage_folder, xml_sps):
    """
    Registra arquivo XML

    Parameters
    ----------
    files_storage : dsm.storage.minio.MinioStorage
        files storage object
    files_storage_folder : str
        pasta de destino no `files_storage`
    xml_sps : dsm.data.sps_package.SPS_Package
        objeto para manipular o XML

    Returns
    -------
    str
        URI do arquivo XML no `files_storage`

    """
    temp_dir = tempfile.mkdtemp()
    xml_path = os.path.join(temp_dir, f"{xml_sps.package_name}.xml")
    files.write_file(
        xml_path,
        xml_sps.xml_content.encode("utf-8"),
        "wb",
    )
    return files_storage_register(
            files_storage, files_storage_folder,
            xml_path, f"{xml_sps.package_name}.xml")


def register_renditions(files_storage, files_storage_folder,
                        doc_package, classic_website_filename):
    """
    Registra as manifestações do XML (`renditions`)

    Parameters
    ----------
    files_storage : dsm.storage.minio.MinioStorage
        files storage object
    files_storage_folder : str
        pasta de destino no `files_storage`
    doc_package : dsm.utils.packages.Package
        pacote de arquivos
    classic_website_filename: str
        nome do arquivo antes da migração, especialmente se html

    Returns
    -------
    list
        Lista de dados das manifestações (`renditions`)
        ```
        {
            "filename": "nome da manifestação",
            "url": "URI da manifestação",
            "size_bytes": quantidade de bytes,
            "mimetype": mimetype,
            "lang": idioma,
            "original_fname": classic_website_filename,
        }
        ```
    """
    mimetypes = MimeTypes()
    _renditions = []
    for lang, rendition_path in doc_package.renditions.items():
        rendition_basename = os.path.basename(rendition_path)
        _mimetype = mimetypes.guess_type(rendition_basename)[0]
        _rendition = {
            "filename": rendition_basename,
            "mimetype": _mimetype,
            "lang": lang,
            "original_fname": (
                f"{classic_website_filename}"
                if lang == "original" else f"{lang}_{classic_website_filename}"
            )
        }
        try:
            uri_and_name = _register_file(
                files_storage,
                files_storage_folder,
                rendition_path,
                doc_package.zip_file_path,
            )
            _rendition["url"] = uri_and_name["uri"]
        except exceptions.FilesStorageRegisterError as e:
            _rendition["error"] = str(e)
        if not doc_package.zip_file_path:
            _rendition["size_bytes"] = os.path.getsize(rendition_path)
        _renditions.append(_rendition)
    return _renditions


def register_assets(files_storage, files_storage_folder,
                    doc_package, assets_in_xml,
                    favorite_types=['.tiff', '.tif']):
    """
    Registra os ativos digitais do XML no `files_storage`

    Parameters
    ----------
    files_storage : dsm.storage.minio.MinioStorage
        files storage object
    files_storage_folder : str
        pasta de destino no `files_storage`
    doc_package : dsm.utils.packages.Package
        pacote de arquivos
    assets_in_xml : list of SPS_Asset
        assets objects found in XML

    """
    errors = []
    for asset_in_xml in assets_in_xml:
        # asset_in_xml is instance of SPS_Asset
        asset_file_path = doc_package.get_asset(asset_in_xml.xlink_href)
        # FIXME
        # parece que há um bug que faz com que asset_file_path is None
        # o xlink_href no XML não tem correspondente no pacote
        if not asset_file_path:
            print("%s has no corresponding file in package" % asset_in_xml.xlink_href)
            continue
        try:
            # atualiza os valores do atributo xlink:href dos ativos com a
            # uri do `files_storage`
            uri_and_name = _register_file(
                files_storage,
                files_storage_folder,
                asset_file_path,
                doc_package.zip_file_path,
            )
            asset_in_xml.xlink_href = uri_and_name["uri"]
        except exceptions.FilesStorageRegisterError as e:
            errors.append({"error": str(e)})
    return errors


def _register_file(files_storage, files_storage_folder, file_path, zip_file_path=None):
    basename = os.path.basename(file_path)

    if zip_file_path:
        with tempfile.TemporaryDirectory() as tmpdirname:
            with ZipFile(zip_file_path) as zf:
                extracted_path = zf.extract(file_path, path=tmpdirname)
                uri_and_name = files_storage_register(
                    files_storage, files_storage_folder,
                    extracted_path, basename)
                files.delete_folder(tmpdirname)
    else:
        # registra o arquivo no `files_storage`
        uri_and_name = files_storage_register(
            files_storage, files_storage_folder,
            file_path, basename)
    return uri_and_name


def files_storage_register(files_storage, files_storage_folder,
                           file_path, filename, preserve_name=True):
    try:
        uri = files_storage.register(
            file_path, files_storage_folder, filename, preserve_name
        )
        return {"uri": uri, "name": filename}

    except:
        raise
        raise exceptions.FilesStorageRegisterError(
            f"Unable to register {file_path}"
        )


def register_received_package(files_storage, pkg_path, _id, annotation=None):
    """

    Raises
    ------
        ReceivedPackageRegistrationError
    """
    zip_path = None
    basename = os.path.basename(pkg_path)

    if files.is_zipfile(pkg_path):
        zip_path = pkg_path
        zip_name = basename
        name, ext = os.path.splitext(basename)
    elif files.is_folder(pkg_path):
        zip_name = basename + ".zip"
        zip_content = [
            os.path.join(pkg_path, f)
            for f in os.listdir(pkg_path)
        ]

        zip_path = files.create_zip_file(zip_content, zip_name)
    if zip_path:
        files_storage_folder = configuration.get_files_storage_folder_for_received_packages(
            name, files.date_now_as_folder_name()
        )
        uri_and_name = files_storage_register(
            files_storage,
            files_storage_folder,
            zip_path,
            zip_name,
        )
        db.register_received_package(
            _id, uri_and_name["uri"], uri_and_name["name"],
            annotation=annotation)
    else:
        raise exceptions.ReceivedPackageRegistrationError(
            f"Unable to register {pkg_path}")
