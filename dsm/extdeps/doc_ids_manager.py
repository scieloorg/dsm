import os

from scielo_v3_manager.v3_gen import generates


def add_pids_to_xml(xml_sps, document, xml_file_path, pid_v2, v3_manager):
    """
    Garante que o PID v3 esteja no XML e que ele esteja registrado no SPF,
    seja no Article ou no v3_manager
    """
    # add scielo_pid_v2, se aplicável
    _add_scielo_pid_v2_to_xml(xml_sps, pid_v2)

    _add_document_pids_to_xml(xml_sps, document)

    if not xml_sps.scielo_pid_v3:
        _add_v3_manager_pids_to_xml(v3_manager, xml_sps, xml_file_path)


def _add_scielo_pid_v2_to_xml(xml_sps, pid_v2):
    """
    Add scielo_pid_v2 to xml_sps

    Parameters
    ----------
    xml_sps : dsm.data.sps_package.SPS_Package
        dados do pacote XML + PDFs + imagens
    pid_v2 : str
        pid v2, required if absent in XML

    """
    xml_sps.scielo_pid_v2 = xml_sps.scielo_pid_v2 or pid_v2


def _add_document_pids_to_xml(xml_sps, document):
    """
    Completa os PIDs do XML com os PIDs do documento registrado

    Parameters
    ----------
    xml_sps : dsm.data.sps_package.SPS_Package
        objeto para manipular o XML
    document : opac_schema.v1.models.Article
        documento registrado
    """
    if document and document._id:
        # completa XML com os PIDs de artigos já publicados no site
        xml_sps.scielo_pid_v3 = xml_sps.scielo_pid_v3 or document._id
        xml_sps.aop_pid = xml_sps.aop_pid or document.aop_pid
        if document.pid != xml_sps.scielo_pid_v2:
            xml_sps.aop_pid = document.pid


def _add_v3_manager_pids_to_xml(v3_manager, xml_sps, xml_file_path):
    """
    Completa os PIDs do XML com os PIDs do registro do v3_manager

    Verifica se o documento tem ou não pid v3 registrado.
    Se não tiver registrado, e nem estiver sugerido no XML,
    o pid v3 é gerado.
    Registra pid v3, se não tiver registrado.

    Parameters
    ----------
    xml_sps : dsm.data.sps_package.SPS_Package
        objeto para manipular o XML
    xml_file_path : str
        nome do arquivo XML

    Returns
    -------
    dict
        Se encontrado `erro`, retorna
            {"error": "mensagem de erro"}

        Se registrado, retorna os dados do registro de v3_manager
            {
                "v3": record.v3,
                "v2": record.v2,
                "aop": record.aop,
                "doi": record.doi,
                "status": record.status,
                "filename": record.filename,
                "created": record.created,
                "updated": record.updated,
            }

    """
    if not v3_manager:
        return {"error": "v3_manager is not instanciated"}

    # completa XML com os PIDs de artigos registrados no v3_manager
    if not xml_sps.scielo_pid_v2:
        return {"error": "Required PID v2"}

    result = v3_manager.manage(
        v2=xml_sps.scielo_pid_v2,
        v3=xml_sps.scielo_pid_v3,
        aop=xml_sps.aop_pid,
        filename=os.path.basename(xml_file_path),
        doi=xml_sps.doi,
        status="active",
        generate_v3=generates,
    )
    if result.get("error"):
        return result

    record = result.get("saved") or result.get("registered")
    xml_sps.scielo_pid_v2 = xml_sps.scielo_pid_v2 or record.get("v2")
    xml_sps.scielo_pid_v3 = xml_sps.scielo_pid_v3 or record.get("v3")
    xml_sps.aop_pid = xml_sps.aop_pid or record.get("aop")
    xml_sps.doi = xml_sps.doi or record.get("doi")
    return record
