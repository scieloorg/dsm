"""
CISIS COMMANDS
"""
import os
from datetime import datetime

from dsm import configuration
from dsm.utils.files import (
    create_temp_file, size, read_file,
    date_now_as_folder_name,
)
from dsm import exceptions


def _get_document_isis_db(pid):
    """
    Consulta a base de dados ISIS artigo e retorna os pids atualizados
    em um intervalo de datas (data de processamento do converter)

    """
    BASES_ARTIGO_PATH = configuration.get_bases_artigo_path()
    name = date_now_as_folder_name()
    finished_file_path = create_temp_file(f"{name}_finished.out")
    output_file_path = create_temp_file(f"{name}_output")
    from_date = from_date or '0'*8
    to_date = to_date or '9'*8

    cisis_path = configuration.get_cisis_path()
    cmds = []
    cmds.append(
        f'''{cisis_path}/mx {BASES_ARTIGO_PATH} btell=0 '''
        f'''"bool=IV={pid}$" '''
        f'''append={output_file_path} now -all'''
    )
    cmds.append(
        f"echo finished > {finished_file_path}"
    )
    os.system(";".join(cmds))
    while "finished" not in read_file(finished_file_path):
        pass
    return output_file_path
