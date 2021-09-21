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


def create_id_file(db_file_path, id_file_path=None):
    """
    Generates ID file `id_file_path` of a ISIS database `db_file_path`

    Parameters
    ----------
    db_file_path: str
        path of an ISIS database without extension
    id_file_path: str
        path of the ID file to be created

    Returns
    -------
    str
        id_file_path

    Raises
    ------
    exceptions.MissingCisisPathEnvVarError
    exceptions.CisisPathNotFoundMigrationError
    exceptions.MissingI2IdCommandPathEnvVarError
    exceptions.IsisDBNotFoundError
    PermissionError
    FileNotFoundError
    """
    # obtém CISIS_PATH
    cisis_path = configuration.get_cisis_path()

    # check if the utilitary i2id exists
    i2id_cmd = os.path.join(cisis_path, "i2id")
    if not os.path.isfile(i2id_cmd):
        raise exceptions.MissingI2IdCommandPathEnvVarError(
            f"Not found: {i2id_cmd}"
        )

    # check if the isis database exists
    if not os.path.isfile(db_file_path + ".mst"):
        raise exceptions.IsisDBNotFoundError(f"Not found {db_file_path}.mst")

    if id_file_path is None:
        # create id_file in a temp folder
        id_file_path = create_temp_file(os.path.basename(db_file_path))
    else:
        # create the destination folder
        dirname = os.path.dirname(id_file_path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname)

        # delete the id_file_path
        write_file(id_file_path, "")

    # execute i2id db > id_file_path
    os.system(f"{i2id_cmd} {db_file_path} > {id_file_path}")

    # check if id_file_path is valid
    try:
        content = read_file(id_file_path, encoding="iso-8859-1")
    except FileNotFoundError:
        return None
    else:
        return id_file_path


def get_id_file_path(source_file_path):
    """
    Evaluate `source_file_path` and returns `source_file_path` if it is ID file
    or create its ID file

    Parameters
    ----------
    source_file_path: str
        path of a ISIS Database or ID file

    Returns
    -------
    str

    Raises
    ------
        exceptions.IdFileNotFoundError
        exceptions.IsisDBNotFoundError

    """
    name, ext = os.path.splitext(source_file_path)
    if ext == ".id":
        # `source_file_path` is an ID file
        if not os.path.isfile(source_file_path):
            raise exceptions.IdFileNotFoundError(
                f"Unable to `get_id_file_path` from {source_file_path}. "
                f"Not found {source_file_path}"
            )
        return source_file_path
    else:
        # `source_file_path` is an ISIS databse, so create its ID file
        if not os.path.isfile(source_file_path + ".mst"):
            raise exceptions.IsisDBNotFoundError(
                f"Unable to `get_id_file_path` from {source_file_path}. "
                f"Not found {source_file_path}.mst"
            )
        return create_id_file(source_file_path)
