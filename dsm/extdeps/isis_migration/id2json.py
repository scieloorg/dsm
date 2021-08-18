"""
!ID 000001
!v002!1414-431X-bjmbr-1414-431X20165409.xml
!v004!v49n8
!v091!20190319
!v092!1010
!v093!20190319
!v700!1
!v701!1
!v702!bjmbr/v49n8/1414-431X-bjmbr-1414-431X20165409.xml
!v703!39
"""
import json
import os

from dsm.utils import files


def _get_value(data, tag):
    """
    Returns first value of field `tag`
    """
    # data['v880'][0]['_']
    try:
        return data[tag][0]['_']
    except (KeyError, IndexError):
        return None


def _get_items(data, tag):
    """
    Returns first value of field `tag`
    """
    # data['v880'][0]['_']
    try:
        return [item['_'] for item in data[tag]]
    except KeyError:
        return None


def _parse_field_content(content):
    if not content:
        return
    if "^" not in content:
        return {"_": content}
    if not content.startswith("^"):
        content = "^_" + content
    content = content.replace("\\^", "ESCAPECIRC")
    subfields = content.split("^")
    items = []
    for subf in subfields:
        if not subf:
            continue
        s = subf[0]
        if s in "_abcdefghijklmnopqrstuvwxyz123456789":
            items.append([s, subf[1:]])
        else:
            items.append(["", "\\^" + s + subf[1:]])

    for i, item in enumerate(items):
        s, v = item
        if s == "":
            items[i-1][1] += v
            items[i][1] = ""

    d = {}
    for s, v in items:
        if s and v:
            d[s] = v
    return d


def _parse_field(data):
    second_excl_char_pos = data[1:].find("!")+1
    tag = data[1:second_excl_char_pos]
    subfields = _parse_field_content(data[second_excl_char_pos+1:])
    return (tag, subfields)


def _build_record(record):
    if not record:
        return
    data = {}
    for k, v in record:
        if not k or not v:
            continue
        data.setdefault(k, [])
        data[k].append(v)
    return data


def journal_id(data):
    return _get_value(data, 'v400')


def issue_id(data):
    try:
        return "".join(
                [
                    _get_value(data, 'v035'),
                    _get_value(data, 'v036')[:4],
                    _get_value(data, 'v036')[4:].zfill(4),
                ]
            )
    except:
        print('issue_id')
        print(data)
        raise


def article_id(data):
    try:
        return _get_value(data, 'v880')[:23]
    except (TypeError, IndexError, KeyError):
        return issue_id(data)


def _journal_filename(_id):
    return "", _id + ".json"


def _issue_filename(_id):
    try:
        return "", _id + ".json"
    except:
        print('issue_filename')
        print(_id)
        raise


def _article_filename(_id):
    try:
        name = _id
        path = name[1:10]
        return path, name + ".json"
    except KeyError:
        return _issue_filename(_id)


def _get_fields_and_their_content(content):
    if not content:
        return
    rows = content.splitlines()
    return [
        _parse_field(row)
        for row in rows[1:]
        if row
    ]


def _save_records(records, curr_filename, prev_filename, output_file_path):
    curr_path, curr_name = curr_filename
    prev_path, prev_name = prev_filename
    if prev_name and prev_name != curr_name:
        if prev_path:
            file_path = os.path.join(
                output_file_path, prev_path, prev_name)
        else:
            file_path = os.path.join(output_file_path, prev_name)
        files.write_file(file_path, json.dumps(records), "a")
        records = []
    return records


def get_json_records(input_file_path, get_id_function):
    content = files.read_file(input_file_path, encoding='iso-8859-1')

    items = {}
    for record_content in ("\n" + content).split("\n!ID "):
        try:
            fields = _get_fields_and_their_content(record_content)
            data = _build_record(fields)
            if not data:
                continue

            _id = get_id_function(data)
            items.setdefault(_id, [])
            items[_id].append(data)
        except:
            raise
    return ((k, v) for k, v in items.items())


def save_json_files(records, output_file_path, get_file_path_function):
    for record in records:
        try:
            _id = record.get("_id")
            records = record.get("records")
            file_folder, file_name = get_file_path_function(_id)
            file_path = os.path.join(output_file_path, file_folder, file_name)
            files.write_file(file_path, json.dumps(records))
        except:
            print('save_json_files')
            print(record)
            raise


def id2json_file(input_file_path, output_file_path, get_id_function,
                 get_file_path_function):
    for records in get_json_docs(input_file_path, get_id_function):
        save_json_files(records, output_file_path, get_file_path_function)


# def create_json(input_file_path, output_file_path, get_filename):
#     content = files.read_file(input_file_path, encoding='iso-8859-1')

#     records = []
#     prev_filename = "", ""
#     for record_content in ("\n" + content).split("\n!ID "):
#         try:
#             fields = _get_fields_and_their_content(record_content)
#             data = _build_record(fields)
#             if not data:
#                 continue
#             records.append(data)

#             curr_filename = get_filename(data)
#             records = _save_records(
#                 records, curr_filename, prev_filename, output_file_path)
#             prev_filename = curr_filename
#         except:
#             print(record_content)
#             raise
