"""
Module converts file.id in

```
!ID 000001
!v002!1414-431X-bjmbr-1414-431X20165409.xml
!v012!New record of Blepharicnema splendens^len
!v049!^cAA970^lpt^tBiodiversidade e Conservação
!v049!^cAA970^len^tBiodiversity and Conservation
```

to JSON

```
[
   "v002": [
       {"_": "1414-431X-bjmbr-1414-431X20165409.xml"}
   ],
   "v012": [
       {"_": "New record of Blepharicnema splendens", "l": "en"}
   ],
   "v049": [
       {"c": "AA970", "l": "pt", "t": "Biodiversidade e Conservação"},
       {"c": "AA970", "l": "en", "t": "Biodiversity and Conservation"},
   ]
]
```

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
    record_type = _get_value(data, 'v706')
    if record_type == "i":
        return issue_id(data)
    try:
        return _get_value(data, 'v880')[:23]
    except (TypeError, IndexError, KeyError):
        return "".join([
            "S",
            _get_value(data, 'v035'),
            _get_value(data, 'v036')[:4],
            _get_value(data, 'v036')[4:].zfill(4),
            _get_value(data, 'v121').zfill(5),
        ])


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
        for row in rows
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


def read_id_file(input_file_path):
    rows = []
    with open(input_file_path, "r", encoding="iso-8859-1") as fp:
        for row in fp:
            if row.startswith("!ID "):
                if len(rows):
                    yield "\n".join(rows)
                    rows = []
            else:
                rows.append(row.strip())
        yield "\n".join(rows)


def get_json_records(input_file_path, get_id_function):
    items = {}
    for record_content in read_id_file(input_file_path):
        if not record_content:
            continue
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
    for records in get_json_records(input_file_path, get_id_function):
        save_json_files(records, output_file_path, get_file_path_function)


def get_paragraphs_records(paragraphs_id_file_path):
    if os.path.isfile(paragraphs_id_file_path):
        _id, p_records = get_json_records(paragraphs_id_file_path, article_id)
        return p_records
