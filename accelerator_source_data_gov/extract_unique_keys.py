import os
import json


def extract_keys_and_extras(data, prefix='', extras_keys=None):
    if extras_keys is None:
        extras_keys = set()
    keys = set()

    if isinstance(data, dict):
        for k, v in data.items():
            current_path = f"{prefix}.{k}" if prefix else k
            if k == 'extras' and isinstance(v, list):
                keys.add(current_path)
                for item in v:
                    if isinstance(item, dict) and 'key' in item:
                        extras_keys.add(item['key'])
                        keys.add(f"{current_path}.[].key")
            else:
                keys.add(current_path)
                child_keys, _ = extract_keys_and_extras(v, current_path, extras_keys)
                keys.update(child_keys)
    elif isinstance(data, list):
        for item in data:
            child_keys, _ = extract_keys_and_extras(item, prefix + '.[]' if prefix else '[]', extras_keys)
            keys.update(child_keys)
    return keys, extras_keys


def read_json_files_from_dir(directory):
    all_keys_terms = set()
    all_unique_keys_in_extras = set()
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    file_keys, file_extras_keys = extract_keys_and_extras(data)
                    all_keys_terms.update(file_keys)
                    all_unique_keys_in_extras.update(file_extras_keys)
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
    return all_keys_terms, all_unique_keys_in_extras


def write_to_file(filename, items):
    with open(filename, 'w', encoding='utf-8') as f:
        for item in sorted(items):
            f.write(item + '\n')
    print(f"Wrote {len(items)} items to {filename}")


# USAGE
json_directory = '../tests/test_resources/datagov_dump_04_02_2025'
unique_keys, extras_key_values = read_json_files_from_dir(json_directory)

write_to_file('../tests/test_resources/schema_keys_terms.txt', unique_keys)
write_to_file('../tests/test_resources/unique_keys_in_extras.txt', extras_key_values)