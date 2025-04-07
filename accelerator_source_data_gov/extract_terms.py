import os
import json
import glob
import csv
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)

# Path to your JSON files
json_dir = "../tests/test_resources/datagov_dump_04_02_2025"  # update this with your actual path
output_csv = "../tests/test_resources/extracted_terms.csv"

# Fields you want to extract
fields_to_extract = {
    "title": "title",
    "name": "name",
    "description": "description",
    "notes": "notes",
    "download URL": "download URL",
    "accessURL": "accessURL",
    "category": "category",
    "about": "about",
    "theme": "theme",
    "Tags": "tags",
    "keyword": "keyword",
    "Homepage URL": "Homepage URL",
    "landingPage": "landingPage",
    "dataset": "dataset",
    "catalog fields": "catalog_conformsTo"
}

# Function to extract from extras if present
def extract_from_extras(extras, key_name):
    for item in extras:
        if item.get("key") == key_name:
            return item.get("value")
    return None

# Function to extract terms from a single JSON object
def extract_terms(data):
    result = {}

    for label, keys in fields_to_extract.items():
        if isinstance(keys, list):
            # Try each possible key in the list
            for key in keys:
                # Check top-level first
                if key in data and data[key]:
                    result[label] = data[key]
                    break
                # Check in 'extras'
                elif "extras" in data:
                    val = extract_from_extras(data["extras"], key)
                    if val:
                        result[label] = val
                        break
        else:
            # Single key
            if keys in data and data[keys]:
                result[label] = data[keys]
            elif "extras" in data:
                val = extract_from_extras(data["extras"], keys)
                if val:
                    result[label] = val

    # Handle special case: Tags
    if "tags" in data:
        result["Tags"] = ", ".join(tag.get("name") for tag in data["tags"] if tag.get("name"))

    return result

# Collect data
all_results = []
for filepath in glob.glob(os.path.join(json_dir, "*.json")):
    logger.info(f"Processing file: {filepath}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            extracted = extract_terms(data)
            extracted["file"] = os.path.basename(filepath)
            all_results.append(extracted)
    except Exception as e:
        print(f"Error processing {filepath}: {e}")

# Write to CSV
if all_results:
    keys = sorted(set().union(*(d.keys() for d in all_results)))
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_results)

print(f"Extraction complete. Output saved to {output_csv}")
