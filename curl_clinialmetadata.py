import csv
import requests
import json
import time

manifest_file = r"C:\Users\PC\Desktop\gene-expression-cancer-prediction\data\manifest.txt"
output_file = "clinical_metadata.json"

file_ids = []

with open(manifest_file, "r", encoding="utf-8-sig") as f:
    reader = csv.reader(f, delimiter="\t")
    header = next(reader, None)
    for row in reader:
        if row and len(row) > 0:
            file_ids.append(row[0])  # file_id

metadata = {}

for file_id in file_ids:
    print(f"Getting case_id from file_id: {file_id}")
    file_url = f"https://api.gdc.cancer.gov/files/{file_id}"
    file_response = requests.get(file_url)

    if file_response.status_code != 200:
        print(f"Failed to get file metadata for {file_id}")
        continue

    file_json = file_response.json()
    hits = file_json.get("data", {}).get("cases", [])
    if not hits:
        print(f"No case found for file_id {file_id}")
        continue

    case_id = hits[0]["case_id"]
    print(f"Fetching clinical data for case_id: {case_id}")

    clinical_url = f"https://api.gdc.cancer.gov/cases/{case_id}?expand=diagnoses,demographic,treatments,exposures"
    clinical_response = requests.get(clinical_url)

    if clinical_response.status_code == 200:
        metadata[case_id] = clinical_response.json()
    else:
        print(f"Failed to fetch clinical data for {case_id}. Status: {clinical_response.status_code}")
    
    # Tránh bị block do gửi nhiều request
    time.sleep(0.5)

# Ghi ra file JSON
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=2)
