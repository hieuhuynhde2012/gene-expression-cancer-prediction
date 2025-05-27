import os
import pandas as pd
import json

# Define the root directory containing all sample subdirectories
root_dir = r"F:\gene-expression-cancer-prediction\data\raw\rna_seq"

# Initialize a list to collect cleaned data from all valid samples
all_samples_data = []

# Traverse all subdirectories in the root directory
for sample_folder in os.listdir(root_dir):
    sample_path = os.path.join(root_dir, sample_folder)

    if not os.path.isdir(sample_path):
        continue

    # Find the first .tsv file in the sample directory
    tsv_files = [f for f in os.listdir(sample_path) if f.endswith('.tsv')]
    if not tsv_files:
        print(f"Warning: No .tsv file found in {sample_path}")
        continue

    tsv_path = os.path.join(sample_path, tsv_files[0])
    print(f"Reading file: {tsv_path}")

    # Use the file name (without extension) as the sample ID
    sample_id = os.path.splitext(tsv_files[0])[0]
    print(f"Processing sample: {sample_id}")

    try:
        # Explicitly set dtype for columns to prevent unwanted type inference
        df = pd.read_csv(
            tsv_path,
            sep="\t",
            comment='#',
            dtype={
                "gene_id": str,
                "gene_name": str,
                "gene_type": str
            },
            na_values=[]  # prevent pandas from auto-interpreting values as NaN
        )

        # Drop rows where gene_id is metadata (e.g., starting with '__')
        df = df[~df["gene_id"].astype(str).str.startswith('__')]

        if df.empty:
            print(f"No valid data left after removing metadata in {sample_id}")
            continue

        # Remove version suffix from gene_id (e.g., ENSG00000000003.15 → ENSG00000000003)
        df["gene_id"] = df["gene_id"].astype(str).str.split('.').str[0]

        # Convert numeric columns (skip first 3 which are gene_id, gene_name, gene_type)
        numeric_cols = df.columns[3:]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        # Drop rows with NaNs in expression values
        df_clean = df.dropna(subset=[
            'unstranded', 'stranded_first', 'stranded_second',
            'tpm_unstranded', 'fpkm_unstranded', 'fpkm_uq_unstranded'
        ])

        if df_clean.empty:
            print(f"No valid data left after dropping NaNs in {sample_id}")
            continue

        # Add sample_id column
        df_clean['sample_id'] = sample_id

        # Reset index
        df_clean = df_clean.reset_index(drop=True)

        print(f"Finished processing sample: {sample_id}, shape: {df_clean.shape}")
        all_samples_data.append(df_clean)

    except Exception as e:
        print(f"Error processing {sample_id}: {e}")
        continue

# Combine all cleaned data
if all_samples_data:
    combined_df = pd.concat(all_samples_data, ignore_index=True)
    print(f"Total samples processed: {len(all_samples_data)}")
    print(f"Combined DataFrame shape: {combined_df.shape}")

    # Save to file
    output_path = r"F:\gene-expression-cancer-prediction\data\processed\cleaned_gene_expression_data.csv"
    combined_df.to_csv(output_path, index=False)
    print(f"Saved cleaned data to {output_path}")
else:
    print("No valid samples were processed.")


csv_file_path = r"F:\gene-expression-cancer-prediction\data\processed\cleaned_gene_expression_data.csv"
df = pd.read_csv(csv_file_path, sep=",")


df['sample_id_clean'] = df['sample_id'].apply(lambda x: x.split(".")[0])
print(df['sample_id_clean'])

json_file = r"F:\gene-expression-cancer-prediction\data\raw\rna_seq\metadata.cart.json"
with open(json_file, 'r') as f:
    metadata = json.load(f)
    
file_mappings = {}
for entry in metadata:
    file_name = entry.get("file_name", "")
    # Extract UUID from the file_name
    if file_name.endswith(".rna_seq.augmented_star_gene_counts.tsv"):
        sample_uuid = file_name.split(".")[0]
        
        #Extract case_id from associated_entities if available
        case_id = None
        associated_entities = entry.get("associated_entities", [])
        if associated_entities and isinstance(associated_entities,list):
            case_id = associated_entities[0].get("case_id")
            
        if not case_id:
            case_id = entry.get("cases", [{}])[0].get("case_id") or entry.get("submitter_id")
            
        file_mappings[sample_uuid] = case_id
        
df['case_id'] = df['sample_id_clean'].map(file_mappings)
missing = df[df['case_id'].isna()]

if not missing.empty:
    print("Sample IDs not found in metadata:")
    print(missing['sample_id'])
    
output_file = "F:\gene-expression-cancer-prediction\data\processed\cleaned_gene_expression_data_with_case_id.csv"
df.drop(columns=['sample_id_clean'], inplace=True)
df.to_csv(output_file, index=False)
print(f"Output saved to {output_file}")