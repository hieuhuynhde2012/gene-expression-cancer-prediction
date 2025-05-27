import pandas as pd
import re
from collections import Counter
import os

# Path to the GTF file
gtf_file = r"F:\gene-expression-cancer-prediction\data\raw\gencode\gencode.v48.annotation.gtf"

# Define GTF column names
gtf_columns = ["seqname", "source", "feature", "start", "end", "score", "strand", "frame", "attribute"]
gtf_data = pd.read_csv(gtf_file, sep="\t", comment="#", names=gtf_columns)

# ==== Phần kiểm tra các thuộc tính trong cột 'attribute' ====
all_attributes = gtf_data['attribute'].dropna().apply(
    lambda x: re.findall(r'(\S+) "', x)
)
flat_attrs = [item for sublist in all_attributes for item in sublist]
attr_counts = Counter(flat_attrs)

print("Các thuộc tính có trong 'attribute':")
for attr, count in attr_counts.items():
    print(f"{attr}: {count}")

# ==== Part 1: Extract Gene Information ====
genes = gtf_data[gtf_data['feature'] == 'gene'].copy()

# Extract gene_id, gene_name, and gene_type
genes['gene_id'] = genes['attribute'].str.extract('gene_id "([^"]+)"')
genes['gene_name'] = genes['attribute'].str.extract('gene_name "([^"]+)"')
genes['gene_type'] = genes['attribute'].str.extract('gene_type "([^"]+)"')

# Remove version from gene_id (e.g., ENSG00000290825.2 -> ENSG00000290825)
genes['gene_id'] = genes['gene_id'].str.replace(r'\.\d+$', '', regex=True)

# Select relevant columns
genes_info = genes[['gene_id', 'gene_name', 'gene_type', 'source', 'seqname', 'start', 'end', 'strand']].drop_duplicates()

# ==== Part 2: Extract Transcript IDs ====
transcripts = gtf_data[gtf_data['feature'] == 'transcript'].copy()
transcripts['gene_id'] = transcripts['attribute'].str.extract('gene_id "([^"]+)"')
transcripts['transcript_id'] = transcripts['attribute'].str.extract('transcript_id "([^"]+)"')

# Remove version from gene_id
transcripts['gene_id'] = transcripts['gene_id'].str.replace(r'\.\d+$', '', regex=True)

transcript_info = transcripts[['gene_id', 'transcript_id']].drop_duplicates()

# ==== Part 3: Extract Exon Coordinates ====
exons = gtf_data[gtf_data['feature'] == 'exon'].copy()
exons['gene_id'] = exons['attribute'].str.extract('gene_id "([^"]+)"')
exons['transcript_id'] = exons['attribute'].str.extract('transcript_id "([^"]+)"')

# Remove version from gene_id
exons['gene_id'] = exons['gene_id'].str.replace(r'\.\d+$', '', regex=True)

exon_info = exons[['gene_id', 'transcript_id', 'start', 'end', 'strand']]

# ==== Lưu file CSV ====
output_dir = r"F:\gene-expression-cancer-prediction\data\processed"
os.makedirs(output_dir, exist_ok=True)

genes_info.to_csv(os.path.join(output_dir, "genes_info.csv"), index=False)
transcript_info.to_csv(os.path.join(output_dir, "transcript_info.csv"), index=False)
exon_info.to_csv(os.path.join(output_dir, "exon_info.csv"), index=False)

# ==== Preview Outputs ====
print("\nGene Information:")
print(genes_info.head())

print("\nTranscript IDs per Gene:")
print(transcript_info.head())

print("\nExon Coordinates:")
print(exon_info.head())
