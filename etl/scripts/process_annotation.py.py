import pandas as pd
import re

# Path to the GTF file
gtf_file = r"C:\Users\PC\Desktop\gene-expression-cancer-prediction\gencode\gencode.v48.annotation.gtf"

# Define GTF column names
gtf_columns = ["seqname", "source", "feature", "start", "end", "score", "strand", "frame", "attribute"]
gtf_data = pd.read_csv(gtf_file, sep="\t", comment="#", names=gtf_columns)

# ==== Part 1: Extract Gene Information ====
# Filter rows where the feature is 'gene'
genes = gtf_data[gtf_data['feature'] == 'gene'].copy()

# Extract gene_id, gene_name, and gene_type using regex
genes['gene_id'] = genes['attribute'].str.extract('gene_id "([^"]+)"')
genes['gene_name'] = genes['attribute'].str.extract('gene_name "([^"]+)"')
genes['gene_type'] = genes['attribute'].str.extract('gene_type "([^"]+)"')

# Select relevant columns and remove duplicates
genes_info = genes[['gene_id', 'gene_name', 'gene_type', 'seqname', 'start', 'end', 'strand']].drop_duplicates()

# ==== Part 2: Extract Transcript IDs ====
# Filter rows where the feature is 'transcript'
transcripts = gtf_data[gtf_data['feature'] == 'transcript'].copy()

# Extract gene_id and transcript_id
transcripts['gene_id'] = transcripts['attribute'].str.extract('gene_id "([^"]+)"')
transcripts['transcript_id'] = transcripts['attribute'].str.extract('transcript_id "([^"]+)"')

# Select relevant columns and remove duplicates
transcript_info = transcripts[['gene_id', 'transcript_id']].drop_duplicates()

# ==== Part 3: Extract Exon Coordinates ====
# Filter rows where the feature is 'exon'
exons = gtf_data[gtf_data['feature'] == 'exon'].copy()

# Extract gene_id and transcript_id
exons['gene_id'] = exons['attribute'].str.extract('gene_id "([^"]+)"')
exons['transcript_id'] = exons['attribute'].str.extract('transcript_id "([^"]+)"')

# Select relevant columns (including exon coordinates and strand)
exon_info = exons[['gene_id', 'transcript_id', 'start', 'end', 'strand']]

# ==== Preview Outputs ====
print("Gene Information:")
print(genes_info.head())

print("\nTranscript IDs per Gene:")
print(transcript_info.head())

print("\nExon Coordinates:")
print(exon_info.head())
