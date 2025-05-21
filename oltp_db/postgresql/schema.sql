-- Xóa bảng nếu tồn tại (theo thứ tự phụ thuộc)
DROP TABLE IF EXISTS FactGeneExpression;
DROP TABLE IF EXISTS DimDate;
DROP TABLE IF EXISTS DimGene;
DROP TABLE IF EXISTS DimPatient;

-- Dimension Patient
CREATE TABLE DimPatient (
    patient_id VARCHAR PRIMARY KEY,
    age_group VARCHAR,
    gender VARCHAR,
    treatment_outcome VARCHAR
);

-- Dimension Gene
CREATE TABLE DimGene (
    gene_id VARCHAR PRIMARY KEY,
    gene_symbol VARCHAR,
    pathway TEXT,
    cancer_related BOOLEAN
);

-- Dimension Date
CREATE TABLE DimDate (
    date_id DATE PRIMARY KEY,
    year INT,
    quarter INT,
    day INT
);

-- Fact table
CREATE TABLE FactGeneExpression (
    expression_id SERIAL PRIMARY KEY,
    patient_id VARCHAR REFERENCES DimPatient(patient_id),
    gene_id VARCHAR REFERENCES DimGene(gene_id),
    date_id DATE REFERENCES DimDate(date_id),
    fpkm_value FLOAT,
    survival_time_months INT
);
