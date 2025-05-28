-- Xoá bảng nếu tồn tại (theo thứ tự để tránh lỗi ràng buộc)
DROP TABLE IF EXISTS gene_expression;
DROP TABLE IF EXISTS exons;
DROP TABLE IF EXISTS transcripts;
DROP TABLE IF EXISTS samples;
DROP TABLE IF EXISTS treatment_outcomes;
DROP TABLE IF EXISTS patients;
DROP TABLE IF EXISTS genes;

-- Bảng gene
CREATE TABLE genes (
    gene_id VARCHAR(50) PRIMARY KEY,
    gene_name VARCHAR(100),
    gene_type VARCHAR(50),
    source VARCHAR(50),
    seqname VARCHAR(50),
    start INT,
    end INT,
    strand CHAR(1)
);

-- Bảng bệnh nhân
CREATE TABLE patients (
    patient_id VARCHAR(255) PRIMARY KEY,
    case_id VARCHAR(255) UNIQUE,
    age_at_diagnosis FLOAT,
    gender VARCHAR(50)
);

-- Bảng kết quả điều trị (có nhiều kết quả cho 1 bệnh nhân)
CREATE TABLE treatment_outcomes (
    treatment_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id VARCHAR(255),
    treatment_outcome VARCHAR(255),
    survival_time_months FLOAT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);

-- Bảng mẫu sinh thiết
CREATE TABLE samples (
    sample_id VARCHAR(255) PRIMARY KEY,
    case_id VARCHAR(255),
    FOREIGN KEY (case_id) REFERENCES patients(case_id)
);

-- Biểu hiện gene
CREATE TABLE gene_expression (
    expression_id INT AUTO_INCREMENT PRIMARY KEY,
    sample_id VARCHAR(255),
    gene_id VARCHAR(255),
    gene_name VARCHAR(255),
    gene_type VARCHAR(100),
    unstranded INT,
    stranded_first INT,
    stranded_second INT,
    tpm_unstranded FLOAT,
    fpkm_unstranded FLOAT,
    fpkm_uq_unstranded FLOAT,
    FOREIGN KEY (sample_id) REFERENCES samples(sample_id),
    FOREIGN KEY (gene_id) REFERENCES genes(gene_id)
);

-- Bảng transcript
CREATE TABLE transcripts (
    transcript_id VARCHAR(255) PRIMARY KEY,
    gene_id VARCHAR(255),
    strand CHAR(1),
    FOREIGN KEY (gene_id) REFERENCES genes(gene_id)
);

-- Bảng exon (thuộc transcript)
CREATE TABLE exons (
    exon_id INT AUTO_INCREMENT PRIMARY KEY,
    transcript_id VARCHAR(255),
    start INT,
    end INT,
    FOREIGN KEY (transcript_id) REFERENCES transcripts(transcript_id)
);
