CREATE DATABASE 4evar_test;

CREATE TABLE variant (
    variant_id VARCHAR(36) PRIMARY KEY,
    VAR_STRING VARCHAR(100) NOT NULL,
    CHROM CHAR(10) NOT NULL, 
    POS INT NOT NULL,
    REF VARCHAR(100) NOT NULL, 
    ALT VARCHAR(50) NOT NULL,
    GENE VARCHAR(300), 
    ACMG VARCHAR(100),
    FEATURE_ID VARCHAR(300), 
    EFFECT VARCHAR(300), 
    HGVS_C VARCHAR(300), 
    HGVS_P VARCHAR(300), 
    ClinVar VARCHAR(500),
    ClinVarCONF VARCHAR(500),
    Varsome_link VARCHAR(500), 
    Franklin_link VARCHAR(500)
);

CREATE TABLE sample (
    sample_id VARCHAR(36) PRIMARY KEY,
    file_name VARCHAR(500),
    VAF DECIMAL(7, 6), 
    GT INT,
    DP INT,
    RELIABLE BOOLEAN
);

CREATE TABLE instance (
    variant_id VARCHAR(36),
    sample_id VARCHAR(36),
    FOREIGN KEY (variant_id) REFERENCES variant(variant_id),
    FOREIGN KEY (sample_id) REFERENCES sample(sample_id)
);