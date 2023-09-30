CREATE DATABASE 4evar_test;

CREATE TABLE gen_info (
    variant_id INT PRIMARY KEY,
    CHROM CHAR(5) NOT NULL, 
    POS INT,
    REF CHAR(100), 
    ALT CHAR(50), 
    VAF DECIMAL(7, 6), 
    GT VARCHAR(10),
    DP INT,
    GENE CHAR(100), 
    FEATURE_ID CHAR(20), 
    EFFECT VARCHAR(100), 
    HGVS_C VARCHAR(200), 
    HGVS_P VARCHAR(100), 
    ClinVar VARCHAR(500),
    ClinVarCONF VARCHAR(500),
    Varsome_link VARCHAR(500) NOT NULL, 
    Franklin_link VARCHAR(500) NOT NULL
);



-- INSERT INTO gen_info (chrom, pos, alt, vaf, gene, feature_id, effect, hgvs_c, hgvs_p, varsome_link, franklin_link)
-- VALUES ('chr1', 1001, 'A', 0.045678, 'BRCA', 'ENSG12345', 'Missense', 'c.123A>T', 'p.Asp41Thr', 'https://varsome.com/123', 'https://franklin.com/123');

-- INSERT INTO gen_info (chrom, pos, alt, vaf, gene, feature_id, effect, hgvs_c, hgvs_p, varsome_link, franklin_link)
-- VALUES ('chr2', 56789, 'G', 0.001234, 'TP53', 'ENSG67890', 'Frameshift', 'c.789_790del', 'p.Glu263fs', 'https://varsome.com/456', 'https://franklin.com/456');

-- INSERT INTO gen_info (chrom, pos, alt, vaf, gene, feature_id, effect, hgvs_c, hgvs_p, varsome_link, franklin_link)
-- VALUES ('chr3', 12345, 'T', 0.008765, 'EGFR', 'ENSG33333', 'Intron', 'c.678+123T>A', NULL, 'https://varsome.com/789', 'https://franklin.com/789');