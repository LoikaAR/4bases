CREATE DATABASE 4evar_test;

CREATE TABLE gen_info (
    chrom CHAR(5) NOT NULL, 
    pos INT PRIMARY KEY, 
    alt CHAR(1) NOT NULL, 
    vaf DECIMAL(7, 6) NOT NULL, 
    gene CHAR(4) NOT NULL, 
    feature_id CHAR(12) NOT NULL, 
    effect VARCHAR(50) NOT NULL, 
    hgvs_c VARCHAR(20) NOT NULL, 
    hgvs_p VARCHAR(20), 
    varsome_link VARCHAR(100) NOT NULL, 
    franklin_link VARCHAR(100) NOT NULL
);



INSERT INTO gen_info (chrom, pos, alt, vaf, gene, feature_id, effect, hgvs_c, hgvs_p, varsome_link, franklin_link)
VALUES ('chr1', 1001, 'A', 0.045678, 'BRCA', 'ENSG12345', 'Missense', 'c.123A>T', 'p.Asp41Thr', 'https://varsome.com/123', 'https://franklin.com/123');

INSERT INTO gen_info (chrom, pos, alt, vaf, gene, feature_id, effect, hgvs_c, hgvs_p, varsome_link, franklin_link)
VALUES ('chr2', 56789, 'G', 0.001234, 'TP53', 'ENSG67890', 'Frameshift', 'c.789_790del', 'p.Glu263fs', 'https://varsome.com/456', 'https://franklin.com/456');

INSERT INTO gen_info (chrom, pos, alt, vaf, gene, feature_id, effect, hgvs_c, hgvs_p, varsome_link, franklin_link)
VALUES ('chr3', 12345, 'T', 0.008765, 'EGFR', 'ENSG33333', 'Intron', 'c.678+123T>A', NULL, 'https://varsome.com/789', 'https://franklin.com/789');