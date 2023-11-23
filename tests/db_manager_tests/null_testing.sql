-- COMMANDS TO SET UP NULL TESTING DB

CREATE DATABASE null_testing;

CREATE TABLE DP (
    dp_id INT PRIMARY KEY,
    val INT,
    row_num INT NOT NULL,
    col_num INT NOT NULL,
    is_null BOOLEAN NOT NULL,
    file_name VARCHAR(100)
);

CREATE TABLE GT (
    gt_id INT PRIMARY KEY,
    val VARCHAR(5),
    row_num INT NOT NULL,
    col_num INT NOT NULL,
    is_null BOOLEAN NOT NULL,
    file_name VARCHAR(100)
);

CREATE TABLE VAF (
    vaf_id INT PRIMARY KEY,
    val DECIMAL(7, 6), 
    row_num INT NOT NULL,
    col_num INT NOT NULL,
    is_null BOOLEAN NOT NULL,
    file_name VARCHAR(100)
);

CREATE TABLE sample (
    sample_id VARCHAR(36) PRIMARY KEY,
    VAF INT, 
    GT INT,
    DP INT,
    FOREIGN KEY (VAF) REFERENCES VAF(vaf_id),
    FOREIGN KEY (GT) REFERENCES GT(gt_id),
    FOREIGN KEY (DP) REFERENCES DP(dp_id)
);