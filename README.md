# 4bases
All relevant work for the Python/SQL backend implementation 

testdb.sql:
+ Contains the table definitions for the databse to be managed by db_manager.py, db_interface.py, vcf2df.py

db_manager.py:
+ Program to populate the db with all variants and samples found in a given directory
    + the given directory needs to contain folders that have a correctly formatted .xlsx file
+ Connects to a MySQL database, according to specifications in db_config dict
+ Requires the folder path to be passed as command line parameter
+ MySQL db needs to already contain tables with matching column names and types
+ To run:
    + python3 db_manager.py ./path/to/folder >> out_files/out.txt
    + check out.txt for database insert actions / errors and connection status

db_interface.py:
+ An extendible interface that communicates with the db via MySQL queries sent by the mysql.connector python library
+ So far contains the following functions:
    + variants_in_sample(file_name)
        + list / return all variants found in a given sample (identified by its file name)
    + samples_containing_variant(var_string)
        + list / return all samples containing given variant (identified by its variant string)
+ To run:
    + call each function with desired input in the main() function, then
        + python3 db_interface.py
        + optionally can run as python3 db_interface.py >> out_files/iface.txt to see full log of results

vcf2df.py:
+ Program to check / add missing variant and sample information
+ Logic flow:
    + for every variant string found in the given .vcf file, check if it exists in the database
        + (1) if the variant string does exist in the db, check that the corresponding sample also exists in the database
            + if the sample does exist in the db, simply record that variant's info to the existing.tsv file (automatically produced). 
            + if the sample does not exist in the db, make a new entry only for the sample in the db. Also make a new 'instance' entry, where sample_id is the id of the sample being inserted, and variant_id is the id of the existing variant 
        + (2) if the variant string does exist in the db, find that variant (by variant string) in the corresponding excel file and make a new entry into the 'variant' table in db, as well as new 'sample' and 'instance' tables.
+ To run:
    + python3 vcf2df.py
        + modify which file is read in the main method
        + optionally can run as python3 vcf2df.py >> out_files/out.txt to see full log of execution
        + check existing.tsv for existing variant results


TESTS:
+ db_manager
    + Made to test NULL values of database to ensure they are being read correctly. So far it checks the NULL values of:
        + VAF
        + GT
        + DP
    + Can be extended to test for null values of any other element of variant or sample data
    + In 2 parts:
        + null_test_db_manager.py - makes a separate table (according to structure in null_testing.sql) for each element of interest in a new test database, recording their coordinates in their original excel files. Important note: it is done in the same way that data is added to main database 
        + null_tester.py - compares the coordinates of null values in db to the coordinates in excel sheet. If they match, test is successful


+ db_interface
    + Reproduces the queries from the db_interface functions step by step, ensuring that:
        + retrieved variant/sample data is correct
        + retrieved data is connected to correct sample/variant

+ vcf2df
    + Accesses new_samples.json and new_variants.json to verify they did not exist before vcf2df.py ran (verify cardinality) as well as that they are connected with the correct variant/sample in the db
