# 4bases
All relevant work for the Python/SQL backend implementation 

testdb.sql:
+ contains the table definitions for the databse to be managed by db_manager.py, db_interface.py, vcf2df.py

db_manager.py:
+ program to populate the db with all variants and samples found in a given directory
    + the given directory needs to contain folders that have a correctly formatted .xlsx file
+ connects to a MySQL database, according to specifications in db_config dict
+ requires the folder path to be passed as command line parameter
+ MySQL db needs to already contain tables with matching column names and types
+ to run:
    + python3 db_manager.py ./path/to/folder >> out_files/out.txt
    + check out.txt for database insert actions / errors and connection status

db_interface.py:
+ an extendible interface that communicates with the db via MySQL queries sent by the mysql.connector python library
+ so far contains the following functions:
    + variants_in_sample(file_name)
        + list / return all variants found in a given sample (identified by its file name)
    + samples_containing_variant(var_string)
        + list / return all samples containing given variant (identified by its variant string)
+ to run:
    + python3 db_interface.py >> out_files/iface.txt

vcf2df.py:
+ program to check / add missing variant and sample information
+ logic flow:
    + for every variant string found in the given .vcf file, check if it exists in the database
        + (1) if the variant string does exist in the db, check that the corresponding sample also exists in the database
            + if the sample does exist in the db, simply record that variant's info to the existing.tsv file (automatically produced). 
            + if the sample does not exist in the db, make a new entry only for the sample in the db. Also make a new 'instance' entry, where sample_id is the id of the sample being inserted, and variant_id is the id of the existing variant 
        + (2) if the variant string does exist in the db, find that variant (by variant string) in the corresponding excel file and make a new entry into the 'variant' table in db, as well as new 'sample' and 'instance' tables.
+ to run:
    + python3 vcf2df.py >> out_files/out.txt


TESTS:
> work in progress
