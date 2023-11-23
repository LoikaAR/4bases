import os
import sys
from colorama import Fore
sys.path.append(os.path.relpath('../../'))
from db_interface import *
import mysql.connector
from mysql.connector import Error

connection = None # prepare the connection
path = str(sys.argv[1])             # the location of the folder with target files
dir_list = list(os.listdir(path))   # list of all files in the target folder

db_config = {
    'host': 'localhost',
    'database': '4evar_test',
    'user': 'root',
    'password': 'PassMass123!'
}

def main():
    try:
        connection = mysql.connector.connect(**db_config)

        if connection and connection.is_connected():
            db_info = connection.get_server_info()
            print("connected to server", db_info)

            cursor = connection.cursor(buffered=True)
            cursor.execute("select database();")

            db_name = cursor.fetchone()[0]
            print("connected to database", db_name)

            for dir in dir_list:
                new_p = path + '/' + dir
                folder = list(os.listdir(new_p))
                target = [i for i in folder if ".xlsx" in i]
                
                sample_file = target[0].split('.')[0]
                print(sample_file)
                res_var1 = variants_in_sample(sample_file) # returns a vector of dict objects - ID, var_string
                file_mismatch = False
                relation_mismatch = False
                # for every returned var string, verify that 1) it belongs to its ID, 2) it actually is found in sample_file
                
                for elem in res_var1:
                    q_verify_relation = ("SELECT VAR_STRING FROM variant WHERE variant_id = %s")
                    cursor.execute(q_verify_relation, [elem["ID"]])
                    relation_res = cursor.fetchone()

                    if (relation_res[0] != elem["var_string"]):
                        relation_mismatch = True
                        print(relation_res[0], "does not match", elem["var_string"])

                    q_verify_file1 = ("SELECT sample_id FROM instance WHERE variant_id = %s")
                    cursor.execute(q_verify_file1, [elem["ID"]])
                    id = cursor.fetchone()
                    
                    q_verify_file2 = ("SELECT file_name FROM sample WHERE sample_id = %s")
                    cursor.execute(q_verify_file2, id)

                    res = cursor.fetchone()
                    if (res[0] != sample_file):
                        file_mismatch = True
                        print(res[0], "does not match", sample_file)


                if (relation_mismatch or file_mismatch):
                    print(Fore.RED + "variants_in_sample(): TESTS FAILED")
                else: print(Fore.GREEN + "variants_in_sample(): TESTS PASSED for", sample_file)

            query_all_var_strings = ("SELECT VAR_STRING FROM variant;")
            cursor.execute(query_all_var_strings)
            var_strings = cursor.fetchall()                     # returns an array of tuples
            var_strings = [str(i[0]) for i in var_strings]      # make into array of strings
            varstring_mismatch = False
            relation_mismatch = False
            
            for vs in var_strings:
                test_vars = samples_containing_variant(vs)      # returns vector of dict objects - ID, sample_file
                for obj in test_vars:
                    q_verify_relation = ("SELECT file_name FROM sample WHERE sample_id = %s")
                    cursor.execute(q_verify_relation, [obj["ID"]])
                    rel_res = cursor.fetchone()

                    if (rel_res[0] != obj["file_name"]):
                        print("sample file", rel_res[0], "does not match", obj["file_name"])
                        relation_mismatch = True

                    q_verify_varstring1 = ("SELECT variant_id FROM instance WHERE sample_id = %s")
                    cursor.execute(q_verify_varstring1, [obj["ID"]])
                    var_id = cursor.fetchone()
                    q_verify_varstring2 = ("SELECT VAR_STRING FROM variant WHERE variant_id = %s")
                    cursor.execute(q_verify_varstring2, var_id)
                    var_string_res = cursor.fetchone()
                    if (var_string_res[0] != vs):
                        print("varstring", var_string_res, "doesnt match", vs)
                        varstring_mismatch = True
                
            if (relation_mismatch or varstring_mismatch):
                print(Fore.RED + "samples_containing_variant(): TESTS FAILED")
            else: print(Fore.GREEN + "samples_containing_variant(): TESTS PASSED")
    
    except Error as e:
        print("Error connecting to database,", e)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("Connection closed")



main()