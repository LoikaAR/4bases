import os
import sys
import mysql.connector
from openpyxl import load_workbook
from mysql.connector import Error

path = str(sys.argv[1])             # the location of the folder with target files
dir_list = list(os.listdir(path))   # list of all files in the target folder
connection = None                   # prepare the connection
DP_THRESHOLD = 20                   # depth threshold

db_config = {
    'host': 'localhost',
    'database': '4evar_test',
    'user': 'root',
    'password': 'PassMass123!'
}

def main():
    try: 
        connection = mysql.connector.connect(**db_config) # user and password should be parameters?

        if connection and connection.is_connected():
            db_info = connection.get_server_info()
            print("connected to server ", db_info)

            cursor = connection.cursor(buffered=True) # buffered=True prevents unread result error 
            cursor.execute("select database();")

            db_name = cursor.fetchone()[0]
            print("connected to database", db_name)

            for dir in dir_list:
                new_p = path + '/' + dir
                folder = list(os.listdir(new_p))
                
                # get the excel file
                target = [i for i in folder if ".xlsx" in i]
                file = target[0]
                file_name = file.replace('.xlsx','')

                data_file = path + dir + '/' + file
                print("current file:", data_file)
                
                wb = load_workbook(data_file) # Load the entire workbook
                ws = wb['Sheet1']             # Load the worksheet

                rows = list(ws.rows)
                cols = list(ws.columns)
                print(f"found {len(rows)-1} variants in {file_name}")

                # remove whitespaces so col names work with MySQL syntax
                for i in range(len(rows[0])):
                    if rows[0][i].value.find(" ") != -1:
                        rows[0][i].value = "_".join(rows[0][i].value.split())

                for i in range (1, len(rows)):
                    # prepare query data for db
                    query_data_variant = {
                        "variant_id": None,
                        "VAR_STRING": None,
                        "CHROM": None,
                        "POS": None,
                        "REF": None,
                        "ALT": None,
                        "GENE": None,
                        "ACMG": None,
                        "FEATURE_ID": None,
                        "EFFECT": None,
                        "HGVS_C": None,
                        "HGVS_P": None,
                        "ClinVar": None,
                        "ClinVarCONF": None,
                        "Varsome_link": None,
                        "Franklin_link": None
                    }

                    query_data_sample = {
                        "sample_id": None,
                        "file_name": file_name,
                        "VAF": None,
                        "GT": None,
                        "DP": None,
                        "RELIABLE": None
                    }

                    query_data_instance = {
                        "variant_id": None,
                        "sample_id": None
                    }

                    j = 0
                    variant_string = ""
                    for cell in rows[i]:
                        current = rows[0][j].value # the column name

                        if cell.value == '.': # assuming "." is equal to NULL
                            cell.value = None

                        if current == "VAF":
                            query_data_sample[current] = cell.value
                        elif current == "DP":
                            query_data_sample[current] = cell.value
                            query_data_sample["RELIABLE"] = True if cell.value != None and int(cell.value) > DP_THRESHOLD else False
                        elif current == "GT":
                            if cell.value == "het":
                                query_data_sample[current] = 1
                            elif cell.value == "hom":
                                query_data_sample[current] = 2

                        else:
                            # print(f"{current}: {cell.value}}")
                            query_data_variant[current] = cell.value
                            if (current == "CHROM" or current == "REF"):
                                variant_string += str(cell.value)
                            elif (current == "POS" or current == "ALT"):
                                variant_string += str(cell.value)
                        query_data_variant["VAR_STRING"] = variant_string
                        j += 1

                    # print(variant_string)

                    # check that var string already exists
                    query_check_vs = ("SELECT variant_id FROM variant WHERE VAR_STRING = %(VAR_STRING)s")
                    cursor.execute(query_check_vs, query_data_variant)

                    vs_res = cursor.fetchone()

                    cursor.execute("SET @var_id = uuid()")
                    cursor.execute("SET @sam_id = uuid()")

                    # print("vs_res:", vs_res)

                    query_sample = (
                            "INSERT INTO sample "
                            "(sample_id, file_name, VAF, GT, DP, RELIABLE) VALUES "
                            "(@sam_id, %(file_name)s, %(VAF)s, %(GT)s, %(DP)s, %(RELIABLE)s)"
                            )
                    cursor.execute(query_sample, query_data_sample)

                    # if variant string was not present:
                    if (vs_res == None):
                        query_variant = (
                                "INSERT INTO variant "
                                "(variant_id, VAR_STRING, CHROM, POS, REF, ALT, GENE, ACMG, FEATURE_ID, "
                                "EFFECT, HGVS_C, HGVS_P, ClinVar, ClinVarCONF, Varsome_link, Franklin_link) "
                                "VALUES (@var_id, %(VAR_STRING)s, %(CHROM)s, %(POS)s, %(REF)s, %(ALT)s, %(GENE)s,"
                                "%(ACMG)s, %(FEATURE_ID)s, %(EFFECT)s, %(HGVS_C)s, %(HGVS_P)s, %(ClinVar)s, "
                                " %(ClinVarCONF)s, %(Varsome_link)s, %(Franklin_link)s)"
                                )
                        cursor.execute(query_variant, query_data_variant)

                        query_instance = (
                                "INSERT INTO instance(variant_id, sample_id) VALUES (@var_id, @sam_id)"
                                )
                        cursor.execute(query_instance, query_data_instance)
                    
                    else:
                        query_instance = (
                                "INSERT INTO instance(variant_id, sample_id) VALUES (%s, @sam_id)"
                                )
                        cursor.execute(query_instance, [vs_res][0])


            
                    connection.commit()
            cursor.close()

    except Error as e:
        print("Error connecting to database,", e)

    finally:
        if connection:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("Connection closed")

main()