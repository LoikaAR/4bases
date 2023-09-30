import os
from openpyxl import load_workbook
import mysql.connector
from mysql.connector import Error

connection = None
path = "../esempio_dati"
dir_list = list(os.listdir(path))
idx = 0 # temporary index as primary key for the db
variant_map = {}

try: 
    connection = mysql.connector.connect(host='localhost',
                         database='4evar_test',
                         user='root',
                         password='PassMass123!')

    if connection:
        if connection.is_connected():
            
            db_Info = connection.get_server_info()
            print("connected to server ", db_Info)

            cursor = connection.cursor()
            cursor.execute("select database();")

            record = cursor.fetchone()
            print("connected to database ", record)
            for dir in dir_list:
                new_p = path + '/' + dir
                folder = os.listdir(new_p)
                for file in folder:
                    if file.endswith(".xlsx"):

                        data_file = path + '/' + dir +'/' + file
                        print("current file:", data_file, '\n')
                        
                        # Load the entire workbook
                        wb = load_workbook(data_file)

                        # Load the worksheet
                        ws = wb['Sheet1']
                        all_rows = list(ws.rows)
                        all_cols = list(ws.columns)
                        print(f"found {len(all_rows)} variants in this file")

                        # correct local spelling to work with SQL syntax
                        for i in range(len(all_rows[0])):
                            if all_rows[0][i].value == "FEATURE ID":
                                all_rows[0][i].value = "FEATURE_ID"
                            elif all_rows[0][i].value == "Varsome link":
                                all_rows[0][i].value = "Varsome_link"
                            elif all_rows[0][i].value == "Franklin link":
                                all_rows[0][i].value = "Franklin_link"

                        for i in range (1, len(all_rows)):
                            # prepare query data for db
                            query_data = {
                                "variant_id": idx,
                                "CHROM": None,
                                "POS": None,
                                "REF": None,
                                "ALT": None,
                                "VAF": None,
                                "GT": None,
                                "DP": None,
                                "GENE": None,
                                "FEATURE_ID": None,
                                "EFFECT": None,
                                "HGVS_C": None,
                                "HGVS_P": None,
                                "ClinVar": None,
                                "ClinVarCONF": None,
                                "Varsome_link": None,
                                "Franklin_link": None
                            }

                            print(f"row {i}:")
                            j = 0
                            variant_string = ""
                            for cell in all_rows[i]:
                                current = all_rows[0][j].value

                                # ASSUMPTION: all empty cells and those with '.' char imply NULL
                                if cell.value == '.':
                                    cell.value = None
                                # print(f"{current}: {cell.value}, type {type(cell.value)}")
                                query_data[current] = cell.value
                                j += 1
                                if (cell.value == None):
                                    variant_string += "."
                                else:
                                    variant_string += str(cell.value)

                            # print(f"DATA FOR DB: {query_data}, total elements: {len(query_data)}")
                            
                            # TODO look into concatenating the variant string and making a dictionary of them
                            # print(f"variant string: {variant_string}")
                            # if variant_string not in variant_map:
                            #     variant_map[variant_string] = 0
                            # else: variant_map[variant_string] += 1

                            query_data["variant_id"] = idx
                            idx += 1

                            query = ("INSERT INTO gen_info "
                                    "(variant_id, CHROM, POS, REF, ALT, VAF, GT, DP, GENE, FEATURE_ID, "
                                    "EFFECT, HGVS_C, HGVS_P, ClinVar, ClinVarCONF, Varsome_link, Franklin_link) "
                                    "VALUES (%(variant_id)s, %(CHROM)s, %(POS)s, %(REF)s, %(ALT)s, %(VAF)s, %(GT)s,"
                                    " %(DP)s, %(GENE)s, %(FEATURE_ID)s, %(EFFECT)s, %(HGVS_C)s, %(HGVS_P)s, %(ClinVar)s,"
                                    " %(ClinVarCONF)s, %(Varsome_link)s, %(Franklin_link)s)") 
                            
                            cursor.execute(query, query_data)
                            connection.commit()
                            print('\n')
            print(variant_map)
            cursor.close()

except Error as e:
    print("error connecting to database:", e)

finally:
    if connection:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("connection terminated")