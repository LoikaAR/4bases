import os
from openpyxl import load_workbook
import mysql.connector
from mysql.connector import Error

path = "../esempio_dati" # this should be a parameter
dir_list = list(os.listdir(path))
connection = None

try: 
    connection = mysql.connector.connect(host='localhost',
                         database='4evar_test',
                         user='root',
                         password='PassMass123!') # user and password should be parameters

    if connection and connection.is_connected():
        db_info = connection.get_server_info()
        print("connected to server ", db_info)

        cursor = connection.cursor()
        cursor.execute("select database();")

        record = cursor.fetchone()
        print("connected to database ", record)

        for dir in dir_list:
            new_p = path + '/' + dir
            folder = os.listdir(new_p)
            for file in folder:
                if file.endswith(".xlsx"):
                    
        # ---------- POSSIBLY EXTRACT THIS AS A SEPARATE FUNCTION ----------
                    data_file = path + '/' + dir + '/' + file
                    print("current file:", data_file)
                    
                    wb = load_workbook(data_file) # Load the entire workbook
                    ws = wb['Sheet1'] # Load the worksheet.

                    all_rows = list(ws.rows)
                    all_cols = list(ws.columns)
                    # print(f"found {len(all_rows)} variants in this file\n")

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
                            "variant_id": None,
                            "VAR_STRING": None,
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

                        j = 0
                        variant_string = ""
                        for cell in all_rows[i]:
                            current = all_rows[0][j].value # the column name

                            if cell.value == '.':
                                cell.value = None

                            # print(f"{current}: {cell.value}}")
                            query_data[current] = cell.value
                            j += 1

                            if (current == "CHROM" or current == "REF"):
                                variant_string += cell.value
                            elif (current == "POS" or current == "ALT"):
                                variant_string += str(cell.value)

                        query_data["VAR_STRING"] = variant_string

                        query = ("INSERT INTO gen_info "
                                "(variant_id, VAR_STRING, CHROM, POS, REF, ALT, VAF, GT, DP, GENE, FEATURE_ID, "
                                "EFFECT, HGVS_C, HGVS_P, ClinVar, ClinVarCONF, Varsome_link, Franklin_link) "
                                "VALUES (uuid(), %(VAR_STRING)s, %(CHROM)s, %(POS)s, %(REF)s, %(ALT)s, %(VAF)s, %(GT)s,"
                                " %(DP)s, %(GENE)s, %(FEATURE_ID)s, %(EFFECT)s, %(HGVS_C)s, %(HGVS_P)s, %(ClinVar)s,"
                                " %(ClinVarCONF)s, %(Varsome_link)s, %(Franklin_link)s)") 
                        
                        cursor.execute(query, query_data)
                        connection.commit()
        #  ------------------------------------------------------------               
        cursor.close()

except Error as e:
    print("Error connecting to database |", e)

finally:
    if connection:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Connection terminated")