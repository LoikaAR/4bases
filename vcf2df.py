import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from openpyxl import load_workbook

connection = None
file = "CARDIOPRO-CQ1-AA33.vcf"
path = '../esempio_dati/CARDIOPRO-CQ1-AA33/'+file

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='4evar_test',
                                         user='root',
                                         password='PassMass123!')
    
    if connection and connection.is_connected():
        db_info = connection.get_server_info()
        print("Connected to server ", db_info)
        cursor = connection.cursor()
        cursor.execute("SELECT DATABASE();")
        record = cursor.fetchone()
        
        with open(path, 'r') as file_:
            for i, line in enumerate(file_):
                if line.startswith('#CHROM'):
                    header = line.strip('#').strip().split('\t')
                    break


        data = pd.read_csv(path, sep ='\t', skiprows=i, header=None)
        data.columns = header

        target_data = data[['CHROM', 'POS', 'REF', 'ALT']]
        n_rows = target_data.shape[0]
        n_cols = target_data.shape[1]
        # print("ILOC:",target_data.iloc[0,3])

        # loop that extracts the variant strings
        for row in range(1, n_rows):
            extracted_info = {"CHROM": None, "REF": None, "POS": None, "ALT": None} # for lookup later
            query_info = {"VAR_STRING": ""}
            for col in range(0, n_cols):
                query_info['VAR_STRING'] += str(target_data.iloc[row, col])

                # separate pieces of data for lookup in excel files 
                if target_data.iloc[0,col] == "#CHROM":
                    extracted_info["CHROM"] = target_data.iloc[row, col]
                elif target_data.iloc[0,col] == "REF":
                    extracted_info["REF"] = target_data.iloc[row, col]
                elif target_data.iloc[0,col] == "POS":
                    extracted_info["POS"] = target_data.iloc[row, col]
                elif target_data.iloc[0,col] == "ALT":
                    extracted_info["ALT"] = target_data.iloc[row, col]

            query = "SELECT variant_id FROM gen_info WHERE VAR_STRING = 'chr116270000AG'" # possible flaw
            cursor.execute(query, query_info)
            res = cursor.fetchall()
            if res == []:
                print("Variant not found, inserting into db")
                # ASSUMPTION: the excel file is in the same folder as the .vcf file
                new_path = path.replace(file, '')
                folder = os.listdir(new_path)

                for elem in folder:
                    if elem.endswith(".xlsx"):
                        data_file = new_path + '/' + elem
                        print("current file:", data_file)

                        wb = load_workbook(data_file)
                        ws = wb['Sheet1']

                        all_rows = list(ws.rows)
                        all_cols = list(ws.columns)

                        for i in range(len(all_rows[0])):
                            if all_rows[0][i].value == "FEATURE ID":
                                all_rows[0][i].value = "FEATURE_ID"
                            elif all_rows[0][i].value == "Varsome link":
                                all_rows[0][i].value = "Varsome_link"
                            elif all_rows[0][i].value == "Franklin link":
                                all_rows[0][i].value = "Franklin_link"
                        # DOES NOT WORK
                        for i in range (1, len(all_rows)):
                            
                            chrom_val = all_rows[i][0].value
                            pos_val = all_rows[i][1].value
                            ref_val = all_rows[i][2].value
                            alt_val = all_rows[i][3].value

                            if chrom_val == extracted_info["CHROM"]      \
                                    and pos_val == extracted_info["POS"] \
                                    and ref_val == extracted_info["REF"] \
                                    and alt_val == extracted_info["ALT"]:
                                    
                                j = 0
                                for cell in all_rows[i]:
                                    current = all_rows[0][j].value # the column name

                                    if cell.value == '.':
                                        cell.value = None

                                    query_data[current] = cell.value
                                    j += 1

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

                                query_data["VAR_STRING"] = query_info["VAR_STRING"]

                                query = ("INSERT INTO gen_info "
                                        "(variant_id, VAR_STRING, CHROM, POS, REF, ALT, VAF, GT, DP, GENE, FEATURE_ID, "
                                        "EFFECT, HGVS_C, HGVS_P, ClinVar, ClinVarCONF, Varsome_link, Franklin_link) "
                                        "VALUES (uuid(), %(VAR_STRING)s, %(CHROM)s, %(POS)s, %(REF)s, %(ALT)s, %(VAF)s, %(GT)s,"
                                        " %(DP)s, %(GENE)s, %(FEATURE_ID)s, %(EFFECT)s, %(HGVS_C)s, %(HGVS_P)s, %(ClinVar)s,"
                                        " %(ClinVarCONF)s, %(Varsome_link)s, %(Franklin_link)s)") 
                                
                                cursor.execute(query, query_data)
                                connection.commit()
            else:
                print("FOUND")

except Error as e:
    print("Error connecting to database |", e)

finally:
    if connection:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Connection terminated")


H1 = "chr116272250AG"