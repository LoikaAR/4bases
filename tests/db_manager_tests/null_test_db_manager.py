import os
import sys
import mysql.connector
from dataclasses import dataclass
from openpyxl import load_workbook
from mysql.connector import Error

path = str(sys.argv[1])             # the location of the files (folder)
dir_list = list(os.listdir(path))   # list of all files in the target folder
connection = None                   # prepare the connection
DP_THRESHOLD = 20                   # depth threshold
VAR_STRINGS = []                    # vector of variant strings


try:
    connection = mysql.connector.connect(host='localhost',
                                         database='null_testing',
                                         user='root',
                                         password='PassMass123!')

    if connection and connection.is_connected():
        db_info = connection.get_server_info()
        print("connected to server ", db_info)

        cursor = connection.cursor(buffered=True) # buffered prevents unread result error 
        cursor.execute("select database();")

        record = cursor.fetchone()
        print("connected to database ", record)

        id_counter = 0
        for dir in dir_list:
            new_p = path + '/' + dir
            folder = list(os.listdir(new_p))
            
            # get the excel file
            target = [i for i in folder if ".xlsx" in i]
            file = target[0]

            data_file = path + '/' + dir + '/' + file
            print("current file:", data_file)
            
            wb = load_workbook(data_file) # Load the entire workbook
            ws = wb['Sheet1']             # Load the worksheet

            rows = list(ws.rows)
            cols = list(ws.columns)

            DP_missing = True
            VAF_missing = True

            for i in range(len(rows[0])):
                if rows[0][i].value == "DP":
                    DP_missing = False
                if rows[0][i].value == "VAF":
                    VAF_missing = False

            # remove whitespaces so col names work with MySQL syntax
            for i in range(len(rows[0])):
                if rows[0][i].value.find(" ") != -1:
                    rows[0][i].value = "_".join(rows[0][i].value.split())
                    
            for i in range (1, len(rows)):
                query_data_vaf = {
                    "vaf_id": None,
                    "val": None,
                    "row_num": None,
                    "col_num": None,
                    "is_null": None,
                    "file_name": file
                }
                
                query_data_gt = {
                    "gt_id": None,
                    "val": None,
                    "row_num": None,
                    "col_num": None,
                    "is_null": None,
                    "file_name": file
                }

                query_data_dp = {
                    "dp_id": None,
                    "val": None,
                    "row_num": None,
                    "col_num": None,
                    "is_null": None,
                    "file_name": file
                }

                query_data_sample = {
                    "sample_id": None,
                    "VAF": None,
                    "GT": None,
                    "DP": None
                }

                j = 0
                variant_string = ""
                for cell in rows[i]:
                    current = rows[0][j].value # the column name

                    if cell.value == '.': # assuming "." is equal to NULL
                        cell.value = None

                    if current == "VAF":
                        query_data_vaf["row_num"] = i
                        query_data_vaf["col_num"] = j
                        query_data_vaf["is_null"] = cell.value == None
                        query_data_vaf["val"] = cell.value
                    elif current == "DP":
                        query_data_dp["col_num"] = j
                        query_data_dp["row_num"] = i
                        query_data_dp["is_null"] = cell.value == None
                        query_data_dp["val"] = cell.value
                    elif current == "GT":
                        query_data_gt["row_num"] = i
                        query_data_gt["col_num"] = j
                        query_data_gt["is_null"] = cell.value == None
                        query_data_gt["val"] = cell.value
                    j += 1
                
                query_data_sample["VAF"] = id_counter
                query_data_vaf["vaf_id"] = id_counter
                query_data_sample["GT"] = id_counter
                query_data_gt["gt_id"] = id_counter
                query_data_sample["DP"] = id_counter
                query_data_dp["dp_id"] = id_counter

                id_counter += 1

                cursor.execute("SET @sam_id = uuid()")
                print("VAF:", query_data_vaf)
                print("GT:", query_data_gt)
                print("DP:", query_data_dp)
                print("DP missing?", DP_missing)
                print("VAF missing?", VAF_missing)

                # some files do not have VAF or DP columns. In that case the values will be 99999
                if VAF_missing:
                    query_vaf = ("INSERT INTO VAF "
                                "(vaf_id, val, row_num, col_num, is_null, file_name) " 
                                "VALUES (%(vaf_id)s, 9.999999, 99999, 99999, True, %(file_name)s)")                
                else:
                    query_vaf = ("INSERT INTO VAF "
                                "(vaf_id, val, row_num, col_num, is_null, file_name) "
                                "VALUES (%(vaf_id)s, %(val)s, %(row_num)s, %(col_num)s, %(is_null)s, %(file_name)s)")
                cursor.execute(query_vaf, query_data_vaf)

                query_gt = ("INSERT INTO GT "
                                "(gt_id, val, row_num, col_num, is_null, file_name) "
                                "VALUES (%(gt_id)s, %(val)s, %(row_num)s, %(col_num)s, %(is_null)s, %(file_name)s)")
                cursor.execute(query_gt, query_data_gt)

                if DP_missing:
                    query_dp = ("INSERT INTO DP "
                                    "(dp_id, val, row_num, col_num, is_null, file_name) VALUES "
                                    "(%(dp_id)s, 99999, 99999, 99999, True, %(file_name)s)")
                else:
                    query_dp = ("INSERT INTO DP "
                                    "(dp_id, val, row_num, col_num, is_null, file_name) "
                                    "VALUES (%(dp_id)s, %(val)s, %(row_num)s, %(col_num)s, %(is_null)s, %(file_name)s)")

                cursor.execute(query_dp, query_data_dp)
                query_sample = ("INSERT INTO sample "
                                "(sample_id, VAF, GT, DP) VALUES (@sam_id, %(VAF)s, %(GT)s, %(DP)s)")
                cursor.execute(query_sample, query_data_sample)
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