import os
import sys
import mysql.connector
from openpyxl import load_workbook
from mysql.connector import Error

path = str(sys.argv[1])             # the location of the files (folder)
dir_list = list(os.listdir(path))   # list of all files in the target folder
connection = None                   # prepare the connection
NULL_MATCHES = []                    # vector of variant strings

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

        record = cursor.fetchone()[0]
        print("connected to database", record)

        id_counter = 0
        for dir in dir_list:
            new_p = path + '/' + dir
            folder = list(os.listdir(new_p))
            
            # get the excel file
            target = [i for i in folder if ".xlsx" in i]
            file = target[0]

            data_file = path + dir + '/' + file
            print("current file:", data_file)
            
            wb = load_workbook(data_file) # Load the entire workbook
            ws = wb['Sheet1']             # Load the worksheet

            rows = list(ws.rows)
            cols = list(ws.columns)
            # print(f"found {len(rows)} variants in this file\n")

            # remove whitespaces so col names work with MySQL syntax
            for i in range(len(rows[0])):
                if rows[0][i].value.find(" ") != -1:
                    rows[0][i].value = "_".join(rows[0][i].value.split())
                    
            for i in range (1, len(rows)):
                j = 0
                for cell in rows[i]:
                    current = rows[0][j].value # the column name
                    if current == "VAF":
                        if cell.value == '.' or cell.value == None:
                            res = {"row": i, "col": j, "file":file}
                            query = ("SELECT row_num, col_num, file_name FROM VAF WHERE row_num = %(row)s AND col_num = %(col)s AND file_name = %(file)s")
                            cursor.execute(query, res)
                            record = cursor.fetchall()
                            if i == record[0][0] and j == record[0][1] and file == record[0][2]:
                                NULL_MATCHES.append(True)
                            else:
                                NULL_MATCHES.append(False)
                    elif current == "GT":
                        if cell.value == '.' or cell.value == None:
                            res = {"row": i, "col": j, "file":file}
                            query = ("SELECT row_num, col_num, file_name FROM GT WHERE row_num = %(row)s AND col_num = %(col)s AND file_name = %(file)s")
                            cursor.execute(query, res)
                            record = cursor.fetchall()
                            if i == record[0][0] and j == record[0][1] and file == record[0][2]:
                                NULL_MATCHES.append(True)
                            else:
                                NULL_MATCHES.append(False)
                    elif current == "DP":
                        if cell.value == '.' or cell.value == None or cell.value == "" or cell.value == " ":
                            res = {"row": i, "col": j, "file":file}
                            query = ("SELECT row_num, col_num, file_name FROM DP WHERE row_num = %(row)s AND col_num = %(col)s AND file_name = %(file)s")
                            cursor.execute(query, res)
                            record = cursor.fetchall()
                            if i == record[0][0] and j == record[0][1] and file == record[0][2]:
                                NULL_MATCHES.append(True)
                            else:
                                NULL_MATCHES.append(False)
                    j += 1

        fail = False
        for b in NULL_MATCHES:
            if b == False:
                print("TESTS FAILED")
                fail = True
                break
        if fail == False:
            print("TESTS PASSED")
            
        cursor.close()

except Error as e:
    print("Error connecting to database,", e)

finally:
    if connection:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Connection closed")