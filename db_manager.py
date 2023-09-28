import os
from openpyxl import load_workbook
import mysql.connector
from mysql.connector import Error

connection = None
path = "./esempio_dati"
dir_list = list(os.listdir(path))

try: 
    connection = mysql.connector.connect(host='localhost',
                         database='4evar_test',
                         user='arseni',
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
                        
                        # Load the entire workbook
                        wb = load_workbook(data_file)

                        # Load the worksheet
                        ws = wb['Sheet1']
                        all_rows = list(ws.rows)
                        all_cols = list(ws.columns)

                        print(f"found {len(all_rows)} of variants in this file")

                        for i in range (1, len(all_rows)):
                            print(f"row {i}:")
                            j = 0
                            for cell in all_rows[i]:
                                current = all_rows[0][j].value
                                print(f"{current}: {cell.value}")
                                # TODO
                                # insert data into the current entry
                                # either: populate an array of values or
                                # create a gene_info class 

                                query = f"INSERT INTO gen_info ({current})"
                                cursor.execute(query)

                                cursor.close()
                                j += 1
                            print('\n')
                            

except Error as e:
    print("error connecting to database", e)
finally:
    if connection:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("connection terminated")