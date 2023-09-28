import mysql.connector
from mysql.connector import Error

connection = None

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

            query = "SELECT * FROM gen_info"
            cursor.execute(query)

            for row in cursor.fetchall():
                print(row)

            cursor.close()




except Error as e:
    print("error connecting to database", e)
finally:
    if connection:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("connection terminated")