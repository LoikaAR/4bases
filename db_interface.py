import mysql.connector
from mysql.connector import Error

connection = None # prepare connection

db_config = {
    'host': 'localhost',
    'database': '4evar_test',
    'user': 'root',
    'password': 'PassMass123!'
}

# finds all variants present in a given sample
# return: list of variant id, variant string
def variants_in_sample(sample_file_name):
    sample_file_name = [sample_file_name] # conversion to iterable needed for mysql connector
    res = []
    try:
        connection = mysql.connector.connect(**db_config)
        if connection and connection.is_connected():
            cursor = connection.cursor(buffered=True)
            cursor.execute("select database();")
            
            query = ("SELECT v.variant_id, v.VAR_STRING FROM variant v "
                     "JOIN instance i ON i.variant_id = v.variant_id "
                     "JOIN sample s ON i.sample_id = s.sample_id "
                     "WHERE s.file_name = %s;")

            cursor.execute(query, sample_file_name)
            records = cursor.fetchall()

            for r in records:
                res.append({"ID": r[0], "var_string": r[1]})
            

    except Error as e:
        print("Error connecting to database,", e)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

            print("Variants present in sample", sample_file_name[0], ":")
            for elem in res:
                print("ID:",elem["ID"])
                print("Variant String:", elem["var_string"])
                print("")
            print("Total retrieved count:", len(records))
            print("Connection closed")
            print("===================================================================")
            return res
            

# finds all samples where the given variant is found
# return: list of file name, sample_id
def samples_containing_variant(variant_string):
    variant_string = [variant_string]
    res = []
    try:
        connection = mysql.connector.connect(**db_config)

        if connection and connection.is_connected():
            cursor = connection.cursor(buffered=True)
            cursor.execute("select database();")

            query = ("SELECT s.sample_id, s.file_name FROM sample s "
                     "JOIN instance i ON s.sample_id = i.sample_id "
                     "JOIN variant v ON v.variant_id = i.variant_id "
                     "WHERE v.VAR_STRING = %s;")
            cursor.execute(query, variant_string)
            records = cursor.fetchall()
            


            for r in records:
                res.append({"ID": r[0], "file_name": r[1]})

    except Error as e:
        print("Error connecting to database,", e)

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("Samples containing variant", variant_string[0] + ":")
            for elem in res:
                print("ID:",elem["ID"])
                print("File name:", elem["file_name"])
                print("")
            print("Total retrieved count:", len(records))
            print("Connection closed")
            print("===================================================================")
            return res

def main():
    sample_file = "HEVA_Nanopore_HD794_ann"
    variants_in_sample(sample_file)


    variant_string = "chr1018816633CT"
    samples_containing_variant(variant_string)

main()