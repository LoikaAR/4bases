import json
import csv
import pandas as pd
import mysql.connector
from mysql.connector import Error
from openpyxl import load_workbook

connection = None       # prepare connection
DP_THRESHOLD = 20       # depth threshold

db_config = {
    'host': 'localhost',
    'database': '4evar_test',
    'user': 'root',
    'password': 'PassMass123!'
}

def test_variants():
    try:
        connection = mysql.connector.connect(**db_config)

        if connection and connection.is_connected():
            db_info = connection.get_server_info()
            cursor = connection.cursor()
            cursor.execute('SELECT DATABASE();')
            db_name = cursor.fetchone()[0]
            
            file = open('./new_variants.json', 'r')
            data = json.load(file)
            cardinal_fail = False


            if (len(data) % 2 != 0):
                print('incorrect cardinality')
                cardinal_fail = True


            count_fail = False
            relation_fail = False
            var_idx = 0
            # iterate over every retrieved variant
            while var_idx < len(data)-1:
                sam_idx = var_idx + 1
                # print('var id:',data[var_idx].get('variant_id'))
                # print('sam id:',data[sam_idx].get('sample_id'))
                count_query = ('SELECT COUNT(*) FROM variant WHERE VAR_STRING = %s') # should be 1
                cursor.execute(count_query, [data[var_idx].get('VAR_STRING')])
                count_res = cursor.fetchone()[0]
                if (count_res > 1 or count_res < 1):
                    print(f"redundant variant with var string {data[var_idx].get('VAR_STRING')} was added")
                    count_fail = True

                relation_query = ('SELECT sample_id FROM instance WHERE variant_id = %s')
                cursor.execute(relation_query, [data[var_idx].get('variant_id')])
                rel_res = cursor.fetchall() # should not return more than 1 result

                if (len(rel_res) > 1):
                    relation_fail = True

                rel_match = data[sam_idx].get('sample_id') == rel_res[0][0]

                if (not rel_match):
                    print(f"variant {data[var_idx].get('variant_id')} is not connected to sample {data[sam_idx].get('sample_id')}")
                    relation_fail = True

                var_idx += 2

            if (relation_fail or count_fail or cardinal_fail):
                print("TESTS FAILED")
            else:
                print("TESTS PASSED")

    except Error as e:
        print('Error connecting to database,', e)

    finally:
        if connection:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("Connection closed")


def test_samples():
    try:
        connection = mysql.connector.connect(**db_config)

        if connection and connection.is_connected():
            db_info = connection.get_server_info()
            cursor = connection.cursor()
            cursor.execute('SELECT DATABASE();')
            db_name = cursor.fetchone()[0]
            
            file = open('./new_samples.json', 'r')
            data = json.load(file)
            cardinal_fail = False

            if (len(data) % 2 != 0):
                print('incorrect cardinality')
                cardinal_fail = True

            relation_fail = False
            sam_idx = 0
            # iterate over every retrieved variant
            while sam_idx < len(data)-1:
                var_idx = sam_idx+1

                relation_query = ('SELECT variant_id FROM instance WHERE sample_id = %s')
                cursor.execute(relation_query, [data[sam_idx].get('sample_id')])
                rel_res = cursor.fetchall() # should not return more than 1 result

                if (len(rel_res) > 1):
                    relation_fail = True

                var_match_query = ('SELECT VAR_STRING FROM variant WHERE variant_id = %s')
                cursor.execute(var_match_query, rel_res[0])
                var_res = cursor.fetchone()[0]

                rel_match = data[var_idx].get('var_string') == var_res

                if (not rel_match):
                    print(f"sample {data[sam_idx].get('sample_id')} is not connected to variant {data[var_idx].get('var_string')}")
                    relation_fail = True
                sam_idx += 2

            if (relation_fail or cardinal_fail):
                print("TESTS FAILED")
            else:
                print("TESTS PASSED")

    except Error as e:
        print('Error connecting to database,', e)

    finally:
        if connection:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("Connection closed")

def test_existing():
    try:
        connection = mysql.connector.connect(**db_config)

        if connection and connection.is_connected():
            db_info = connection.get_server_info()
            cursor = connection.cursor()
            cursor.execute('SELECT DATABASE();')
            db_name = cursor.fetchone()[0]

        existing = []
        count_fail = False
        with open('../../out_files/existing.tsv') as file:
            target = csv.reader(file, delimiter="\t")
                
            for line in target:
                existing.append(line)
        
        for idx in range(1, len(existing)):
            var_string = "".join(str(i) for i in existing[idx])

            query = ('SELECT COUNT(*) FROM variant WHERE VAR_STRING = %s')
            cursor.execute(query, [var_string])

            res = cursor.fetchone()[0]

            if (res != 1):
                count_fail = True
                print(var_string, "appears more than one in the database")

        if (count_fail):
            print('TESTS FAILED')
        else:
            print('TESTS PASSED')

    except Error as e:
        print('Error connecting to database,', e)

    finally:
        if connection:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("Connection closed")




def main():
    # test_variants()
    # test_samples()
    test_existing()

main()