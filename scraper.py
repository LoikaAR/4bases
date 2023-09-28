import os
from openpyxl import load_workbook

path = "./esempio_dati"
dir_list = list(os.listdir(path))

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

            # print the names of the columns and their corresponding values  
            for i in range (1, len(all_rows)):
                print(f"row {i}:")
                j = 0
                for cell in all_rows[i]:
                    print(f"{all_rows[0][j].value}: {cell.value}")
                    j += 1
                print('\n')
