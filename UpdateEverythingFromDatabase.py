import sqlite3
from os import listdir
from os.path import isfile, join
import glob
from datetime import datetime
import pandas as pd
import mysql.connector
from pathlib import Path
import csv



connection = sqlite3.connect("db.sqlite3")
cursor = connection.cursor()



# All files ending with .csv
files = glob.glob("Dataset/unaltered dataset/20*.csv")

file = files[-1]

name_of_stored_file = file[file.rfind("/") + 1 :]
date_of_stored_file = name_of_stored_file.split(".")[0].split(" ")
print(date_of_stored_file)

date_time_str = date_of_stored_file[0] + "-" + date_of_stored_file[1] + "-01"
print('Date:', date_time_str)

query_and_names = pd.date_range(date_time_str,str(datetime.now().date()), freq='MS').strftime("%m-%Y").tolist()

new_query_and_names = []

for i in query_and_names:
    x = i.split("-")
    y = (x[0], x[1])
    new_query_and_names.append(y)

query_and_names = new_query_and_names

db = mysql.connector.connect(
  host="sindabusdb2.cjibhismj6nf.eu-central-1.rds.amazonaws.com",
  user="knx_log_reader",
  password="IL4JFWLu6l13f9YSQrZliSKvf",
  database="knx_log"
)

db.autocommit = True
cur = db.cursor()


print(query_and_names)

for x in query_and_names:
    csv_file_path = ''
    month = str(x[0])
    year = str(x[1])
    print(month)
    print(year)
    if(int(x[0]) < 10):
        csv_file_path = year + ' ' + month + '.csv'
    else:
        csv_file_path = year + ' ' + month + '.csv'

    csv_file_path = "Dataset/unaltered dataset/" + csv_file_path

    sql = "SELECT * from log_kzh where year(timestamp) = " + year + " and month(timestamp) = " + month

    print(sql)


    cur.execute(sql)
    rows = cur.fetchall()

    # finally:
    #     db.close()

    # Continue only if there are rows returned.
    if rows:
        # New empty list called 'result'. This will be written to a file.
        result = list()

        # The row name is the first entry for each entity in the description tuple.
        column_names = list()
        for i in cur.description:
            column_names.append(i[0])

        result.append(column_names)
        for row in rows:
            result.append(row)

        # Check if path and file exist

        path = Path(csv_file_path)

        if not path.is_file():
            f = open(csv_file_path, "x")

        # Write result to file.
        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in result:
                csvwriter.writerow(row)
    else:
        print("No rows found for query: {}".format(sql))

print("The program has been executed completely")