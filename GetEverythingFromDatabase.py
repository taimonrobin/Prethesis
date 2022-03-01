import mysql.connector
import csv
import sys
from pathlib import Path


db = mysql.connector.connect(
  host="sindabusdb2.cjibhismj6nf.eu-central-1.rds.amazonaws.com",
  user="knx_log_reader",
  password="IL4JFWLu6l13f9YSQrZliSKvf",
  database="knx_log"
)

db.autocommit = True
cur = db.cursor()


query_and_names = [(11, 2018), (12, 2018),
                   (1, 2019), (2, 2019), (3, 2019), (4, 2019), (5, 2019), (6, 2019), (7, 2019), (8, 2019), (9, 2019), (10, 2019), (11, 2019), (12, 2019),
                   (1, 2020), (2, 2020), (3, 2020), (4, 2020), (5, 2020), (6, 2020), (7, 2020), (8, 2020), (9, 2020), (10, 2020), (11, 2020), (12, 2020),
                   (1, 2021), (2, 2021), (3, 2021), (4, 2021), (5, 2021), (6, 2021), (7, 2021), (8, 2021), (9, 2021), (10, 2021), (11, 2021), (12, 2021),]



for x in query_and_names:
    csv_file_path = ''
    month = str(x[0])
    year = str(x[1])
    if(x[0] < 10):
        csv_file_path = year + ' ' + '0' + month + '.csv'
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