import pandas as pd
from os import listdir
from os.path import isfile, join
import glob

onlyfiles = [f for f in listdir("dataset/") if isfile(join("dataset/", f))]

# All files ending with .csv
files = glob.glob("dataset/20*.csv")
print(files)

new_files = []
for file in files:
    file = file[file.rfind("\\") + 1 :]
    file = "dataset/" + file
    new_files.append(file)

# Get list of unique Destination Address
destination_addresses = []
for file in new_files:
    df = pd.read_csv(file)
    x = df['destination_addr'].unique().tolist()
    destination_addresses.extend(x)

unique_destination_addresses = set(destination_addresses)
unique_destination_addresses = [x for x in unique_destination_addresses if str(x) != 'nan']

#Get the column names, group and save files
for destination_addr in unique_destination_addresses:
    new_df = pd.DataFrame([], columns = ['datetime', 'destination_addr'])

    for file in new_files:
        df = pd.read_csv(file)
        df = df.loc[df['destination_addr'] == destination_addr]

        df = df[["timestamp", "destination_addr"]]
        df["destination_addr"] = 1

        new_df = pd.concat([new_df, df])


    new_df["datetime"] = pd.to_datetime(new_df['timestamp'], format='%Y-%m-%d').copy()

    new_df = new_df.drop('timestamp', 1)

    new_df = new_df.resample('H', on='datetime').destination_addr.sum()

    destination_addr = destination_addr.replace("/", "-")

    new_df.to_csv("dataset/destination addresses/" + destination_addr + ".csv", encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
    print(destination_addr)

print("Program Completed")






