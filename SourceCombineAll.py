import pandas as pd
from os import listdir
from os.path import isfile, join
import glob

onlyfiles = [f for f in listdir("Dataset/unaltered dataset/") if isfile(join("Dataset/unaltered dataset/", f))]

# All files ending with .csv
files = glob.glob("Dataset/unaltered dataset/20*.csv")

new_files = []
for file in files:
    file = file[file.rfind("\\") + 1 :]
    file = file
    new_files.append(file)

print(new_files)
# Get list of unique Destination Address
source_addresses = []
for file in new_files:
    print(file)
    df = pd.read_csv(file)
    x = df['source_addr'].unique().tolist()
    source_addresses.extend(x)

unique_source_addresses = set(source_addresses)
unique_source_addresses = [x for x in unique_source_addresses if str(x) != 'nan']

#Get the column names, group and save files
for source_addr in unique_source_addresses:
    new_df = pd.DataFrame([], columns = ['datetime', 'source_addr'])

    for file in new_files:
        df = pd.read_csv(file)
        df = df.loc[df['source_addr'] == source_addr]

        df = df[["timestamp", "source_addr"]]
        df["source_addr"] = 1

        new_df = pd.concat([new_df, df])


    new_df["datetime"] = pd.to_datetime(new_df['timestamp'], format='%Y-%m-%d').copy()

    new_df = new_df.drop('timestamp', 1)

    new_df = new_df.resample('H', on='datetime').source_addr.sum()

    source_addr = source_addr.replace("/", "-")

    new_df.to_csv("Dataset/source addresses/updated source address/" + source_addr + ".csv", encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
    print(source_addr)

print("Program Completed")






