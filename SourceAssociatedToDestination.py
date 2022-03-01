import pandas as pd
from os import listdir
from os.path import isfile, join
import glob

onlyfiles = [f for f in listdir("dataset/unaltered dataset/") if isfile(join("dataset/unaltered dataset/", f))]

# All files ending with .csv
files = glob.glob("dataset/unaltered dataset/20*.csv")
print(files)

new_files = []
for file in files:
    file = file[file.rfind("\\") + 1 :]
    file = "dataset/unaltered dataset/" + file
    new_files.append(file)

# Get list of unique Source Address
source_addresses = []
for file in new_files:
    df = pd.read_csv(file)
    x = df['source_addr'].unique().tolist()
    source_addresses.extend(x)

uniqie_source_addresses = set(source_addresses)

uniqie_source_addresses = [x for x in uniqie_source_addresses if str(x) != 'nan']

new_df = pd.DataFrame([], columns = ["source_addr", "destination_addr"])
i = 0
#Get the column names, group and save files
for source_address in uniqie_source_addresses:
    print("Working with source address: " + source_address)
    all_destination_address_list = []
    for file in new_files:
        df = pd.read_csv(file)
        df = df.loc[df['source_addr'] == source_address]
        df = df[["source_addr", "destination_addr"]]

        destination_addr_list = df.destination_addr.tolist()
        destination_addr_list = [addr.replace("/", "-") for addr in destination_addr_list]
        all_destination_address_list += destination_addr_list
        
    all_destination_address_list = set(all_destination_address_list)
    destination_addresses = ",".join(all_destination_address_list)

    data = pd.DataFrame({"source_addr": source_address, "destination_addr": destination_addresses}, index = [i])
    i = i + 1
    print("Found associated destination address: " + destination_addresses)
    new_df = new_df.append(data)

new_df.to_csv("dataset/source to destination/source_to_destination.csv", encoding='utf-8')
print("Program Completed")
