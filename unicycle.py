from collections import namedtuple
import pandas as pd
import requests
import os
import io
import sys
import zipfile

# Some static metadata about the systems
SystemMetadata = namedtuple("SystemMetadata", ['uza', 's3_bucket', 'file_format'])
SYSTEM_METADATA = {
    "baywheels": SystemMetadata("San Francisco Bay Area", "https://s3.amazonaws.com/baywheels-data/", "{0}{1}-baywheels-tripdata.csv.zip"),
    "nyc_citibike": SystemMetadata("New York City", "https://s3.amazonaws.com/tripdata/", "{0}{1}-citibike-tripdata.csv.zip"),
    "jc_citibike": SystemMetadata("Jersey City", "https://s3.amazonaws.com/tripdata/", "JC-{0}{1}-citibike-tripdata.csv.zip"),
    # Note, divvy data actually is a CSV file once unzipped, it's just not in the filename.
    "divvy": SystemMetadata("Chicago", "https://divvy-tripdata.s3.amazonaws.com/", "{0}{1}-divvy-tripdata.zip"), 
}

""" Downloads a fresh copy of the given national transit database, then returns the NTD as a pandas dataframe."""
def LoadNationalTransitDatabase(ntd_url):
    r = requests.get(ntd_url)
    output = io.BytesIO()
    output.write(r.content)
    return pd.read_excel(output, engine="openpyxl", sheet_name="UPT")

""" Download new data as appropriate, shortcircuiting if the file already exists locally. """
def UpdateLyftSystemDataCache(system_name, year, month):
    system_metadata = SYSTEM_METADATA[system_name]
    filename = system_metadata.file_format.format(str(year), str(month).zfill(2))
    key = os.path.join(system_metadata.s3_bucket, filename)

    # TERRIBLE KLUDGE HACK: Baywheels renamed from fordgobike in 2019-05, gross.
    if system_name == "baywheels" and (year <= 2018 or (year <= 2019 and month <= 4)):
        kludge_filename = "{0}{1}-fordgobike-tripdata.csv.zip".format(str(year), str(month).zfill(2))
        key = os.path.join(system_metadata.s3_bucket, kludge_filename)


    # If the file does not already exist, we want to download it.
    file_location = os.path.join("data_cache", system_name, filename)
    if not os.path.exists(file_location):
        print("Downloading {key}.")
        r = requests.get(key)
        if r.status_code != 200:
            print("Error: could not retrieve key {key}.")
            return -1
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = zf.namelist()[0] # Naively assumes the zip file contains exactly one csv file; good enough.
        csv = zf.open(csv_name)
        f = open(file_location, "wb")
        f.write(csv.read())
        f.close()
        return 0

    return 0



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("This script is intended to be called with one argument: the path to the latest NTD download URL.")
        print("For example, try $ python3 unicycle.py https://www.transit.dot.gov/sites/fta.dot.gov/files/2021-10/August%202021%20Raw%20Database.xlsx")
    NTD_URL = sys.argv[1]
    #ntd_df = LoadNationalTransitDatabase(NTD_URL)
    #print(ntd_df)
    success = UpdateLyftSystemDataCache("baywheels", 2019, 5)
        

