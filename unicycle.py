""" Compare lyft bikeshare ridership against NTD data on a per-month basis since 2018-01. """

from collections import namedtuple
from collections import defaultdict

import pandas as pd
import datetime
import requests
import os
import io
import sys
import zipfile
import multiprocessing

# Some static metadata about the systems
SystemMetadata = namedtuple("SystemMetadata", ['uza', 's3_bucket', 'file_format'])
SYSTEM_METADATA = {
    "baywheels": SystemMetadata("San Francisco Bay Area, CA", "https://s3.amazonaws.com/baywheels-data/", "{0}{1}-baywheels-tripdata.csv.zip"),
    "nyc_citibike": SystemMetadata("New York City, NY", "https://s3.amazonaws.com/tripdata/", "{0}{1}-citibike-tripdata.csv.zip"),
    "jc_citibike": SystemMetadata("Jersey City, NJ", "https://s3.amazonaws.com/tripdata/", "JC-{0}{1}-citibike-tripdata.csv.zip"),
    # Note, divvy data actually is a CSV file once unzipped, it's just not in the filename.
    "divvy": SystemMetadata("Chicago, IL", "https://divvy-tripdata.s3.amazonaws.com/", "{0}{1}-divvy-tripdata.zip"), 
}

""" Downloads a fresh copy of the given national transit database, then returns the NTD as a pandas dataframe."""
def LoadNationalTransitDatabase(ntd_url):
    r = requests.get(ntd_url)
    output = io.BytesIO()
    output.write(r.content)
    df = pd.read_excel(output, engine="openpyxl", sheet_name="UPT")

    # Do some post-processing on the NTD excel sheet.
    # First off, drop data before 2018-01.
    old_data = df.columns[list(range(9, 201))]
    df = df.drop(old_data, axis=1)

    # Next, drop some fields we don't care about.
    df = df.drop(columns=["5 digit NTD ID", "4 digit NTD ID", "Active", "Reporter Type", "UZA"])

    # Next, strip out the meaningless summary data at the bottom of these sheets, which is generally identifable as data without an agency name.
    df = df[df["Agency"].notna()]

    # Next, group the data by Agency, UZA Name and sum unlinked passenger trips across transit modes.
    df = df.groupby(["Agency", "UZA Name"], as_index=False).sum()

    return df

""" Download new data as appropriate, shortcircuiting if the file already exists locally. """
def DownloadLyftSystemDataCache(system_name, year, month):
    system_metadata = SYSTEM_METADATA[system_name]
    filename = system_metadata.file_format.format(str(year), str(month).zfill(2))
    key = os.path.join(system_metadata.s3_bucket, filename)

    # TERRIBLE KLUDGE HACK: Baywheels renamed from fordgobike in 2019-05, gross.
    if system_name == "baywheels" and (year <= 2018 or (year <= 2019 and month <= 4)):
        kludge_filename = "{0}{1}-fordgobike-tripdata.csv.zip".format(str(year), str(month).zfill(2))
        key = os.path.join(system_metadata.s3_bucket, kludge_filename)
    
    # TODO: Another terrible kludge hack: divvy files between 2018 and 2021-04 actually have the file format Divvy_Trips_2018_Q1.zip
    # Additionally, it looks like they have the file format DivvyTrips_2017Q1Q2 for the years before 2018... what a mess.
    # We'll have to roll everything up to annual at some point...



    # If the file does not already exist, we want to download it.
    file_location = os.path.join("data_cache", system_name, filename)
    if not os.path.exists(file_location):
        print("Downloading {0}.".format(key))
        r = requests.get(key)
        if r.status_code != 200:
            print("Error: could not retrieve key {0}.".format(key))
            return -1
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = zf.namelist()[0] # Naively assumes the zip file contains exactly one csv file; good enough.
        csv = zf.open(csv_name)
        f = open(file_location, "wb")
        f.write(csv.read())
        f.close()
        return 0

    return 0

# In parallel, download all the files.
def UpdateLocalBikeshareCache(date_range):
    args = [(system_name, d.year, d.month) for d in date_range for system_name in SYSTEM_METADATA.keys()]
    with multiprocessing.Pool(16) as p:
        p.starmap(DownloadLyftSystemDataCache, args)



""" Counts number of lyft bike rides that occurred on the given system in the given month. """
def CountLyftRides(system_name, year, month):
    # There's one ride per row, so: stupid hack: count number of lines in the file, minus the header.
    system_metadata = SYSTEM_METADATA[system_name]
    filename = system_metadata.file_format.format(str(year), str(month).zfill(2))
    file_location = os.path.join("data_cache", system_name, filename)
    if not os.path.exists(file_location):
        return 0
    else:
        return sum(1 for i in open(file_location)) - 1

def CountAllLyftRides(date_range):
    system_rides = defaultdict(dict)
    for system_name in SYSTEM_METADATA.keys():
        system_rides[system_name]["Agency"] = system_name
        system_rides[system_name]["UZA Name"] = SYSTEM_METADATA[system_name].uza
        for d in date_range:
            column_name = d.strftime("%b").upper() + d.strftime("%y")
            system_rides[system_name][column_name] = CountLyftRides(system_name, d.year, d.month)
    return system_rides

def AugmentDataset(ntd_df, bike_counts):
    # Bike Counts is a dictionary {
    #    "baywheels": {"JAN18": 94802, "FEB18": 106718}
    #    "nyc_citibike": ...
    # }
    bicycle_df = pd.DataFrame(bike_counts).transpose()
    return pd.concat([ntd_df, bicycle_df], join="inner") # return only those overlapping columns that are shared


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("This script is intended to be called with one argument: the path to the latest NTD download URL.")
        print("For example, try $ python3 unicycle.py https://www.transit.dot.gov/sites/fta.dot.gov/files/2021-11/September%202021%20Raw%20Database_0.xlsx")

    start_date = "2018-01-01"
    end_date = "2021-09-01"
    date_range = pd.date_range(start_date, end_date, freq="MS")

    print("Updating sheet through most recent data.")

    # 1) Load the NTD from the URL passed in, fresh download.
    ntd_df = LoadNationalTransitDatabase(sys.argv[1])

    # 2) Update the local data_cache with all of the most recent data for all of the bikeshare systems we have.
    UpdateLocalBikeshareCache(date_range)

    # 3) Count the number of rides for each bikesharesystem-month
    bike_counts = CountAllLyftRides(date_range)

    # 4) Augment the ntd_df with the bike counts in the appropriate rows.
    augmented_df = AugmentDataset(ntd_df, bike_counts)
    print(augmented_df)

    augmented_df.to_clipboard(index=False)
    print("Output copied to clipboard as TSV; paste into google drive if you want.")

    augmented_df.to_csv("output.csv", index=False)

    

