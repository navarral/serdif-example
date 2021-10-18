#import requests, zipfile, io
import pandas as pd
import numpy as np
import subprocess
import os

# Extract station codes from Met Eireann station details
MEStatDet_File = os.getcwd() + '/stationsMetadata/MetStationDetails.csv'
# Read file as CSV
MEStatDet_df = pd.read_csv(MEStatDet_File, sep=',') #, skiprows=1)
print(MEStatDet_df.head())
# Select stations with years from 2003 to Present
MEStatDet_df[['Close Year']] = MEStatDet_df[['Close Year']].fillna(value=999)
MEStatDet_Period = MEStatDet_df.loc[#(MEStatDet_df['Open Year'] <= 2010) &
                                    ((MEStatDet_df['Close Year'] == 0) |
                                    (MEStatDet_df['Close Year'] == 999)|
                                    (MEStatDet_df['Close Year'] >= 2002))]
# Select stations that meet the criteria
MEStatCodes_list = MEStatDet_Period['Station Number'].values.tolist()
# Daily 'dly, Hourly 'hly' or Monthly 'mly'
MetFreq = 'hly'

if MetFreq == 'hly':
    MetFolder = 'Hourly'
#elif MetFreq == 'dly':
#    MetFolder = 'DailyData'

# Populate lists with MetEireann csv url locations and paths to save them
MetURL_List = []
MetPaths_List = []
for MetFZ in MEStatCodes_list:
    # Met Eireann base url for daily data zip files
    MetURL_base = 'http://cli.fusio.net/cli/climate_data/webdata/'
    MetURL_List.append(MetURL_base + MetFreq + str(MetFZ) + '.csv')
    MetPaths_List.append(os.getcwd() + '/' + MetFreq + str(MetFZ))

# Download files with wget command from bash
bashCommand = lambda url, filename: "wget -O %s.csv %s" % (filename, url)

# Arrange uls and file destination paths in a dictionary
save_locations = dict(zip(MetURL_List, MetPaths_List))
for url, filename in save_locations.items():
    process = subprocess.Popen(bashCommand(url, filename).split(), stdout=subprocess.PIPE)
    output = process.communicate()[0]


# Remove empty files
for MetFileCheck in MetPaths_List:
    MetPathsAbs = MetFileCheck + '.csv'
    if os.path.getsize(MetPathsAbs) == 0:
        os.remove(MetPathsAbs)

