import pycristoforo as pyc
import pandas as pd
from datetime import datetime
import numpy as np
from subprocess import call
import os

# 1. Random events (geolocated dates) within an country
# Event patient ID
while True:
    try:
        nInvEv = int(input('Enter number of individuals related to the events: '))
    except ValueError:
        print('Sorry, you did not enter a valid integer value. Please try again')
        continue
    else:
        # The number of event were not successfully parsed
        break

while True:
    try:
        nEvent = int(input('Enter number of random events: '))
    except ValueError:
        print('Sorry, you did not enter a valid integer value. Please try again')
        continue
    else:
        # The number of event were not successfully parsed
        break

# 1.1 Download geoJSON file
ireGeoJ = pyc.get_shape('Ireland')
# 1.2. Generate random points within
ireRndLoc = pyc.geoloc_generation(ireGeoJ, nInvEv, 'Ireland')


# 1.3. Generate random dates within an interval
def rndDatesGen(start, end, n):
    start_u = start.value // 10 ** 9
    end_u = end.value // 10 ** 9
    return pd.DatetimeIndex((10 ** 9 * np.random.randint(start_u, end_u, n, dtype=np.int64)).view('M8[ns]'))


# Set interval range
stDate = pd.to_datetime('2013-01-01')
endDate = pd.to_datetime('2020-12-31')
# Get dates within the interval range
ireRndDate = rndDatesGen(start=stDate, end=endDate, n=nInvEv*nEvent)

# 1.4. Random events generation
# Event types
rndEventType = np.random.choice(['Definite', 'HighProbability', 'Possible', 'No'],
                                size=nInvEv*nEvent, replace=True)

rndSubEventID = np.random.choice(list(range(1, int(nInvEv))),
                                 size=nInvEv, replace=True)
# 1.5. Build a dataframe
ireRndDf = pd.DataFrame({
    'indID': [p['properties']['point'] for p in ireRndLoc],
    'lat': [round(p['geometry']['coordinates'][1], 4) for p in ireRndLoc],
    'lon': [round(p['geometry']['coordinates'][0], 4) for p in ireRndLoc],
})

ireRndDf = pd.concat([ireRndDf]*nEvent, ignore_index=True)
ireRndDf['date'] = [str(dateT.isoformat()) + 'Z' for dateT in ireRndDate]
ireRndDf['evID'] = ireRndDf.index + 1
ireRndDf['eventType'] = rndEventType
print(ireRndDf)

# Save as csv file
ireRndDf.to_csv('raw/events/RndHealthEvents_Ireland.csv', index=False)

# Run R2RML to convert CSV to RDF file
datasetVersion = '20211012T120000'
outPropName = 'mapping/events/' + 'dataset-events-' + datasetVersion + '-IE-mapping.properties'

# 3. Execute mapping to convert csv file to RDF
call(['java', '-Xmx4112m', '-jar', 'r2rml-v1.2.3b/r2rml.jar', outPropName])
