# -*- coding: utf-8 -*-
from jinja2 import Environment, FileSystemLoader
import os
import glob
from tqdm import tqdm
from subprocess import call
import time
import io
import pandas as pd
import re


# add_delimiters function to read irregular csv files
def add_delimiters(fpath, delimiter=','):
    s_data = ''
    max_num_delimiters = 0

    with open(fpath, 'r') as f:
        for line in f:
            s_data += line
            delimiter_count = line.count(delimiter)
            if delimiter_count > max_num_delimiters:
                max_num_delimiters = delimiter_count

    s_delimiters = delimiter * max_num_delimiters + '\n'

    return io.StringIO(s_delimiters + s_data)


# Check for all .csv files in the raw/weather directory
path2Files = 'raw/weather'
rawFilesList = glob.glob(path2Files + '/*.csv')

# Load environment (directory) for jinja2 templates
file_loader = FileSystemLoader('templates')
env = Environment(loader=file_loader)

# Dictionary with Met Eireann weather variables full names, units and extra info
metWeatherVars_Dict = {
    'rain': {
        'fullName': 'Precipitation Amount - mm',
        'unit': 'unit:MilliM',
        'metInfo': 'https://www.met.ie/climate/what-we-measure/rainfall',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/A05/current/EV_RAIN/',
    },
    'temp': {
        'fullName': 'Air Temperature - ºC',
        'unit': 'unit:DEG_C ',
        'metInfo': 'https://www.met.ie/climate/what-we-measure/temperature',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/A05/current/EV_AIRTEMP/'
    },
    'wetb': {
        'fullName': 'Wet Bulb Air Temperature - ºC',
        'unit': 'unit:DEG_C',
        'metInfo': 'https://www.met.ie/climate/what-we-measure/temperature',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P01/current/CWETZZ01/'
    },
    'dewpt': {
        'fullName': 'Dew Point Air Temperature - ºC',
        'unit': 'unit:DEG_C',
        'metInfo': 'https://www.met.ie/climate/what-we-measure/temperature',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P01/current/CDEWZZ01/'
    },
    'vappr': {
        'fullName': 'Vapour Pressure - hPa',
        'unit': 'unit:HectoPA',
        'metInfo': 'https://www.met.ie/climate/what-we-measure/water-vapour',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P07/current/CFSN0032/'
    },
    'rhum': {
        'fullName': 'Relative Humidity - %',
        'unit': 'unit:PERCENT',
        'metInfo': 'https://www.met.ie/climate/what-we-measure',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P01/current/CRELZZ01/',
    },
    'msl': {
        'fullName': 'Mean Sea Level Pressure - hPa',
        'unit': 'unit:HectoPA',
        'metInfo': 'https://www.met.ie/climate/what-we-measure',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P01/current/CDEWZZ01/'
    },
    'wdsp': {
        'fullName': 'Mean Hourly Wind Speed - kt',
        'unit': 'unit:KN',
        'metInfo': 'https://www.met.ie/climate/what-we-measure/wind',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/A05/current/EV_WSPD/',
    },
    'wddir': {
        'fullName': 'Predominant Hourly wind Direction - º',
        'unit': 'unit:DEG',
        'metInfo': 'https://www.met.ie/climate/what-we-measure/wind',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/A05/current/EV_WDIR/',
    },
    'sun': {
        'fullName': 'Sunshine duration - hours',
        'unit': 'unit:HR',
        'metInfo': 'https://www.met.ie/climate/what-we-measure/sunshine',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P07/current/CFSN0643/',
    },
    'vis': {
        'fullName': 'Visibility - m',
        'unit': 'unit:M',
        'metInfo': 'https://www.met.ie/climate/what-we-measure',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P07/current/CFSN0061/'
    },
    'clht': {
        'fullName': 'Cloud Ceiling Height - 100s feet',
        'unit': '<http://vocab.nerc.ac.uk/collection/P06/current/UUUU/>',
        'metInfo': 'https://www.met.ie/climate/what-we-measure',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P07/current/CFSN0747/'
    },
    'clamt': {
        'fullName': 'Cloud Amount - okta',
        'unit': '<http://vocab.nerc.ac.uk/collection/P06/current/UUUU/>',
        'metInfo': 'https://www.met.ie/climate/what-we-measure',
        'envoVocLink': 'http://vocab.nerc.ac.uk/collection/P07/current/CFSN0745/'
    }

}

datasetVersion = '20211012T120000'

# Semantic uplift for each of the weather datasets
for f in tqdm(rawFilesList):
    # Use splitext() to get filename and extension separately.
    rawFileName = os.path.splitext(os.path.basename(f))[0]
    csvFileRead = pd.read_csv(add_delimiters(f), sep=',', encoding='utf-8')
    # Select Station code
    try:
        stCode = re.search('/hly(.+?).csv', f).group(1)
    except AttributeError:
        # ly, .csv not found in the original string
        stCode = 'NoCode'  # apply your error handling

    # Select row to extract station location
    indLoc = csvFileRead[['Latitude' in x for x in csvFileRead['Unnamed: 0']]].values.tolist()[0]
    # Extract latitude and longitude coordinates within the 2 first columns
    indLatLon = [re.findall(r'[-+]?\d*\.\d+|\d+', s) for s in indLoc[0:2]]
    # Flatten LatLon list due to formatting Location
    indLatLonF = [item for sublist in indLatLon for item in sublist]
    # Select station height
    checkHeight = 'Station Height: '
    indHeight = csvFileRead[[checkHeight in x for x in csvFileRead['Unnamed: 0']]].values.tolist()[0]
    statHeight = indHeight[0].replace(checkHeight, '', 1).replace(' ', '').replace('M', '')
    # Select row where the word date is present
    indDate = csvFileRead.loc[csvFileRead['Unnamed: 0'] == 'date'].index.values.astype(int)[0]
    # Drop previous rows
    csvFileNoHeader = csvFileRead.iloc[indDate:]
    # Set column names
    csvFileNoHeader.columns = csvFileNoHeader.iloc[0]
    csvFileNoHeader = csvFileNoHeader[1:]
    csvFileNoHeader.pop('ind')
    # Select dates time interval
    dateTimeList = pd.to_datetime(
        csvFileNoHeader['date'],
        format='%d-%b-%Y %H:%M',
        yearfirst=True)
    startDateTime = str(min(dateTimeList)).replace(' ', 'T') + 'Z'
    endDateTime = str(max(dateTimeList)).replace(' ', 'T') + 'Z'

    # Column names in capitals for R2RML
    metVarCap = [varL.capitalize() for varL in csvFileNoHeader.columns[1:] if (varL != 'w') and (varL != 'ww')]
    # Capitalize only first letter for properties
    metVarUp = [varL.upper() for varL in csvFileNoHeader.columns[1:] if (varL != 'w') and (varL != 'ww')]
    # Lower case variables for label
    metVarLow = [varL.lower() for varL in csvFileNoHeader.columns[1:] if (varL != 'w') and (varL != 'ww')]
    # Full name and unit for the variables Upper case for comment at the start
    metVarNameUnitUp = [metWeatherVars_Dict[varL]['fullName'].upper() for varL in metVarLow]
    # Full name and unit for the variables
    metVarNameUnit = [metWeatherVars_Dict[varL]['fullName'] for varL in metVarLow]
    # Variable unit
    metVarUnit = [metWeatherVars_Dict[varL]['unit'] for varL in metVarLow]
    # Met Eireann information about the variable
    metVarInfo = [metWeatherVars_Dict[varL]['metInfo'] for varL in metVarLow]
    # The NERC Vocabulary Server (NVS) information about the variable
    metVarEnvoLink = [metWeatherVars_Dict[varL]['envoVocLink'] for varL in metVarLow]

    # Save temporary csv file without original header for R2RML
    #csvFileNoHeader = csvFileNoHeader.applymap(str)
    csvFileNoHeader.to_csv('raw/weather/hly' + stCode + '_temp.csv', index=False)
    # 1. Generate mapping file from template
    # Load eea template file
    tempMap = env.get_template('metTemplate_DataCube_Obs.ttl')
    # Set data dictionary for input
    tempMap_dict = {
        'metDataFile': 'hly' + stCode + '_temp',
        'version': datasetVersion,
        'stCode': stCode,
        'metVarsC': zip(metVarCap, metVarUp)
    }
    outMap = tempMap.stream(data=tempMap_dict)
    # Export resulting mapping
    outMap.dump('mapping/weather/' + 'dataset-met-' + datasetVersion + '-IE' + stCode + '-mapping.ttl')

    # 2. Generate map properties file from template
    # Load eea template file
    tempProp = env.get_template('metTemplate_DataCube_Obs.properties')
    # Set data dictionary for input
    tempProp_dict = {
        'mappingFile': 'mapping/weather/dataset-met-' + datasetVersion + '-IE' + stCode + '-mapping.ttl',
        'metDataFile': 'raw/weather/hly' + stCode + '_temp.csv',
        'rdfDataFile': 'rdf/weather/dataset-met-' + datasetVersion + '-IE' + stCode + '-data.ttl',
    }
    outProp = tempProp.stream(data=tempProp_dict)
    # Export resulting mapping properties file
    outPropName = 'mapping/weather/' + 'dataset-met-' + datasetVersion + '-IE' + stCode + '-mapping.properties'
    outProp.dump(outPropName)

    # 3. Execute mapping to convert csv file to RDF
    call(['java', '-Xmx4112m', '-jar', 'r2rml-v1.2.3b/r2rml.jar',outPropName])
    # Remove temporal CSV file to save space
    os.remove('raw/weather/hly' + stCode + '_temp.csv')

    # Fill weather dataset RDF template with the values from the heading of the CSV file
    # Load template
    datasetTemp = env.get_template('metTemplate_DataCube_DataSet.ttl')
    # Set data dictionary for input
    datasetTemp_dict = {
        'version': datasetVersion,
        'stCode': stCode,
        'metDataFile': rawFileName + '_temp',
        'versionDateTime': '2021-10-12T12:00:00Z',
        'bitSize': os.path.getsize('rdf/weather/dataset-met-' + datasetVersion + '-IE' + stCode + '-data.ttl'),
        'startDate': startDateTime,
        'endDate': endDateTime,
        'lat': indLatLonF[0],
        'lon': indLatLonF[1],
        'altitude': statHeight,
        'metVars': metVarCap,
        'metVarsD': zip(*[metVarNameUnitUp, metVarCap, metVarLow,
                          metVarNameUnit, metVarUnit, metVarInfo, metVarEnvoLink]),
        'metVarsFile': ['rdfTempFiles/test.trig'],
    }
    outDatasetTemp = datasetTemp.stream(data=datasetTemp_dict)
    # Export resulting mapping
    outDatasetTemp.dump('rdf/weather/' + 'dataset-met-' + datasetVersion + '-IE' + stCode + '-metadata.ttl')

    rdfDataFile = 'rdf/weather/dataset-met-' + datasetVersion + '-IE' + stCode + '-data.ttl'
    rdfMetadataFile = 'rdf/weather/dataset-met-' + datasetVersion + '-IE' + stCode + '-metadata.ttl'
    zipName = 'rdf/weather/dataset-met-' + datasetVersion + '-IE' + stCode + '-data.zip'
    # Zip RDF data and metadata files
    call(['zip', zipName, rdfDataFile, rdfMetadataFile, '-j'])
    # Remove originals to save space
    os.remove(rdfDataFile)
    os.remove(rdfMetadataFile)
    time.sleep(3)

