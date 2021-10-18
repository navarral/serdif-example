# -*- coding: utf-8 -*-
from jinja2 import Environment, FileSystemLoader
import os
import glob
from tqdm import tqdm
from subprocess import call
import pandas as pd
import time
from pytz import timezone

# Check for all .csv files in directory
path2Files = 'raw/airQuality'
rawFilesList = glob.glob(path2Files + '/*.csv')
# Name for the metadata file
eeaMetadataFileName = 'PanEuropean_metadata'
eeaMetadataDF = pd.read_csv(path2Files + '/stationsMetadata/' + eeaMetadataFileName + '.csv')
# Load environment for jinja2 templates
file_loader = FileSystemLoader('templates')
env = Environment(loader=file_loader)
# Dictionary with Met Eireann weather variables full names, units and extra info
eeaAirQualityVars_Dict = {
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7': {
        'eeaVar': 'O3',
        'fullName': 'Ozone (air) - µg/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7012': {
        'eeaVar': 'Pb',
        'fullName': 'Lead (precip+dry_dep) - µg/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7012',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7014': {
        'eeaVar': 'Cd',
        'fullName': 'Cadmium (precip+dry_dep) - µg/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7014',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7015': {
        'eeaVar': 'Ni',
        'fullName': 'Nickel (precip+dry_dep) - µg/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7015',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7018': {
        'eeaVar': 'As',
        'fullName': 'Arsenic (precip+dry_dep) - µg/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7018',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5': {
        'eeaVar': 'PM10',
        'fullName': 'Particulate matter < 10 um (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/6001': {
        'eeaVar': 'PM2.5',
        'fullName': 'Particulate matter < 2.5 um (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/6001',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/2': {
        'eeaVar': 'NO2',
        'fullName': 'Nitrogen dioxide (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/2',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/8': {
        'eeaVar': 'NO2',
        'fullName': 'Nitrogen dioxide (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/8',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/9': {
        'eeaVar': 'NOX',
        'fullName': 'Nitrogen oxides (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/9',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1': {
        'eeaVar': 'SO2',
        'fullName': 'Sulphur dioxide (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1',
    },

    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/10': {
        'eeaVar': 'CO',
        'fullName': 'Carbon monoxide (air) - mg/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/mg.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/10',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/20': {
        'eeaVar': 'C6H6',
        'fullName': 'Benzene (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/20',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/21': {
        'eeaVar': 'C6H5CH3',
        'fullName': 'Toluene (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/21',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/431': {
        'eeaVar': 'C6H5CH3',
        'fullName': 'Ethyl benzene (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/431',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/464': {
        'eeaVar': 'mpC6H4CH32',
        'fullName': 'm,p-Xylene (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/464',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/482': {
        'eeaVar': 'oC6H4CH32',
        'fullName': 'o-Xylene (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/482',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/351': {
        'eeaVar': 'Acenaphthene',
        'fullName': 'Acenaphthene - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/351',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/352': {
        'eeaVar': 'Acenaphtylene',
        'fullName': 'Acenaphtylene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/352',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/435': {
        'eeaVar': 'Fluorene',
        'fullName': 'fluorene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/435',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/4406': {
        'eeaVar': 'Chrysene',
        'fullName': 'Chrysene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/4406',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/465': {
        'eeaVar': 'Naphtalene',
        'fullName': 'Naphtalene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/465',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5012': {
        'eeaVar': 'PbinPM10',
        'fullName': 'Lead in PM10 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5012',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5014': {
        'eeaVar': 'CdinPM10',
        'fullName': 'Cadmium in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5014',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5015': {
        'eeaVar': 'NiinPM10',
        'fullName': 'Nickel in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5015',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5018': {
        'eeaVar': 'AsinPM10',
        'fullName': 'Arsenic in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5018',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5029': {
        'eeaVar': 'BaPinPM10',
        'fullName': 'Benzo(a)pyrene in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5029',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5380': {
        'eeaVar': 'BenzoFluorantheneinPM10',
        'fullName': 'Benzo(b,j,k)fluoranthene in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5380',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5419': {
        'eeaVar': 'DibenzoanthraceneinPM10',
        'fullName': 'Dibenzo(ah)anthracene in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5419',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5610': {
        'eeaVar': 'BenzoaanthraceneinPM10',
        'fullName': 'Benzo(a)anthracene in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5610',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5617': {
        'eeaVar': 'BenzobfluorantheneinPM10',
        'fullName': 'Benzo(b)fluoranthene in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5617',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5626': {
        'eeaVar': 'BenzokfluorantheneinPM10',
        'fullName': 'Benzo(k)fluoranthene in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5626',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5655': {
        'eeaVar': 'Indeno123cdpyreneinPM10',
        'fullName': 'Indeno_123cd_pyrene in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5655',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5759': {
        'eeaVar': 'BenzojfluorantheneinPM10',
        'fullName': 'Benzo(j)fluoranthene in PM10 (aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/5759',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/606': {
        'eeaVar': 'AnthraceneinPM10',
        'fullName': 'Anthracene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/606',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/622': {
        'eeaVar': 'Benzoghiperylene',
        'fullName': 'Benzo(ghi)perylene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/622',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/643': {
        'eeaVar': 'Fluoranthene',
        'fullName': 'fluoranthene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/643',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/712': {
        'eeaVar': 'Phenanthrene',
        'fullName': 'Phenanthrene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/712',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/715': {
        'eeaVar': 'Pyrene',
        'fullName': 'Pyrene (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/715',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/4813': {
        'eeaVar': 'Hg0Hgreactive',
        'fullName': 'Total gaseous mercury (air+aerosol) - ng/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ng.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/4813',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/611': {
        'eeaVar': 'Benzoaanthracene',
        'fullName': 'Benzo(a)anthracene (precip+dry_dep) - ug/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/611',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/618': {
        'eeaVar': 'Benzobfluoranthene',
        'fullName': 'Benzo(b)fluoranthene (precip+dry_dep) - ug/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/618',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/627': {
        'eeaVar': 'Benzokfluoranthene',
        'fullName': 'Benzo(k)fluoranthene (precip+dry_dep) - ug/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/627',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/656': {
        'eeaVar': 'Indeno123cdpyrene',
        'fullName': 'Indeno-(1,2,3-cd)pyrene (precip+dry_dep) - ug/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/656',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7029': {
        'eeaVar': 'BaP',
        'fullName': 'Benzo(a)pyrene (precip+dry_dep) - ug/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7029',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7419': {
        'eeaVar': 'Dibenzoahanthracene',
        'fullName': 'Dibenzo(ah)anthracene (precip+dry_dep) - ug/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-2.day-1',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/7419',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1045': {
        'eeaVar': 'NH4inPM2.5',
        'fullName': 'Ammonium in PM2.5 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1045',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1046': {
        'eeaVar': 'NO3inPM2.5',
        'fullName': 'NO3- in PM2.5 (aerosol) - µg/m2/day',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1046',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1047': {
        'eeaVar': 'SO42inPM2.5',
        'fullName': 'Sulphate in PM2.5 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1047',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1629': {
        'eeaVar': 'Ca2inPM2.5',
        'fullName': 'Calcium in PM2.5 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1629',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1631': {
        'eeaVar': 'ClinPM2.5',
        'fullName': 'Chloride in PM2.5 (aerosol - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1631',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1657': {
        'eeaVar': 'KinPM2.5',
        'fullName': 'Potassium in PM2.5 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1657',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1659': {
        'eeaVar': 'Mg2inPM2.5',
        'fullName': 'Magnesium in PM2.5 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1659',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1668': {
        'eeaVar': 'NainPM2.5>',
        'fullName': 'Sodium in PM2.5 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1668',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1771': {
        'eeaVar': 'ECinPM2.5',
        'fullName': 'Elemental carbon in PM2.5 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1771',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1772': {
        'eeaVar': 'OCinPM2.',
        'fullName': 'Organic carbon in PM2.5 (aerosol) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/1772',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/38': {
        'eeaVar': 'NO',
        'fullName': 'Nitrogen monoxide (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/38',
    },
    'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/11': {
        'eeaVar': 'H2S',
        'fullName': 'Hydrogen sulphide (air) - ug/m3',
        'unit': 'http://dd.eionet.europa.eu/vocabulary/uom/concentration/ug.m-3',
        'eeaInfo': 'http://dd.eionet.europa.eu/vocabulary/aq/pollutant/11',
    },
}

datasetVersion = '20211012T120000'

# Generate a mapping and properties files for each
for f in tqdm(rawFilesList):
    # Use splitext() to get filename and extension separately.
    rawFileName = os.path.splitext(os.path.basename(f))[0]
    # Select Sampling Point from data file
    samplingP = pd.read_csv(f).SamplingPoint.unique()[0]
    stCode = samplingP.split('Sample')[0].split('IE.')[1]
    # Select Sampling Point from data file
    avgTime = pd.read_csv(f).AveragingTime.unique()[0]
    translatorAvgTime = {
        'hour': 'PT1H',
        'day': 'P1D',
        'var': 'P1M',
        'year': 'P1Y',
    }
    # Select dates time interval
    dateTimeList = pd.to_datetime(
        pd.read_csv(f).DatetimeBegin,
        format='%Y-%m-%d %H:%M:%S %z',
        yearfirst=True)
    startDateTime = str(min(dateTimeList).astimezone(timezone('UTC')).isoformat()).replace('+00:00', 'Z')
    endDateTime = str(max(dateTimeList).astimezone(timezone('UTC')).isoformat()).replace('+00:00', 'Z')
    airPollutant = pd.read_csv(f).AirPollutantCode.unique()[0]
    # Select rows with the previous Sampling Point
    eeaFilter = eeaMetadataDF[(eeaMetadataDF == samplingP).any(axis=1)]
    # Get download link from file name:
    # 1) EEA Airbase files are <2012
    # 2) Recent files are >2013
    if 'airbase' in rawFileName:
        fileDownloadUrl = 'https://ereporting.blob.core.windows.net/downloadservice-airbase/' + rawFileName + '.csv'
        yearDataset = rawFileName.split('_airbase')[0].split('_')[3]
    else:
        fileDownloadUrl = 'https://ereporting.blob.core.windows.net/downloadservice/' + rawFileName + '.csv'
        yearDataset = rawFileName.split('_timeseries')[0].split('_')[3]

    # 1. Generate mapping file from template
    # Load eea template file
    tempMap = env.get_template('eeaTemplate_DataCube_Obs.ttl')
    # Set data dictionary for input
    tempMap_dict = {'eeaDataFile': rawFileName,
                    'fileDownloadUrl': fileDownloadUrl,
                    'version': datasetVersion,
                    'stCode': stCode,
                    }
    outMap = tempMap.stream(data=tempMap_dict)
    # Export resulting mapping
    outMap.dump('mapping/airQuality/dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-mapping.ttl')
    # 2. Generate map properties file from template
    # Load eea template file
    tempProp = env.get_template('eeaTemplate_DataCube_Obs.properties')
    # Set data dictionary for input
    tempProp_dict = {
        'mappingFile': 'mapping/airQuality/dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-mapping.ttl',
        'eeaDataFile': 'raw/airQuality/' + rawFileName + '.csv',
        'rdfDataFile': 'rdf/airQuality/dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-data.ttl',
    }
    print(rawFileName)
    outProp = tempProp.stream(data=tempProp_dict)
    # Export resulting mapping properties file
    outPropName = 'mapping/airQuality/' + 'dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-mapping.properties'
    outProp.dump(outPropName)
    # 3. Execute mapping to convert csv file to RDF
    call(['java', '-Xmx4112m', '-jar', 'r2rml-v1.2.3b/r2rml.jar', outPropName])

    # Fill air quality dataset RDF template with the values from the metadata file
    # Load template
    datasetTemp = env.get_template('eeaTemplate_DataCube_DataSet.ttl')
    # Set data dictionary for input
    datasetTemp_dict = {
        'version': datasetVersion,
        'stCode': stCode,
        'versionDateTime': '2021-10-12T12:00:00Z',
        'downloadFileUrl': fileDownloadUrl,
        'yearDataset': yearDataset,
        'bitSize': os.path.getsize('rdf/airQuality/dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-data.ttl'),
        'timeUnit': translatorAvgTime[avgTime],
        'startDate': startDateTime,
        'endDate': endDateTime,
        'lat': str(eeaFilter.Latitude.values[0]),
        'lon': str(eeaFilter.Longitude.values[0]),
        'altitude': str(eeaFilter.Altitude.values[0]),
        'eeaVar': eeaAirQualityVars_Dict[airPollutant]['eeaVar'],
        'eeaVar_nameAndUnitUpper': eeaAirQualityVars_Dict[airPollutant]['fullName'].upper(),
        'eeaVar_nameAndUnit': eeaAirQualityVars_Dict[airPollutant]['fullName'],
        'eeaVar_Unit': eeaAirQualityVars_Dict[airPollutant]['unit'],
        'eeaVar_eeaInfo': eeaAirQualityVars_Dict[airPollutant]['eeaInfo'],
    }

    outDatasetTemp = datasetTemp.stream(data=datasetTemp_dict)
    # Export resulting mapping
    outDatasetTemp.dump('rdf/airQuality/' + 'dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-metadata.ttl')

    rdfDataFile = 'rdf/airQuality/dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-data.ttl'
    rdfMetadataFile = 'rdf/airQuality/' + 'dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-metadata.ttl'
    zipName = 'rdf/airQuality/dataset-eea-' + datasetVersion + '-' + stCode + '-' + yearDataset + '-data.zip'
    # Zip RDF data and metadata files
    call(['zip', zipName, rdfDataFile, rdfMetadataFile, '-j'])
    # Remove originals to save space
    os.remove(rdfDataFile)
    os.remove(rdfMetadataFile)
    time.sleep(3)
