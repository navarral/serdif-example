import requests
import json
import xmltodict
from pprint import pprint
from collections import defaultdict
import pandas as pd
import io

localHost = 'https://serdif-example.adaptcentre.ie/repositories/'
repoID = 'repo-serdif-envo-ie'

qPref2 = '''
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX eg: <http://example.org/ns#>
PREFIX geohive-county-geo: <http://data.geohive.ie/pathpage/geo:hasGeometry/county/>
PREFIX sdmx-dimension: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
'''

bodyQ = '''
CONSTRUCT{
    ?sliceName
        a qb:Slice;
        qb:sliceStructure 			eg:sliceByTime ;
        eg:refArea 					geohive-county-geo:2ae19629-1454-13a3-e055-000000000001 ;
        eg:refEvent        			?event ;
        qb:observation   			?obsName ;
        .

    ?obsName
        a qb:Observation ;
        qb:dataSet 					?datasetName ;
        sdmx-dimension:timePeriod 	?obsTimePeriod ;
        ?envProp 					?envVar ;
        .
}
WHERE {
    {
        SELECT ?event ?yearT ?monthT ?dayT ?envProp (AVG(?envVar) AS ?envVar)
        WHERE {
            {
                SELECT ?obsData ?obsTime
                WHERE{
                    VALUES ?envoDataSet {eg:dataset-eea-20211012T120000-IE001DM eg:dataset-eea-20211012T120000-IE003DP eg:dataset-met-20211012T120000-IE1775}
                    ?obsData
                        a qb:Observation ;
                        qb:dataSet ?envoDataSet ;
                        sdmx-dimension:timePeriod ?obsTime .        
                    FILTER(?obsTime > "2019-02-03T00:00:00Z"^^xsd:dateTime && ?obsTime <= "2019-03-03T00:00:00Z"^^xsd:dateTime)
                }
            }
            ?obsData ?envProp ?envVar .
            FILTER(datatype(?envVar) = xsd:float)    
            #String manipulation to order dates
            BIND(YEAR(?obsTime) AS ?yearT)
            BIND(MONTH(?obsTime) AS ?monthT)
            BIND(DAY(?obsTime) AS ?dayT)
            BIND(HOURS(?obsTime) AS ?hourT)
            BIND("eventA" AS ?event)
        }
        GROUP BY ?event ?envProp ?dayT ?monthT ?yearT
        ORDER BY ?obsTime
    } UNION{
        SELECT ?event ?yearT ?monthT ?dayT ?envProp (AVG(?envVar) AS ?envVar)
        WHERE {
            {
                SELECT ?obsData ?obsTime
                WHERE{
                    VALUES ?envoDataSet {eg:dataset-eea-20211012T120000-IE004CP eg:dataset-eea-20211012T120000-IE0125A eg:dataset-met-20211012T120000-IE1875 eg:dataset-met-20211012T120000-IE275}
                    ?obsData
                        a qb:Observation ;
                        qb:dataSet ?envoDataSet ;
                        sdmx-dimension:timePeriod ?obsTime .        
                    FILTER(?obsTime > "2018-02-03T00:00:00Z"^^xsd:dateTime && ?obsTime <= "2018-03-03T00:00:00Z"^^xsd:dateTime)
                }
            }
            ?obsData ?envProp ?envVar .
            FILTER(datatype(?envVar) = xsd:float)    
            #String manipulation to order dates
            BIND(YEAR(?obsTime) AS ?yearT)
            BIND(MONTH(?obsTime) AS ?monthT)
            BIND(DAY(?obsTime) AS ?dayT)
            BIND(HOURS(?obsTime) AS ?hourT)
            BIND("eventB" AS ?event)
        }
        GROUP BY ?event ?envProp ?dayT ?monthT ?yearT
        ORDER BY ?obsTime
    }
    BIND( IF( BOUND(?monthT), IF(STRLEN( STR(?monthT) ) = 2, STR(?monthT), CONCAT("0", STR(?monthT)) ), "01") AS ?monthTF )
    BIND( IF( BOUND(?dayT), IF( STRLEN( STR(?dayT) ) = 2, STR(?dayT), CONCAT("0", STR(?dayT)) ), "01" ) AS ?dayTF )
    BIND( IF( BOUND(?hourT) , STR(?hourT), "00" ) AS ?hourTF )
    BIND(CONCAT(str(?yearT),"-",?monthTF,"-",?dayTF,"T",?hourTF,":00:00Z") AS ?obsTimePeriod)
    BIND(IRI(CONCAT("http://example.org/ns#dataset-eea-20211012T120000-IE-", ?event ,"-obs-", str(?yearT),?monthTF,?dayTF,"T",?hourTF,"0000Z")) AS ?obsName)
    BIND(IRI(CONCAT("http://example.org/ns#dataset-eea-20211012T120000-IE-", ?event ,"-slice")) AS ?sliceName)
    BIND(IRI(CONCAT("http://example.org/ns#dataset-eea-20211012T120000-IE-QT_", ENCODE_FOR_URI(STR(NOW())))) AS ?datasetName)

}
'''

rQuery = requests.post(
    localHost + repoID,
    # data={'query': 'SELECT ?s ?p ?o { ?s ?p ?o . } LIMIT 4'},
    data={'query': qPref2 + bodyQ},
    auth=('hdr001', 'hdr001?'),
    headers={
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0',
        'Referer': 'https://serdif-example.adaptcentre.ie/sparql',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    }
)
print(rQuery.status_code, rQuery.reason)

q_dict = json.loads(json.dumps(xmltodict.parse(rQuery.content)))
# pprint(q_dict['rdf:RDF']['rdf:Description']) #[0]['@rdf:about'])

# Select events
eventKeys = [od['eg:refEvent'] for od in q_dict['rdf:RDF']['rdf:Description'] if 'eg:refEvent' in od.keys()]

# Build dictionary with environmental observations associated to events
ee_dict = dict()
for ev in eventKeys:
    # Check if there is already an event key available
    if ev not in ee_dict:
        ee_dict[ev] = {}
        for od in q_dict['rdf:RDF']['rdf:Description']:
            if ev + '-obs-' in od['@rdf:about']:
                dateTimeKey = od['@rdf:about'].split('obs-')[1]
                # check if there is already an event-dateT pair available
                if dateTimeKey not in ee_dict[ev]:
                    ee_dict[ev][dateTimeKey] = {}
                # Store values for specific event-dateTime pair
                for envProp in od.keys():
                    if 'eg:has' in envProp:
                        envPropKey = envProp.split('eg:has')[1]
                        ee_dict[ev][dateTimeKey][envPropKey] = od[envProp]['#text']

# Nested dictionary to pandas dataframe
df_ee = pd.DataFrame.from_dict(
    {(i, j): ee_dict[i][j]
     for i in ee_dict.keys()
     for j in ee_dict[i].keys()},
    orient='index'
)
# Multi-index to column
df_ee = df_ee.reset_index()
# 1.Convert to CSV
df_ee_csv = df_ee.to_csv(index=False)
# 2.ReParse CSV object as text and then read as CSV. This process will
# format the columns of the data frame to data types instead of objects.
df_ee_r = pd.read_csv(io.StringIO(df_ee_csv), index_col='level_1').round(decimals=2)
# Converting the index as dateTime
df_ee_r.index = pd.to_datetime(df_ee_r.index)
df_ee_r.rename(columns={'level_0': 'event'}, inplace=True)
# Sort by event and dateT
df_ee_r = df_ee_r.rename_axis('dateT').sort_values(by=['dateT', 'event'], ascending=[False, True])

print(df_ee_r)

# --------------------------------

from SPARQLWrapper import SPARQLWrapper
from pprint import pprint

localHost = 'https://serdif-example.adaptcentre.ie/repositories/'
repoID = 'repo-serdif-envo-ie'

qPref = '''
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX qb: <http://purl.org/linked-data/cube#>
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX sdmx-dimension: <http://purl.org/linked-data/sdmx/2009/dimension#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX eg: <http://example.org/ns#>
'''

bodyQ = '''
SELECT ?event ?evDateT ?dateLag ?dateStart ?envoDataSet
WHERE {
    # Filter environmental data within a region
    ?envoDataSet
        a qb:DataSet, geo:Feature, prov:Entity, dcat:Dataset ;
        dct:Location/geo:asWKT ?envoGeo .
    #County geom    
    ?county
        a geo:Feature, <http://ontologies.geohive.ie/osi#County> ;
        rdfs:label ?LOI ;
        geo:hasGeometry/geo:asWKT ?countyGeo .
    FILTER(?LOI = "WEXFORD" )
    FILTER(geof:sfWithin(?envoGeo, ?countyGeo))

    # Filter events data within a region
    SERVICE <repository:repo-serdif-events-ie>{
        ?event
            a prov:Activity ;
            rdfs:label ?eventType ;
            prov:startedAtTime ?eventTime ;
            prov:atLocation/geo:asWKT ?eventGeo .
        #County geom    
        ?county
            a geo:Feature, <http://ontologies.geohive.ie/osi#County> ;
            rdfs:label ?LOI ;
            geo:hasGeometry/geo:asWKT ?countyGeo .
        FILTER(?LOI = "CAVAN" )
        FILTER(geof:sfWithin(?eventGeo, ?countyGeo))
		BIND(xsd:dateTime(?eventTime) AS ?evDateT)
        BIND(?evDateT - "P7D"^^xsd:duration AS ?dateLag)
		BIND(?dateLag - "P30D"^^xsd:duration AS ?dateStart)
    }     
}
'''

# 1.2.Query parameters
SPARQLQuery = SPARQLWrapper(localHost + repoID)

SPARQLQuery.setMethod('POST')
SPARQLQuery.setQuery(qPref + bodyQ)
SPARQLQuery.setReturnFormat('json')
SPARQLQuery.setCredentials(user='hdr001', passwd='hdr001?')

# 1.3.Fire query and convert results to json (dictionary)
qEvLocTime_dict = SPARQLQuery.query().convert()
# 1.4.Return results
jEvLocTime = qEvLocTime_dict['results']['bindings']

pprint(jEvLocTime)
