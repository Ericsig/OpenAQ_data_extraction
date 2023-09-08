####OpenAQ data extraction
##author: Eric Sigwald

'''
The objective of this code is to extract data from the OpenAQ (Open Air Quality) API.
This API provides data about air quality in at least 300k locations arround the world.
The data for each location is updated every day. 
The parameters that define air quality are:
um005_particles/cm³, um100_particles/cm³, temperature_c, 
pm1_µg/m³, pressure_mb, um003_particles/cm³_avg, pm25_µg/m³,
humidity_%_avg, um025_particles/cm³, pm10_µg/m³,
pm10_µg/m³, um050_particles/cm³, temperature_f, 
um010_particles/cm³, no2_ppm, co_ppm, o3_ppm, so2_ppm, 
o3_µg/m³, voc_iaq, co_µg/m³, nox_µg/m³, no_µg/m³, so2_µg/m³,
no2_µg/m³.
The API provides information about the average and the last value measured of each parameter
In the next iterations, the idea is to extract each of those parameters and write them in the
final table inside Redshift
For the time being, this code generates a json with the following data:
'id', 'city', 'name', 'entity', 'country', 'sources', 'isMobile',
'isAnalysis', 'parameters', 'sensorType', 'lastUpdated', 'firstUpdated',
'measurements', 'bounds', 'manufacturers', 'coordinates.latitude',
'coordinates.longitude'. 

'''


###

import requests
import json
import os
#
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from transform_openaq_data import TransformData
class DataETL:
    def __init__(self, api_url, headers, redshift_config):
        self.api_url = api_url
        self.headers = headers
        self.redshift_config = redshift_config
        self.api_data = None

    def extract_data(self):
        '''
        The OpenAQ provides data about air quality in different locations.
        Every country has several locations, which can be retrieved using the country code.
        The first API request retrieves the country data. Then, 
        the country code is used for the second API request, which brings the locations-level data.
        '''
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[ 500, 502, 503, 504, 408])
        http_adapter = HTTPAdapter(max_retries=retries)
        session = requests.Session()
        session.mount("http://", http_adapter)
        session.mount("https://", http_adapter)
        try:
            response = session.get(self.api_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            api_data = response.json()
        except requests.exceptions.RequestException as e:
            print("API request failed:", e)
            exit()
        
        data_by_country={}
        for i in api_data['results']:
            country_code = i['code']
            print(f'Extracting data for {country_code}')
            try:
                url2 = f"https://api.openaq.org/v2/locations?country={country_code}"
                response = requests.get(url2, headers=self.headers)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print('API second request exception', e)
                continue
            data = response.json()
            data_by_country[country_code]=data['results']
        self.api_data = json.dumps(data_by_country, indent=4)


def main():
    api_url = "https://api.openaq.org/v2/countries?limit=200&offset=0&sort=asc"
    
    headers = {"Accept": "application/json", "X-API-Key": os.environ.get('apikey_openaq')}

    redshift_config = {
        'dbname': 'data-engineer-database',
        'host': 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        'port': '5439',
        'user': os.environ.get('redshift_db_user'),
        'password': os.environ.get('redshift_db_pass')
    }

    etl = DataETL(api_url, headers, redshift_config)
    etl.extract_data()
    open_aq_df = TransformData(json_data=etl.api_data)

if __name__ == "__main__":
    main()