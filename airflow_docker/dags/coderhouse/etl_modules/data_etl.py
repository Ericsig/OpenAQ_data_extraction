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


###Imports
#Import libraries
import requests
import json
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#Custom imports
from coderhouse.etl_modules.connect_openaq_api import CountryListExtraction
from coderhouse.etl_modules.transform_openaq_data import TransformData
from coderhouse.etl_modules.load_to_redshift import RedshiftDataLoader

#ETL Class
class DataETL:
    def __init__(self, api_url, headers, db_user, db_pass, db_host,
                country_list, 
                low_threshold, 
                high_threshold,
                filter_parameter):
        self.api_url = api_url
        self.headers = headers
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.country_list = country_list
        #The following parameters are necessary to filter and alert if a value of the given parameter goes beyond the thresholds
        self.low_threshold = low_threshold
        self.high_threshold = high_threshold
        self.parameter = filter_parameter
   
   
    # Function to extract data from OpenAQ API
    def extract_data(self):
        '''
        The OpenAQ provides data about air quality in different locations.
        Every country has several locations, which can be retrieved using the country code.
        The first API request retrieves the country data. Then, 
        the country code is used for the second API request, which brings the locations-level data.
        '''
        data_by_country = {}
        for country_code in self.country_list:
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

    def transform_data(self):  
        if self.api_data is not None:
            transformer = TransformData(self.api_data)
            transformer.transform_json_to_df()
            transformer.clean_dataframe()
            transformer.check_parameters(self.low_threshold, self.high_threshold, self.parameter)
            transformer.convert_column_to_string()
            self.anomalies = transformer.anomalies
            self.transformed_data = transformer.open_aq_df
    
    def load_data(self):
        db_user = self.db_user
        db_pass = self.db_pass
        db_host = self.db_host
        loader = RedshiftDataLoader(db_pass=db_pass, db_host=db_host, db_user=db_user)
        try:
            loader.connect_to_database()
        except Exception as e:
            print('Failed to connect to DB', e)
        try:
            loader.load_data_to_database(self.transformed_data)
        except Exception as e:
            print('Failed to load data to DB', e)
        

def main():
    api_url = "https://api.openaq.org/v2/countries?limit=200&offset=0&sort=asc"
    
    headers = {"Accept": "application/json", "X-API-Key": 'apikey_openaq'}
    db_user = 'redshift_db_user'
    db_pass = 'redshift_db_pass'
    country_extraction = CountryListExtraction(api_url, headers)
    country_list = country_extraction.extract_country_list()

    etl = DataETL(api_url, headers, db_user, db_pass, country_list)
    etl.extract_data()
    etl.transform_data()
    etl.load_data()


if __name__ == "__main__":
    main()
