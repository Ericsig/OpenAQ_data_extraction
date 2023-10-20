import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os

class CountryListExtraction:
    def __init__(self, api_url, headers):
        self.api_url = api_url
        self.headers = headers


    def extract_country_list(self):
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
            
            country_dict_list = api_data['results'] #No devuelve lista, sino lista de diccionarios con el siguiente formato [{'code': 'AF', 'name': 'Afghanistan', 'locations': 3, 'firstUpdated': '2019-10-20 22:30:00+00', 'lastUpdated': '2023-10-16 15:33:25+00', 'parameters': ['humidity', 'pm1', 'pm10', 'pm25', 'pressure', 'temperature', 'um003', 'um005', 'um010', 'um025', 'um050', 'um100'], 'count': 1742472, 'cities': 1, 'sources': 3}, {'code': 'DZ', 'name': 'Algeria', 'locations': 1, 'firstUpdated': '2019-06-15 22:00:00+00', 'lastUpdated': '2023-10-16 15:00:00+00', 'parameters': ['pm25'], 'count': 33975, 'cities': 1, 'sources': 1}]
            self.country_code_list = []
            for i in country_dict_list:
                self.country_code_list.append(i['code'])


def main():
    api_url = "https://api.openaq.org/v2/countries?limit=200&offset=0&sort=asc"
    
    headers = {"Accept": "application/json", "X-API-Key": os.environ.get('apikey_openaq')}
    country_extraction = CountryListExtraction(api_url=api_url, headers=headers)
    country_extraction.extract_country_list()
    country_list = country_extraction.country_code_list

    return country_list

if __name__ == 'main':
    main()
