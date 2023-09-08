import json
import pandas as pd


class TransformData:
    def __init__(self, json_data):
        self.json_data = json_data
   
    def transform_json_to_df(self):
        data_dict = json.loads(self.json_data)
        data_list = []
        # Iterate through the dictionary where each key is a country code
        for country_code, locations in data_dict.items():
            # Iterate through the list of locations for the current country
            for location in locations:
               # Create a dictionary for each location
               location_data = {
                     'id': location['id'],
                     'city': location['city'],
                     'name': location['name'],
                     'entity': location['entity'],
                     'country': location['country'],
                     'sources': location['sources'],
                     'isMobile': location['isMobile'],
                     'isAnalysis': location['isAnalysis'],
                     'parameters': location['parameters'],
                     'sensorType': location['sensorType'],
                     'lastUpdated': location['lastUpdated'],
                     'firstUpdated': location['firstUpdated'],
                     'measurements': location['measurements'],
                     'bounds': location['bounds'],
                     'manufacturers': location['manufacturers'],
                     'coordinates.latitude': location['coordinates']['latitude'],
                     'coordinates.longitude': location['coordinates']['longitude']
               }

               # Append the location_data to the list
               data_list.append(location_data)
         # Create a Pandas DataFrame from the list of location data
        self.open_aq_df = pd.DataFrame(data_list)

 ### Function to transform parameters data. Incomplete 
    # def transform_data(self, dicts_list):
    #     '''
    #     Each location providede has different 
    #     parameters that define the air quality at that location. 
    #     This parameters (temperature, humidity, pressure, 
    #     amount of particles in the air, etc) 
    #     are grouped in a column which contains a list of dictionaries.
    #     The function extracts the name of the parameter and 
    #     calculates the value for that parameter as 
    #     the difference between the last value and the 
    #     historical average, which could be taken as a 
    #     measure of the change.
    #     '''
    #     data_dict = {}
    #     for dicts in dicts_list:
    #         for entry in dicts:
    #             display_name = dicts['displayName']
    #             average = dicts['average']
    #             last_value = dicts['lastValue']
    #             if average != 0.0:
    #                 percentage_difference = ((last_value - average) / average) * 100
    #             else:
    #                 percentage_difference = 0

    #             data_dict[display_name] = percentage_difference
    #     return pd.Series(data_dict)
    
    ### Function to extract parameters data and write it into splitted columns.
    # def expand_data(self):
    #     try:
    #         aq_params=self.api_data['parameters'].apply(self.transform_data)
    #     except Exception as e:
    #         print('Error during data expansion', e)
    #     # Create column_mapping dictionary
    #     column_mapping = {f"{col}": f"{col.replace(' ', '_')}_%diff" for col in aq_params.columns}

    #     aq_params=aq_params.rename(columns=column_mapping)

    #     self.api_data = self.api_data.join(aq_params)
    #     self.api_data.drop(columns='parameters', inplace=True)

    ### Function to load data to redshift database. Incomplete   
    # def load_data_to_redshift(self):
    #     conn = psycopg2.connect(**self.redshift_config)
    #     cursor = conn.cursor()

    #     for row in self.api_data:
    #         values = self.transform_data(row)
    #         query = "INSERT INTO your_table_name (column1, column2, ...) VALUES (%s, %s, ...)"
    #         cursor.execute(query, values)

    #     conn.commit()
    #     cursor.close()
    #     conn.close()