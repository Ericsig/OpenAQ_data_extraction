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
        print('JSON converted to DF')
    
    #Function to remove duplicates and NaNs
    def clean_dataframe(self):
        self.open_aq_df.drop_duplicates(subset='id', inplace=True)
        self.open_aq_df.dropna(axis=0, subset=['id', 'city', 'name', 'parameters'], inplace=True)
        print('Dataframe cleaned')

    
    def _transform_parameters_data(self, dicts_list):
        '''
        Each location providede has different 
        parameters that define the air quality at that location. 
        This parameters (temperature, humidity, pressure, 
        amount of particles in the air, etc) 
        are grouped in a column which contains a list of dictionaries.
        The function extracts the name of the parameter and writes the last value measured.
        '''
        data_dict = {}
        for dicts in dicts_list:
            for entry in dicts:
                display_name = dicts['displayName']
                last_value = dicts['lastValue']
                data_dict[display_name] = last_value
        return pd.Series(data_dict)
    
    def check_parameters(self, low_threshold, high_threshold, parameter):
        try:
            aq_params=self.open_aq_df['parameters'].apply(self._transform_parameters_data)
        except Exception as e:
            print('Error during data expansion', e)
        # Create column_mapping dictionary
        column_mapping = {f"{col}": f"{col.replace(' ', '_')}" for col in aq_params.columns}
        aq_params=aq_params.rename(columns=column_mapping)

        if parameter in aq_params.columns:
            prm_df = self.open_aq_df.join(aq_params)
            join_df = prm_df[prm_df[parameter].apply(lambda x: x < float(low_threshold) or x > float(high_threshold))]
            selected_columns = ['id', 'name', parameter, 'lastUpdated']
            self.anomalies = join_df[selected_columns].to_dict(orient='records')
            
        elif parameter not in aq_params.columns:
            self.anomalies = f'No {parameter} data'
        else:
            self.anomalies = f'No anomalies in {parameter}'
    
    #Function to transform "parameters", "bounds" and "manufacturers" columns to string
    def convert_column_to_string(self):
        self.open_aq_df['parameters'] = self.open_aq_df['parameters'].apply(json.dumps)
        self.open_aq_df['manufacturers'] = self.open_aq_df['manufacturers'].apply(json.dumps)
        self.open_aq_df['bounds'] = self.open_aq_df['bounds'].astype(str)



def main(json_data):
    df_transform = TransformData(json_data)
    df_transform.transform_json_to_df()
    df_transform.clean_dataframe()
    df_transform.check_parameters()
    df_transform.convert_column_to_string()

if __name__ == "__main__":
    main()   