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
    #Function to remove duplicates and NaNs

    def clean_dataframe(self):
        self.open_aq_df.drop_duplicates(subset='id', inplace=True)
        self.open_aq_df.dropna(axis=0, subset=['id', 'city', 'name', 'parameters'], inplace=True)


def main(json_data):
    df_transform = TransformData(json_data)
    df_transform.transform_json_to_df()
    df_transform.clean_dataframe()

    # Print or return the cleaned DataFrame
    print(df_transform.open_aq_df)

if __name__ == "__main__":
    main()   