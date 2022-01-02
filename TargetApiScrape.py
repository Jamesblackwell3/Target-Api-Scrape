from typing import List
from bs4 import BeautifulSoup
from pandas.core.frame import DataFrame
import requests
import pandas as pd
import numpy as np
import json

# Scraping Target api to create a database
stores = np.arange(0000, 4000, 1).tolist()
stores = [str(store).zfill(4) for store in stores]
# stores = ['2185', '0874'] #list for isolated store numbers
def get_store(store_keys: List):
    ef = pd.DataFrame()
    for store in store_keys:
        ef=ef
        print(store) # Used as a status to monitor where code is at while running
        keys = f"https://redsky.target.com/v3/stores/location/{store}?key=8df66ea1e1fc070a6ea99e942431c9cd67a80f02"
        result = requests.get(keys)
        if result.status_code == 404: continue # Handling 404 pages
        content = result.text
        soup = BeautifulSoup(content, 'lxml',)
        data_json = soup.find('p').getText()
        nested_data = json.loads(data_json)
# Turning nested data into a dot notation
        def nested(nested_data, prefix='Object'):
            new_json=[]
            def dotJson(nested_json, prefix="Object"):
                if isinstance(nested_json, dict):
                    for kwargs, json in nested_json.items():
                        p = "{}.{}".format(prefix, kwargs)
                        dotJson(json, p)
                elif isinstance(nested_json, list):
                    for kwargs, json in enumerate(nested_json):
                        kwargs = ''
                        p = "{}{}".format(prefix, kwargs)
                        dotJson(json, p)
                else:
                    new_json.append(['{}'.format(prefix), nested_json])
                return new_json
            dotJson(nested_data, prefix)
            return new_json
        dot_data = nested(nested_data, prefix='Store')

# Creating the dataframe from the scraped data
        df = pd.DataFrame(dot_data)
        df = df.set_index(0)
        df = df.T
# Filtering data to analyze
        filters = [
            'Store.location_id',
            'Store.physical_specifications.format',
            'Store.physical_specifications.merchandise_level',
            'Store.physical_specifications.total_building_area',
            'Store.sub_type_code',
            'Store.address.address_line1',
            'Store.address.city',
            'Store.address.state',
            'Store.address.region',
            'Store.milestones.milestone_date',
            'Store.status',
            'Store.drive_up.geofence.radius_in_meters',
            'Store.geographic_specifications.latitude',
            'Store.geographic_specifications.longitude'
        ]
# Handling exceptions where a column is missing or duplicated
        def select_filters(data:DataFrame, filter:List):
            for filters in filter:
                try:
                    data.loc[1, filters]
                except KeyError:
                    data.insert(0, filters, None)
                    continue
                else: continue
            return data
        df = select_filters(df, filters)
        df = df[filters]
        df = df.loc[:,~df.columns.duplicated()]
        ef = ef.append(df, ignore_index=True)
    return ef
df = get_store(stores)
print(df)
df.to_excel('Target.xlsx')
df.to_csv('Target.csv')
