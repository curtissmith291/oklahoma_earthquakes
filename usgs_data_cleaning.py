import json
import os
import pandas as pd

# This script parses the usgs GeoJSON files and returns a cleaned GeoJson and a reduced CSV file for Pandas

# will export a geo_json for maps later
features_geojson = []
# will create list of dicts for import into pandas
pandas_import_list = []
count = 0
# list of error ids
errors = []

path = '/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/usgs_pre-cleaned/'

print("Running")

for filename in os.listdir(path):
    file = os.path.join(path, filename)
    # print(file)

    try:

        with open(file) as data_import:
                data = json.loads(data_import.read())

        for item in data['features']:
            temp_dict = {}
            try:
                if "Oklahoma" in item['properties']['place']:
                    count += 1
                    # Add dictionary to export GeoJson
                    features_geojson.append(item)
                    temp_dict = {
                        "id": item['id'],
                        "time": item['properties']['time'],
                        "mag": item['properties']['mag'],
                        "magType": item['properties']['magType'],
                        "cdi": item['properties']['cdi'],
                        "place": item['properties']['place'],
                        "status": item['properties']['status'],
                        "latitude": item['geometry']['coordinates'][1],
                        "longitude": item['geometry']['coordinates'][0],
                        "depth": item['geometry']['coordinates'][2],
                    }
                    # append selected items to 
                    pandas_import_list.append(temp_dict)

            except:
                errors.append(item['id'])
                # errors_properties.append[item['properties']]

    except: 
        continue
# print(count)
# print(errors)
# print(len(errors))

# print(len(features))

# print(data.keys())

df = pd.DataFrame(pandas_import_list)
df['date'] = pd.to_datetime(df['time'],unit='ms')

df.to_csv("/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/usgs_eqs_reduced_data.csv", index = False)


# update the count of features in the usgs geojson
data["metadata"]["count"] = count

usgs_json = {
    "type": data["type"],
    "metadata": data["metadata"],
    "features": features_geojson,
    "bbox": data["bbox"]
}

file_name = "usgs_all_eq_data"

# with open(f'/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/{file_name}.json', 'w') as outfile:
#     json.dump(usgs_json, outfile, indent = 4)

print("Complete")