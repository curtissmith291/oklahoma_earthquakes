import json
import os

with open('/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/usgs/eq_2009-2014.json') as file:
        data = json.loads(file.read())

print(len(data['features']))
# print(type(data['features']))
# print(data['features'][158]['properties']['place'])
# if "Oklahoma" in data['features'][158]['properties']['place']:
#     print(True)

# will export a geo_json for maps later
features_geojson = []
# will create list of dicts for import into pandas
pandas_import_list = []
count = 0
# list of error ids
errors = []

for item in data['features']:
    temp_dict = {}
    try:
        if "Oklahoma" in item['properties']['place']:
            count += 1
            # Add dictionary to export GeoJson
            features_geojson.append(item)
            temp_dict = {
                "id": item['id'],
                "mag": item['properties']['mag'],
                "magType": item['properties']['magType'],
                "cdi": item['properties']['cdi'],
                "place": item['properties']['place'],
                "status": item['properties']['status'],
                "latitude": item['geometry']['coordinates'][1],
                "longitude": item['geometry']['coordinates'][1],
                "depth": item['geometry']['coordinates'][1],
            }
            # append selected items to 
            pandas_import_list.append(temp_dict)

    except:
        errors.append(item['id'])
        # errors_properties.append[item['properties']]

# print(count)
# print(errors)
print(len(errors))

# print(len(features))

# print(data.keys())

data["metadata"]["count"] = count

usgs_json = {
    "type": data["type"],
    "metadata": data["metadata"],
    "features": features_geojson,
    "bbox": data["bbox"]
}