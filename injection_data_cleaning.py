import os
import pandas as pd


path = '/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/injection_pre-cleaned/'

print("Running")

for filename in os.listdir(path):
    file = os.path.join(path, filename)

    df = pd.read_csv(file, low_memory=False)
    
    # drop empty rows
    df.dropna(how='all', inplace=True)

    # drop rows with negative injection volumes
    df.drop(df[df['Volume_BPD'] < 0].index, inplace=True)

    # convert columns with dates to datetime data type
    df.iloc[:, 5:8] = df.iloc[:, 5:8].apply(pd.to_datetime)

    # save cleaned file
    file_name = filename + "_cleaned"
    df.to_csv(f"/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/{file_name}.csv", index = False)
