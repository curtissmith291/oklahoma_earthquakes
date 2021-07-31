import os
import pandas as pd

path = '/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/injection_cleaned/'

print("Running")

created_df = False
for filename in os.listdir(path):
    file = os.path.join(path, filename)

    df = pd.read_csv(file, low_memory=False)

    df = df[["Report_Date", "Volume_BPD"]]

    if created_df == False:
        export_df = df
        created_df = True
    
    elif created_df == True:
        export_df = export_df.append(df, True)


# save cleaned file
file_name =  "weekly_volume"
export_df.to_csv(f"/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/{file_name}.csv", index = False)

print("finished")