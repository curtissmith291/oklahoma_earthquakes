import os
import pandas as pd

# this code cleans the monthly OCC data to prepare for import into database

path = '/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/ooc_monthly_pre/'

print("Running")

created_df = False
for filename in os.listdir(path):
    file = os.path.join(path, filename)
    try:
        df = pd.read_csv(file, low_memory=False, encoding='utf8')
        
        # drop empty rows
        df.dropna(how='all', inplace=True)

        # drop duplicates
        df.drop_duplicates(subset=['API'], inplace=True)

        # get rid of spaces in column names
        df.columns = df.columns.str.replace(' ', '')

        # replace nulls with blanks
        df.replace("NULL", "")

        df['year_volume'] = df.JanVol + df.FebVol + df.MarVol + df.AprVol + df.MayVol + df.JunVol + df.JulVol + df.AugVol + df.SepVol + df.OctVol + df.NovVol + df.DecVol

        # create new column to be used as primary key
        year = filename[0:4]
        # print(df.API.astype(str) + "-" + year)
        df['API_year'] = df.API.astype(str) + "-" + year

        df['year'] = year

        if created_df == False:
            export_df = df
            created_df = True
        
        elif created_df == True:
            export_df = export_df.append(df, True)
    except:
        continue

file_name =  "db_import"
export_df.to_csv(f"/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/{file_name}.csv", index = False)

print("Finished")