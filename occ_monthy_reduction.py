import os
import pandas as pd

# reduces monthly data into reduces csv for Pandas

path = '/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/ooc_monthly_pre/'

print("Running")

month_list = ["Jan Vol", "Feb Vol", "Mar Vol", "Apr Vol", "May Vol", "Jun Vol", 
    "Jul Vol", "Aug Vol", "Sep Vol", "Oct Vol", "Nov Vol", "Dec Vol"
]

month_dates = {'Jan Vol': '01-31', 'Feb Vol': '02-28', 'Mar Vol': '03-31', 'Apr Vol': '04-30', 
        'May Vol': '05-31', 'Jun Vol': '06-30', 'Jul Vol': '07-31', 'Aug Vol': '08-31', 
        'Sep Vol': '09-30', 'Oct Vol': '10-31', 'Nov Vol': '11-30', 'Dec Vol': '12-31'
}

import_list = []

created_df = False

for filename in os.listdir(path):
    try:
        file = os.path.join(path, filename)

        # open file
        df = pd.read_csv(file, low_memory=False)

        # select only Arbuckle rows
        df= df[df["FormationName"] == "ARBUCKLE"]

        year = filename[0:4]

        for item in month_list:
            temp_dict = {
                "date": f'{year}-{month_dates[item]}',
                "monthly_volume": df[item].sum(),
                "count": df[item].count(),
                "count_>0": df[df[item] > 0][item].count()
            }
            import_list.append(temp_dict)
    except:
        continue

# print(import_list)
df_export = pd.DataFrame(import_list)
df_export.to_csv("/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/occ_monthly_reduced.csv", index = False)
print("finished")