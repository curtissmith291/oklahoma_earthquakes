import os
import pandas as pd

# reduces monthly data into reduces csv for Pandas

# path = '/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/ooc_monthly_pre/'

print("Running")

month_list = ["Jan_Vol", "Feb_Vol", "Mar_Vol", "Apr_Vol", "May_Vol", "Jun_Vol", 
    "Jul_Vol", "Aug_Vol", "Sep_Vol", "Oct_Vol", "Nov_Vol", "Dec_Vol"
]

month_dates = {'Jan_Vol': '01-31', 'Feb_Vol': '02-28', 'Mar_Vol': '03-31', 'Apr_Vol': '04-30', 
        'May_Vol': '05-31', 'Jun_Vol': '06-30', 'Jul_Vol': '07-31', 'Aug_Vol': '08-31', 
        'Sep_Vol': '09-30', 'Oct_Vol': '10-31', 'Nov_Vol': '11-30', 'Dec_Vol': '12-31'
}


import_list = []

file = "/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/df_filtered_monthly.csv"

try:

    # open file
    df = pd.read_csv(file, low_memory=False)

    for year in df.year.unique():
        df_temp = df[df.year == year]
        for item in month_list:
            temp_dict = {
                "date": f'{year}-{month_dates[item]}',
                "monthly_volume": df_temp[item].sum(),
                "well_count": df_temp[item].count(),
            }
            import_list.append(temp_dict)
except:
    print("uh oh")

# print(import_list)
df_export = pd.DataFrame(import_list)
df_export.to_csv("/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/df_filtered_monthly_reduced.csv", index = False)
print("Finished")