import os
import numpy as np
import pandas as pd
#false positive warning, will not display
pd.options.mode.chained_assignment = None  # default='warn'

path = '/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/occ_daily_injection_cleaned/'

print("Running")

created_df = False
for filename in os.listdir(path):
    file = os.path.join(path, filename)

    daily = pd.read_csv(file, low_memory=False)
    
    # add api_year_month column
    # will be unique column to group APIs and return monthly volumes for each API
    
    # get rid of decimal places, some APIs are floats
    daily.API = daily.API.round(0).astype(int)
    daily["API_year_month"] = daily.API.astype(str).str[:] + "-" + daily.Report_Date.str[0:7]

    # create API_year column for unique identification for the export DF later
    daily["API_year"] = daily.API.astype(str).str[:] + "-" + daily.Report_Date.str[0:4]
    # daily

    # Get the monthly injection volumes for each well using the new unique API_year_month column

    # group by API-year-month
    daily_grouped = daily.groupby(["API_year_month"]).sum()
    # select just the volume
    daily_grouped = daily_grouped["Volume_BPD"]
    # convert to dictionary; will be searched later
    daily_grouped_dict = daily_grouped.to_dict()

    # create dataframe of just unique wells/API; will be the start of export dataframe
    # dataframe of one well per year
    daily_export = daily.drop_duplicates(subset="API")

    # Drop unnecessary columns from new DF
    daily_export.drop(columns = ["Daily_Report_Date_Start", "Daily_Report_Date_End", "Volume_BPD", 
        "Pressure_PSI", "DirArea", "Directive_Status", "API_year_month", "Report_Date"], inplace = True)

    # creating list of new columns for new DF
    new_col_list = "Jan_Vol, Feb_Vol, Mar_Vol, Apr_Vol, May_Vol, Jun_Vol, Jul_Vol, Aug_Vol, Sep_Vol, Oct_Vol, Nov_Vol, Dec_Vol, year"
    new_col_list = new_col_list.split(", ")

    # adding new colums to new DF to match monthly 
    for i in new_col_list:
        daily_export[i] = ''

    daily_export["year"] = daily_export.apply(
        lambda row: row.API_year[11:15], axis = 1)

    # define function to populate columns with monthly data

    monthly_volume_cols = ['Jan_Vol', 'Feb_Vol', 'Mar_Vol', 'Apr_Vol', 'May_Vol', 'Jun_Vol', 'Jul_Vol', 'Aug_Vol', 'Sep_Vol', 'Oct_Vol', 'Nov_Vol', 'Dec_Vol']

    month_dict = {"Jan_Vol": "01", "Feb_Vol": "02", "Mar_Vol": "03", "Apr_Vol": "04", 
                "May_Vol": "05", "Jun_Vol": "06", "Jul_Vol": "07", "Aug_Vol": "08", 
                "Sep_Vol": "09", "Oct_Vol": "10", "Nov_Vol": "11", "Dec_Vol": "12"}

    def populate_values(row):
        try:
            result = daily_grouped_dict[(str(row.API) + "-" + row.year+ "-" + month_dict[col])]
            return result
    #   if the key is not found in the dictionary containing values for each API_month, NaN is returned
        except:
            return np.nan

    # Apply above function to each column
    for col in monthly_volume_cols:
        daily_export[col] = daily_export.apply(populate_values, axis = 1)

    daily_export.fillna(0, inplace = True)

    #  Calcalate and add column for yearly volume
    daily_export["year_volume"] = daily_export.apply(
        lambda row: row.Jan_Vol + row.Feb_Vol + row.Mar_Vol + row.Apr_Vol + 
            row.May_Vol + row.Jun_Vol + row.Jul_Vol + row.Aug_Vol + row.Sep_Vol + 
            row.Oct_Vol + row.Nov_Vol + row.Dec_Vol, axis = 1)

    # Add data origin column (monthly vs daily); will be less obvious in the final master file
    daily_export["data_origin"] = "daily"

    # Add columns to match monthly df
    daily_export["County"] = ""
    daily_export["Total_Depth"] = ""
    daily_export["Formation_Name"] = "Arbuckle"

    # Reorder Columns
    daily_export =daily_export[['API', 'API_year', 'year', 'Operator_Name', 'Operator_Number',
        'Well_Name', 'Well_Number', 'Latitude', 'Longitude', 'County',
        'Total_Depth', 'Formation_Name', 'Jan_Vol', 'Feb_Vol', 'Mar_Vol',
        'Apr_Vol', 'May_Vol', 'Jun_Vol', 'Jul_Vol', 'Aug_Vol', 'Sep_Vol',
        'Oct_Vol', 'Nov_Vol', 'Dec_Vol', 'year_volume', 'data_origin']]

    if created_df == False:
        export_df = daily_export
        created_df = True
    
    elif created_df == True:
        export_df = export_df.append(daily_export, ignore_index = True)

# sort values
export_df.year = export_df.year.astype(int)
export_df.sort_values(by=['year'])


# clean and prep monthly data
monthly_all = pd.read_csv("/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/occ_monthly_cleaned/occ_monthly_cleaned.csv", low_memory=False)
# drop columns
monthly_all.drop(columns = ["JanPSI", "FebPSI", "MarPSI", "AprPSI", "MayPSI", "JunPSI", "JulPSI", "AugPSI", "SepPSI", "OctPSI", "NovPSI", "DecPSI"], inplace = True)
monthly_all.drop(columns = ["Township", "Range", "QTR4", "QTR3", "QTR2", "QTR1", "PM", "FluidType"], inplace=True)
monthly_all.drop(columns = ["ProdReportType","CommissionOrderNo", "Status", "WellStatus", "WellType"], inplace = True)
monthly_all.drop(columns = ["Section", "ModifyDate", "Packer", "PackerDepth", "ReportYear", "MeasurementType"], inplace = True)
monthly_all.drop(columns = ["LastMITDate", "InjTopDepth", "InjBotDepth", "PlugBackTotalDepth"], inplace = True)
final_export = export_df.append(monthly_all, ignore_index = True)

# Renaming column for consistency, multiple lines for readability

monthly_all.rename(columns={"OperatorName": "Operator_Name", "WellName": "Well_Name", "WellNumber":"Well_Number"}, inplace = True)
monthly_all.rename(columns={"LAT": "Latitude", "LON": "Longitude", "FormationName":"Formation_Name"}, inplace = True)
monthly_all.rename(columns={"JanVol": "Jan_Vol", "FebVol": "Feb_Vol", "MarVol":"Mar_Vol", "AprVol":"Apr_Vol"}, inplace = True)
monthly_all.rename(columns={"MayVol": "May_Vol", "JunVol": "Jun_Vol", "JulVol":"Jul_Vol", "AugVol":"Aug_Vol"}, inplace = True)
monthly_all.rename(columns={"SepVol": "Sep_Vol", "OctVol": "Oct_Vol", "NovVol":"Nov_Vol", "DecVol":"Dec_Vol"}, inplace = True)
monthly_all.rename(columns={"TotalDepth": "Total_Depth"}, inplace = True)

# Add data origin column (monthly vs daily); will be less obvious in the final master file
monthly_all["data_origin"] = "monthly"

# Add columns to match daily export df
monthly_all["Operator_Number"] = ""

# Reorder Columns
monthly_all = monthly_all[['API', 'API_year', 'year', 'Operator_Name', 'Operator_Number',
    'Well_Name', 'Well_Number', 'Latitude', 'Longitude', 'County',
    'Total_Depth', 'Formation_Name', 'Jan_Vol', 'Feb_Vol', 'Mar_Vol',
    'Apr_Vol', 'May_Vol', 'Jun_Vol', 'Jul_Vol', 'Aug_Vol', 'Sep_Vol',
    'Oct_Vol', 'Nov_Vol', 'Dec_Vol', 'year_volume', 'data_origin']]


final_export = export_df.append(monthly_all, ignore_index = True)

# save cleaned file
file_name =  "daily_monthly_combined"
final_export.to_csv(f"/Users/curtissmith/Projects/oklahoma_earthquakes_largefiles/python_exports/{file_name}.csv", index = False)

print("Finished")