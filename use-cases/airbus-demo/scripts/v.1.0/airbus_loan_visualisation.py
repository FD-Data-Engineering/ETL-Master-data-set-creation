import numpy as np 
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, to_timestamp, split, explode, sum, count, row_number, desc, expr, when
from pyspark.sql.types import StructType,TimestampType, StringType, IntegerType, DoubleType
from datetime import datetime
import ibm_boto3
from ibm_botocore.client import Config, ClientError
import io
from functools import reduce
import sys
import folium
from folium import plugins
from folium.plugins import Search, HeatMap
from glob import glob
from datetime import datetime
import math

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

log4j_logger = spark._jvm.org.apache.log4j  # noqa
logger = log4j_logger.LogManager.getRootLogger()

dtStr = datetime.today().strftime('%Y%m%d')

####################################
# Get COS config
####################################
def getCOSconfig():
  COS_ENDPOINT = "https://s3.direct.eu-de.cloud-object-storage.appdomain.cloud" # Current list avaiable at https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints
  COS_API_KEY_ID = "lDN2rL5N-52OZANVY3pjc1lbXqhAI1KGHNMj8IgBP9PV" # eg "W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk"
  COS_INSTANCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/f2d7386c3c18406b9e2eed413aa7d007:629d310c-63f6-474a-826c-323c2af3d861::" # eg "crn:v1:bluemix:public:cloud-object-storage:global:a/f2d7386c3c18406b9e2eed413aa7d007:629d310c-63f6-474a-826c-323c2af3d861::"
  auth_endpoint = 'https://iam.bluemix.net/oidc/token'
  # Create resource
  cos = ibm_boto3.resource("s3",
      ibm_api_key_id=COS_API_KEY_ID,
      ibm_service_instance_id=COS_INSTANCE_CRN,
      ibm_auth_endpoint=auth_endpoint,
      config=Config(signature_version="oauth"),
      endpoint_url=COS_ENDPOINT
  )
  return cos
  
def getBucketContents(cos,bucket_name,filterArgs):
    itemList = []
    print("Retrieving bucket contents from:{0}".format(bucket_name))
    try:
        arr = []
        files = cos.Bucket(bucket_name).objects.all()
        for file in files:
            arr.append("{0}".format(file.key, file.size))
    
        itemList = [item for item in arr if all(filters in item for filters in filterArgs)]
    
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to retrieve bucket contents: {0}".format(e))
    return itemList

def getCSVFromBucket(cos,bucket_name,itemList):
    df_dict = {}
    for item in itemList:
        length = len(item)
        name = item[:length-4] 
        print("{0} : {1}".format(name,length))
        #using the substring before the .csv extension as the name
        key = name
        file = cos.Object(bucket_name, item)
        csvFile = file.get()
        stream = io.StringIO(csvFile["Body"].read().decode('utf-8'))
        df = pd.read_csv(stream)
        df_dict[key] = df
    return df_dict

def getCSVdf(dataDict):
  csvList = []  
  for csv in dataDict.values():
    csvList.append(csv)
  frame = pd.concat(csvList, axis=0, ignore_index=True)
  return frame

####################################
# Parameters
####################################
dtStr = datetime.today().strftime('%Y%m%d')
cos = getCOSconfig()
itemList = getBucketContents(cos,"publishedairbusdata",[dtStr,".csv"])
print(itemList)
dataDict =  getCSVFromBucket(cos,"publishedairbusdata",itemList)

logger.info("######################################")
logger.info("READING INPUT FILE")
logger.debug("Enriched Data :: "+str(dataDict))
logger.info("######################################")

####################################
# Read CSV Data
####################################
logger.info("######################################")
logger.info("READING Enriched CSV DATA ")
logger.info("######################################")

df_nationwide_loanbook =  getCSVdf(dataDict)

#df_nationwide_loanbook_ICB = df_nationwide_loanbook.withColumn("INITIAL_BORROW_CAPITAL",col("currentloan") * 1/ col("currentltv"))
vars_ln=['currentloan','currentltv']
cuurlnamt='currentloan'
currltv='currentltv'
df_nationwide_loanbook['INITIAL_BORROW_CAPITAL'] = df_nationwide_loanbook[vars_ln].apply(lambda x :  x[cuurlnamt]*1/x[currltv] ,axis=1)

# read published airbus-loan data
print("######################################")
print("READING PUBLISHED AIRBUS-LOAN DATA FILE")
print("######################################")

#Bristol co-ordinates
lat= 51.4467729
lang= -2.5985602

#Color codes for BER rating scale
colors_epc={
    np.nan:'#deebf6',
    'A':'#00a54f',
    'B':'#4cb848',
    'C':'#bed630',
    'D':'#fff101',
    'E':'#fcb814',
    'F':'#f36e21',
    'G':'#ee1d23',
}

#Color codes for flood rating scale
colors_flood={
    'no-colour':'#deebf6',
    'green':'#a5d45d',
    'amber':'#ecc63a',
    'red':'#d92424',
    'black-1':'#919191',
    'black-2':'#242424',
    'purple':'#9370db',
}

# Function to convert number into string
# Switcher is dictionary data type here
### Planned for combinedscore/combinedscore_adj -  Combined Floodability undefended/defended - score
def FloodRatingSwitch(undefend_sum,defend_sum):
    value_search = undefend_sum if (undefend_sum > defend_sum) else defend_sum
    # print('value_search_='+str(value_search))
    list_no_colour = [0]
    list_green = [1,2,3,4,5,6,7,8,9,10]
    list_amber = [11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29]
    list_red = [30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45]
    list_black1 = [45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60]
    list_black2 = [61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100]
    # Evaluate from List
    if value_search in list_no_colour:
        return 'no-colour'
    elif value_search in list_green:
        return 'green'
    elif value_search in list_amber:
        return 'amber'
    elif value_search in list_red:
        return 'red'
    elif value_search in list_black1:
        return 'black-1'
    elif value_search in list_black2:
        return 'black-2'
    else:
        return 'purple' #ERROR out-of-bounds
    
    # get() method of dictionary data type returns
    # value of passed argument if it is present
    # in dictionary otherwise second argument will
    # be assigned as default value of passed argument
    # return switcher.get(value_search, 'purple') # 'purple':'#9370db', ERROR
    
## Function to Define Ratio Value between 0 ( 0.001) and 100 as %
def FloodRating_Ration_0_100_Value(undefend_sum,defend_sum):
    value_search = undefend_sum if (undefend_sum > defend_sum) else defend_sum
    # augment in 40% for colour grading on map
    ratio_0_100 = (value_search/100)*1.40 + 0.001
    ratio_0_100 = round( ratio_0_100, 4)
    return ratio_0_100

# Creating the popup labels for energy rating data
def popup_html_epc(row):
    i = row
    uprn_id= df_nationwide_loanbook['uprn'].iloc[i] 
    address= 'Property_type='+str(df_nationwide_loanbook['property_type'].iloc[i])+'. '+str(df_nationwide_loanbook['built_form'].iloc[i])+'. '+  str( df_nationwide_loanbook['postcode_flag'].iloc[i] ) +'. Easting/Norting='+ str( df_nationwide_loanbook['easting'].iloc[i] ) +'/'+ str( df_nationwide_loanbook['northing'].iloc[i] ) +'. Latitude/Longitude='+ str( df_nationwide_loanbook['latitude'].iloc[i] ) +'/'+ str( df_nationwide_loanbook['longitude'].iloc[i] )
    Epc_Rating = df_nationwide_loanbook['current_energy_rating'].iloc[i]
    Contruction_Age = df_nationwide_loanbook['construction_age_band'].iloc[i]
    Glazed_Type = df_nationwide_loanbook['glazed_type'].iloc[i]
    Epc_Energy_Cons_Kwh = df_nationwide_loanbook['energy_consumption_current'].iloc[i]
    Co2_Emission = df_nationwide_loanbook['co2_emissions_current'].iloc[i]
    
    Hotwater = df_nationwide_loanbook['hotwater_description'].iloc[i]
    Hotwater_energy_eff = df_nationwide_loanbook['hot_water_energy_eff'].iloc[i]
    Hotwater_env_eff = df_nationwide_loanbook['hot_water_env_eff'].iloc[i]

    Floor = df_nationwide_loanbook['floor_description'].iloc[i]
    Floor_energy_eff = df_nationwide_loanbook['floor_energy_eff'].iloc[i]
    Floor_env_eff = df_nationwide_loanbook['floor_env_eff'].iloc[i]

    Windows = df_nationwide_loanbook['windows_description'].iloc[i]
    Windows_energy_eff = df_nationwide_loanbook['windows_energy_eff'].iloc[i]
    Windows_env_eff = df_nationwide_loanbook['windows_env_eff'].iloc[i]

    Walls = df_nationwide_loanbook['walls_description'].iloc[i]
    Walls_energy_eff = df_nationwide_loanbook['walls_energy_eff'].iloc[i]
    Walls_env_eff = df_nationwide_loanbook['walls_env_eff'].iloc[i]

    Roof = df_nationwide_loanbook['roof_description'].iloc[i]
    Roof_energy_eff = df_nationwide_loanbook['roof_energy_eff'].iloc[i]
    Roof_env_eff = df_nationwide_loanbook['roof_env_eff'].iloc[i]

    left_col_color = "#808080"
    right_col_color = "#dcdcdc"
    
    html = """
    <!DOCTYPE html>
    <html>
    <center><h4 style="margin-bottom:5"; width="200px">UPRN ID:{}</h4>""".format(uprn_id) + """</center>
    <center> <table style="height: 90px; width: 600px;">
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Property </span></td>
    <td style="border: 1px solid white; width: 200px;background-color: """+ right_col_color +""";">"""+ address + """</td>
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Epc Rating </span></td>
    <td style="border: 1px solid white; width: 200px;background-color: """+ right_col_color +""";">{}</td>""".format(Epc_Rating) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Epc Energy Consumption Kwh </span></td>
    <td style="border: 1px solid white; width: 200px;background-color: """+ right_col_color +""";">{} kWh/m\u00b2/year</td>""".format(Epc_Energy_Cons_Kwh) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">CO\u2082 Emission Current</span></td>
    <td style="border: 1px solid white; width: 200px;background-color: """+ right_col_color +""";">{}</td>""".format(Co2_Emission) + """
    </tr>
    </tbody>
    </table></center>
    
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Construction and Glazing</h5>
    <center><table style="height: 50px; width: 600px;">
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Contruction Age</span></td>
    <td style="border: 1px solid white; width: 200px;background-color: """+ right_col_color +""";">{}</td>""".format(Contruction_Age) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Glazing</span></td>
    <td style="border: 1px solid white; width: 200px;background-color: """+ right_col_color +""";">{}</td>""".format(Glazed_Type) + """
    </tr>
    </tbody>
    </table></center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold"> EPC Energy Rating and Consumption</h5>
    <center> <table style="height: 50px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y Current </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y Potential </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> EPC Energy Rating and Consumption </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} ({} kWh/m\u00b2/year)</td>""".format(df_nationwide_loanbook['current_energy_rating'].iloc[i],df_nationwide_loanbook['energy_consumption_current'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} ({} kWh/m\u00b2/year)</td>""".format(df_nationwide_loanbook['potential_energy_rating'].iloc[i],df_nationwide_loanbook['energy_consumption_potential'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold"> Property CO\u2082 Emissions</h5>
    <center> <table style="height: 50px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y Current </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y Potential </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> CO\u2082 Emissions </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} CO\u2082 Tonnes/year </td>""".format(df_nationwide_loanbook['co2_emissions_current'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} CO\u2082 Tonnes/year </td>""".format(df_nationwide_loanbook['co2_emissions_potential'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold"> Energy Efficiency</h5>
    <center> <table style="height: 50px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y Current </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y Potential </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style= "border: 1px solid white; width=100px; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Energy Efficiency </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} Based on cost of energy, i.e. heating, water and lighting [in kWh/year] mult. by fuel costs. (£/m²/year cost is derived from kWh). </td>""".format(df_nationwide_loanbook['current_energy_efficiency'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} Based on cost of energy, i.e. heating, water and lighting [in kWh/year] mult. by fuel costs. (£/m²/year cost is derived from kWh). </td>""".format(df_nationwide_loanbook['potential_energy_efficiency'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Hotwater, Floor, Windows, Wall Efficiency Information</h5>
    <center> <table style="height: 50px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Decription </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Energy Efficiency</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Environment Efficiency</span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Hotwater</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(Hotwater) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Hotwater_energy_eff) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Hotwater_env_eff) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Floor</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(Floor) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Floor_energy_eff) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Floor_env_eff) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Windows</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(Windows) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Windows_energy_eff) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Windows_env_eff) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Walls</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(Walls) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Walls_energy_eff) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Walls_env_eff) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Roof</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(Roof) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Roof_energy_eff) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(Roof_env_eff) + """
    </tr>
    </tbody>
    </table> </center>
    
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Loan Details</h5>
    <center> <table style="height: 50px; width:600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">UPRN </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current Loan Balance</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current Loan to Value LTV</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Initial Borrow Capital</span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_nationwide_loanbook['uprn'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">£{}</td>""".format(df_nationwide_loanbook['currentloan'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}%</td>""".format(round(100*df_nationwide_loanbook['currentltv'].iloc[i],4)) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">£{}</td>""".format(round(df_nationwide_loanbook['INITIAL_BORROW_CAPITAL'].iloc[i],2)) + """
    </tr>
    </tbody>
    </table> </center>
    
    
    </html>
"""
    return html
# display COMB_RCP26_2050_UD - Climate Change Combined Flood Score (defended) for RCP 2.6 for 2050

column_list = ['sop_river_value']
for column_name in column_list:
      print(df_nationwide_loanbook[column_name].unique())
    
# Creating the popup labels for flood rating data
def popup_html_flood(row):
    i = row
    uprn_id=df_nationwide_loanbook['uprn'].iloc[i] 
    address= 'Property_type='+str(df_nationwide_loanbook['property_type'].iloc[i])+'. '+str(df_nationwide_loanbook['built_form'].iloc[i])+'. '+  str( df_nationwide_loanbook['postcode_flag'].iloc[i] ) +'. Easting/Norting='+ str( df_nationwide_loanbook['easting'].iloc[i] ) +'/'+ str( df_nationwide_loanbook['northing'].iloc[i] ) +'. Latitude/Longitude='+ str( df_nationwide_loanbook['latitude'].iloc[i] ) +'/'+ str( df_nationwide_loanbook['longitude'].iloc[i] )
    river_flooding_first = df_nationwide_loanbook['rhighscore'].iloc[i]
    coastal_flooding_first = df_nationwide_loanbook['chighscore'].iloc[i]
    surf_water_flooding_first = df_nationwide_loanbook['swhighscore'].iloc[i]
    combined_scoring_flooding_first = df_nationwide_loanbook['combinedscore'].iloc[i]
    river_value = df_nationwide_loanbook['sop_river_value'].iloc[i]
    water_surface_value = df_nationwide_loanbook['sop_surface_water_value'].iloc[i]
    canal_failure = df_nationwide_loanbook['canal_failure_value'].iloc[i]
    dam_break = df_nationwide_loanbook['dam_break_value'].iloc[i]
    ground_height = df_nationwide_loanbook['unflood_value'].iloc[i]
    peat_surface = df_nationwide_loanbook['npd_peat'].iloc[i]
    sand_soil = df_nationwide_loanbook['npd_sand'].iloc[i]
    soft_soil = df_nationwide_loanbook['npd_soft'].iloc[i]
    silt_soil = df_nationwide_loanbook['npd_silt'].iloc[i]
    
    
    left_col_color = "#808080"
    right_col_color = "#dcdcdc"
    
    html = """
    <!DOCTYPE html>
    <html>
    <center><h4 style="margin-bottom:5"; width="200px">UPRN ID:{}</h4>""".format(uprn_id) + """</center>
    <center> <table style="height: 90px; width: 500px;">
    <tbody>
    <tr>
    <td style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Property </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">"""+ address + """</td>
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Highest score from the Defended river scores  </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(river_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Highest score from the Defended coastal scores </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(coastal_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Highest score from the Defended surface water scores </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(surf_water_flooding_first) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Combined Sum of all Scores - Defended River, Coastal and Surface Water</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(combined_scoring_flooding_first) + """
    </tr>
    </tbody>
    </table>
    </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Loan Details</h5>
    <center> <table style="height: 50px; width:600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">UPRN </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current Loan Balance</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current Loan to Value LTV</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Initial Borrow Capital</span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_nationwide_loanbook['uprn'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">£{}</td>""".format(df_nationwide_loanbook['currentloan'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}%</td>""".format(round(100*df_nationwide_loanbook['currentltv'].iloc[i],4)) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">£{}</td>""".format(round(df_nationwide_loanbook['INITIAL_BORROW_CAPITAL'].iloc[i],2)) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Floodability score</h5>
    <center> <table style="height: 50px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Floodability Type </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y defended </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y undefended </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">River</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['rhighscore'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['rhighscore_adj'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Coastal</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['chighscore'].iloc[i])  + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['chighscore_adj'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Surface Water</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['swhighscore'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['swhighscore_adj'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Overall score</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";"><b>{} </b></td>""".format(df_nationwide_loanbook['combinedscore'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";"><b>{} </b></td>""".format(df_nationwide_loanbook['combinedscore_adj'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Ground Water</h5>
    <center> <table style="height: 50px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"></span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y highest </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Highest score from the ground water scores </span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_nationwide_loanbook['gwhighscore'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Dam and Canal Failure</h5>
    <center> <table style="height: 50px; width: 600px;">
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Risk to flood following canal failure </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(canal_failure) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Risk to flood following dam break </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(dam_break) + """
    </tr>
    </tbody>
    </table> </center>
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Ground Height Information</h5>
    <center> <table style="height: 50px; width: 600px;">
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> How high the ground is above flooding </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(ground_height) + """
    </tr>
    </tbody>
    </table> </center>
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Fixed defences and standard of protection</h5>
    <center> <table style="height: 50px; width: 600px;">
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Any fixed river flood defences and the standard of protection identified </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(river_value) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Any fixed surface water flood defences and the standard of protection identified </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(water_surface_value) + """
    </tr>
    </tbody>
    </table> </center>
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Soil Related Information</h5>
    <center> <table style="height: 50px; width: 600px;">
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Risk of peat relted subsidence </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(peat_surface) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Risk of sand related subsidence </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(sand_soil) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Risk of silt related subsidence </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(silt_soil) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Risk of soft soil related subsidence </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}</td>""".format(soft_soil) + """
    </tr>
    </tbody>
    </table> </center>
    </html>
""" 
    return html
    
# Creating the popup labels for airbus and loan data
def popup_html_index(row):
    i = row
    uprn_id=df_nationwide_loanbook['uprn'].iloc[i] 
    address= 'Property_type='+str(df_nationwide_loanbook['property_type'].iloc[i])+'. '+str(df_nationwide_loanbook['built_form'].iloc[i])+'. '+  str( df_nationwide_loanbook['postcode_flag'].iloc[i] ) +'. Easting/Norting='+ str( df_nationwide_loanbook['easting'].iloc[i] ) +'/'+ str( df_nationwide_loanbook['northing'].iloc[i] ) +'. Latitude/Longitude='+ str( df_nationwide_loanbook['latitude'].iloc[i] ) +'/'+ str( df_nationwide_loanbook['longitude'].iloc[i] )
    river_flooding_first = df_nationwide_loanbook['rhighscore'].iloc[i]
    coastal_flooding_first = df_nationwide_loanbook['rhighscore'].iloc[i] 
    surf_water_flooding_first = df_nationwide_loanbook['rhighscore'].iloc[i]
    
    left_col_color = "#808080"
    right_col_color = "#dcdcdc"
    
    html = """
    <!DOCTYPE html>
    <html>
    <center><h4 style="margin-bottom:5"; width="200px"> UPRN :{}</h4>""".format(uprn_id) + """</center>
    <center> <table style="height: 90px; width: 600px;">
    <tbody>
    <tr>
    <td style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Property </span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">"""+ address + """</td>
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current EPC Energy Rating and Consumption</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{} ({} kWh/m\u00b2/year)</td>""".format(df_nationwide_loanbook['current_energy_rating'].iloc[i],df_nationwide_loanbook['energy_consumption_current'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Potential EPC Energy Rating and Consumption</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{} ({} kWh/m\u00b2/year)</td>""".format(df_nationwide_loanbook['potential_energy_rating'].iloc[i],df_nationwide_loanbook['energy_consumption_potential'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current CO\u2082 Emissions</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{} CO\u2082 Tonnes/year </td>""".format(df_nationwide_loanbook['co2_emissions_current'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Potential CO\u2082 Emissions</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{} CO\u2082 Tonnes/year </td>""".format(df_nationwide_loanbook['co2_emissions_potential'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current Energy Efficiency</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}% Based on cost of energy, i.e. heating, water and lighting [in kWh/year] mult. by fuel costs. (£/m²/year cost is derived from kWh). </td>""".format(df_nationwide_loanbook['current_energy_efficiency'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Potential Energy Efficiency</span></td>
    <td style="border: 1px solid white; width: 150px;background-color: """+ right_col_color +""";">{}% Based on cost of energy, i.e. heating, water and lighting [in kWh/year] mult. by fuel costs. (£/m²/year cost is derived from kWh). </td>""".format(df_nationwide_loanbook['potential_energy_efficiency'].iloc[i]) + """
    </tr>
    </tbody>
    </table>
    </center>
    
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Loan Details</h5>
    <center> <table style="height: 50px; width:600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">UPRN </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current Loan Balance</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Current Loan to Value LTV</span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Initial Borrow Capital</span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}</td>""".format(df_nationwide_loanbook['uprn'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">£{}</td>""".format(df_nationwide_loanbook['currentloan'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{}%</td>""".format(round(100*df_nationwide_loanbook['currentltv'].iloc[i],4)) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">£{}</td>""".format(round(df_nationwide_loanbook['INITIAL_BORROW_CAPITAL'].iloc[i],2)) + """
    </tr>
    </tbody>
    </table> </center>
    
    <h5 style="margin-bottom:5px; margin-left:5px; font-weight: bold">Floodability score</h5>
    <center> <table style="height: 50px; width: 600px;">
    <thead>
    <tr>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> Floodability Type </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y defended </span></th>
    <th style= "border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;"> 2023y undefended </span></th>
    </tr>
    </thead>
    <tbody>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">River</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['rhighscore'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['rhighscore_adj'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Coastal</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['chighscore'].iloc[i])  + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['chighscore_adj'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Surface Water</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['swhighscore'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";">{} </td>""".format(df_nationwide_loanbook['swhighscore_adj'].iloc[i]) + """
    </tr>
    <tr>
    <td style="border: 1px solid white; background-color: """+ left_col_color +""";"><span style="color: #ffffff;">Overall score</span></td>
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";"><b>{} </b></td>""".format(df_nationwide_loanbook['combinedscore'].iloc[i]) + """
    <td style="border: 1px solid white; background-color: """+ right_col_color +""";"><b>{} </b></td>""".format(df_nationwide_loanbook['combinedscore_adj'].iloc[i]) + """
    </tr>
    </tbody>
    </table> </center>
    
    </html>
""" 
    return html
    
#Creating lagends
def add_legend(maps, title, colors, labels):
    if len(colors) != len(labels):
        raise ValueError("colors and labels must have the same length.")

    color_label = dict(zip(labels, colors))
    
    legends = ""     
    for label, color in color_label.items():
        legends += f"<li><span style='background:{color}'></span>{label}</li>"
        
    legend_html = f"""
    <div id='legend' class='legend'>
      <div class='legend-title'>{title}</div>
      <div class='legend-scale'>
        <ul class='legend-labels'>
        {legends}
        </ul>
      </div>
    </div>
    """
    script = f"""
        <script type="text/javascript">
        var Layout = (function() {{
                    var flag = false;
                    return function() {{
                        if (!flag) {{
                             var checkExist = setInterval(function() {{
                                       if ((document.getElementsByClassName('leaflet-top leaflet-right').length) || (!flag)) {{
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].style.display = "flex"
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].style.flexDirection = "column"
                                          document.getElementsByClassName('leaflet-top leaflet-right')[0].innerHTML += `{legend_html}`;
                                          clearInterval(checkExist);
                                          flag = true;
                                       }}
                                    }}, 100);
                        }}
                    }};
                }})();
        Layout()
        </script>
      """

    css = """

    <style type='text/css'>
      .legend {
        z-index:9999;
        float:right;
        background-color: rgb(255, 255, 255);
        border-radius: 5px;
        border: 2px solid #bbb;
        padding: 10px;
        font-size:12px;
        positon: relative;
      }
      .legend .legend-title {
        text-align: left;
        margin-bottom: 5px;
        font-weight: bold;
        font-size: 90%;
        }
      .legend .legend-scale ul {
        margin: 0;
        margin-bottom: 5px;
        padding: 0;
        float: left;
        list-style: none;
        }
      .legend .legend-scale ul li {
        font-size: 80%;
        list-style: none;
        margin-left: 0;
        line-height: 18px;
        margin-bottom: 2px;
        }
      .legend ul.legend-labels li span {
        display: block;
        float: left;
        height: 16px;
        width: 30px;
        margin-right: 5px;
        margin-left: 0;
        border: 0px solid #ccc;
        }
      .legend .legend-source {
        font-size: 80%;
        color: #777;
        clear: both;
        }
      .legend a {
        color: #777;
        }
    </style>
    """

    maps.get_root().header.add_child(folium.Element(script + css))

    return maps
 
#Plotting EPC RATING data
print("\n------")
print("GENERATING EPC RATING MAP")
print("------")

bristol_epc=folium.Map(location=[lat,lang],zoom_start=14)
for d in df_nationwide_loanbook.iterrows():
        html = popup_html_epc(d[0])
        popup = folium.Popup(folium.Html(html, script=True), max_width=500)
        folium.CircleMarker(
                    [d[1]['latitude'], d[1]['longitude']],
                    radius=6,
                    color=colors_epc[d[1]["current_energy_rating"]],
                    fill=True,
                    fill_color=colors_epc[d[1]["current_energy_rating"]],
                    fill_opacity=0.7,
                    popup=popup
            ).add_to(bristol_epc)
           
bristol_epc = add_legend(bristol_epc, 'Building Energy Rating', colors = list(colors_epc.values()), labels = list(colors_epc.keys()))
loc = ' Geo-Location Map - EPC Ratings of Nationwide Mortgages '
title_html = '''
             <h2 align="center" style="font-size:24px"><b>{}</b></h2>
             '''.format(loc) 
bristol_epc = bristol_epc.get_root().html.add_child(folium.Element(title_html))
bristol_epc.save('Bristol_Airbus_Nwide_Energy_Rating.html')
bucket = cos.Bucket("publishedairbusdata")
obj = bucket.Object('visualization/Bristol_Airbus_Nwide_Energy_Rating.html')

with open('Bristol_Airbus_Nwide_Energy_Rating.html', 'rb') as bristol_epc:
    obj.upload_fileobj(bristol_epc)

print("EPC RATING MAP GENERATED AND SAVED")


#Plotting flood_rating data
print("\n------")
print("GENERATING FLOOD RATING MAP")
print("------")

bristol_flood=folium.Map(location=[lat,lang],zoom_start=14)
for d in df_nationwide_loanbook.iterrows():
        html = popup_html_flood(d[0])
        popup = folium.Popup(folium.Html(html, script=True), max_width=500)
        colour_d1point = FloodRatingSwitch(d[1]['combinedscore_adj'], d[1]['combinedscore'])
        folium.CircleMarker(
                    [d[1]['latitude'], d[1]['longitude']],
                    radius=6,
                    color=colors_flood[colour_d1point],
                    fill=True,
                    fill_color=colors_flood[colour_d1point],
                    fill_opacity=0.7,
                    popup=popup
            ).add_to(bristol_flood)
            
bristol_flood = add_legend(bristol_flood, 'Likelihood of flooding', colors = list(colors_flood.values()), labels = ('Very Low', 'Low', 'Moderate', 'Moderate to High', 'High', 'Very High', 'ERROR'))
loc = ' Geo-Location Map - Floodability of Nationwide Mortgages '
title_html = '''
             <h2 align="center" style="font-size:24px"><b>{}</b></h2>
             '''.format(loc) 
bristol_flood = bristol_flood.get_root().html.add_child(folium.Element(title_html))
bristol_flood.save('Bristol_Airbus_Nwide_Flood_Rating.html')
bucket = cos.Bucket("publishedairbusdata")
obj = bucket.Object('visualization/Bristol_Airbus_Nwide_Flood_Rating.html')

with open('Bristol_Airbus_Nwide_Flood_Rating.html', 'rb') as bristol_flood:
    obj.upload_fileobj(bristol_flood)
print("FLOOD RATING MAP GENERATED AND SAVED")

#Plotting Nwide_loan data - index search
print("\n------")
print("GENERATING Nwide-LOAN INDEX SEARCH MAP")
print("------")

bristol_airbus_nwide_loanbook = folium.Map(location=[lat,lang],zoom_start=12)
cluster=plugins.MarkerCluster().add_to(bristol_airbus_nwide_loanbook)

for d in df_nationwide_loanbook.iterrows(): 
        html = popup_html_index(d[0])
        popup = folium.Popup(folium.Html(html, script=True), max_width=600)
        folium.Marker(location=[d[1]['latitude'], d[1]['longitude']], popup=popup,name=d[1]["uprn"]).add_to(cluster)
Search(cluster,search_label='name',placeholder='Search for UPRN ID').add_to(bristol_airbus_nwide_loanbook) 
loc = ' Geo-Location Map - Search by UPRN - Nationwide Mortgages '
title_html = '''
             <h2 align="center" style="font-size:24px"><b>{}</b></h2>
             '''.format(loc) 
bristol_airbus_nwide_loanbook = bristol_airbus_nwide_loanbook.get_root().html.add_child(folium.Element(title_html))

bristol_airbus_nwide_loanbook.save('Bristol_Airbus_Nwide_Loanbook_IndexSearch.html')
bucket = cos.Bucket("publishedairbusdata")
obj = bucket.Object('visualization/Bristol_Airbus_Nwide_Loanbook_IndexSearch.html')

with open('Bristol_Airbus_Nwide_Loanbook_IndexSearch.html', 'rb') as bristol_airbus_nwide_loanbook:
    obj.upload_fileobj(bristol_airbus_nwide_loanbook)
print("Airbus-LOAN INDEX SEARCH MAP GENERATED AND SAVED")

## HeatMap Example
#### Follow this example of how to create a HEATMAP : https://wellsr.com/python/plotting-geographical-heatmaps-with-python-folium-module/ . 

bristol_cluster = folium.Map(location=[lat,lang],zoom_start=17)

# HeatMap array of values
array_heatmap = []
for d in df_nationwide_loanbook.iterrows(): 
         score_all_weight_0_100 = FloodRating_Ration_0_100_Value(d[1]['combinedscore_adj'], d[1]['combinedscore'])
         array_val = [[ d[1]['latitude'], d[1]['longitude'], score_all_weight_0_100 ]]
         array_heatmap = array_heatmap + array_val
# print(array_heatmap)
html = popup_html_flood(d[0])
popup = folium.Popup(folium.Html(html, script=True), max_width=500)
HeatMap(array_heatmap).add_to(bristol_cluster)

loc = ' HeatMap - Floodability Risk '   
title_html = '''
             <h2 align="center" style="font-size:24px"><b>{}</b></h2>
             '''.format(loc) 
bristol_cluster = bristol_cluster.get_root().html.add_child(folium.Element(title_html))

#bristol_cluster.save('data/output/visualization_Bristol_Airbus_Nationwide_loanbook/Bristol_Airbus_Nwide_Flood_Rating_heatmap.html')

bristol_cluster.save('Bristol_Airbus_Nwide_Flood_Rating_heatmap.html')
bucket = cos.Bucket("publishedairbusdata")
obj = bucket.Object('visualization/Bristol_Airbus_Nwide_Flood_Rating_heatmap.html')

with open('Bristol_Airbus_Nwide_Flood_Rating_heatmap.html', 'rb') as bristol_cluster:
    obj.upload_fileobj(bristol_cluster)
print("Airbus-FLOOD RATING HEAT MAP GENERATED AND SAVED")
