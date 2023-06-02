import numpy as np 
import pandas as pd 
import sys
from pyspark.sql import SparkSession
#from openpyxl import Workbook
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, to_timestamp, split, explode, sum, count, row_number, desc, expr, when
from pyspark.sql.types import StructType,TimestampType, StringType, IntegerType, DoubleType
from datetime import datetime
import ibm_boto3
from ibm_botocore.client import Config, ClientError
import io
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
  COS_INSTANCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/f2d7386c3c18406b9e2eed413aa7d007:629d310c-63f6-474a-826c-323c2af3d861::"
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


####################################
# Parameters
####################################
dtStr = datetime.today().strftime('%Y%m%d')
cos = getCOSconfig()
itemList = getBucketContents(cos,"publishedairbusdata",[dtStr,"csv"])
print(itemList)
loanBookData = "cos://publishedairbusdata.Airbus/"+itemList[0]
logger.info("######################################")
logger.info("READING INPUT FILE")
logger.debug("Enriched Data :: "+loanBookData)
logger.info("######################################")

####################################
# Read CSV Data
####################################
logger.info("######################################")
logger.info("READING Enriched CSV DATA ")
logger.info("######################################")

df_lnbk_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .option("inferSchema", "true")
    .load(loanBookData)
).cache()

df_lnbk_csv.show(5,False)

df_lnbk_csv_1 = df_lnbk_csv.withColumn("initial_borrow_capital",col("currentloan") * 1/ col("currentltv"))
df_lnbk_csv_1.show(5)

# Total UK (initial_borrow_capital)
df_lnbk_ib_sum = df_lnbk_csv_1.select(sum("initial_borrow_capital"))
df_lnbk_ib_sum.show(5,False)

# EPC rating initial_borrow_capital

df_lnbk_EPC = df_lnbk_csv_1.groupBy(col('current_energy_rating').substr(1,1).alias("current_energy_rating")).agg(sum("initial_borrow_capital").alias("initial_borrow_capital").cast('Float'),count("*").alias("Total_Count")).sort(col('current_energy_rating').substr(1,1))
df_lnbk_EPC.show(10,False) 

# kwh consumption initial_borrow_capital
df_lnbk_kwh = df_lnbk_csv_1.selectExpr("current_energy_efficiency","initial_borrow_capital","CASE WHEN current_energy_efficiency <= 100 THEN  '0; <= 100' WHEN current_energy_efficiency > 100 and current_energy_efficiency <= 200 THEN  '> 100; <= 200' WHEN current_energy_efficiency > 200 and current_energy_efficiency <= 300 THEN  '> 200; <= 300' WHEN current_energy_efficiency > 300 and current_energy_efficiency <= 400 THEN  '> 300; <= 400' WHEN current_energy_efficiency > 400 and current_energy_efficiency <= 500 THEN  '> 400; <= 500' ELSE '> 500' END AS epc_rating_new")
df_lnbk_kwh.show(10,False)
df_lnbk_kwh_final = df_lnbk_kwh.groupBy(col('epc_rating_new').alias("epc_rating_category")).agg(sum("initial_borrow_capital").alias("initial_borrow_capital").cast('Float'),count("*").alias("Total_Count")).sort(col('epc_rating_new'))
df_lnbk_kwh_final.show(10,False)

#generate dataframe
dict_header={('Counterparty sector', ' ', ' '): {1:'Total UK area', 
                                    2:'Of which Loans collateralised by commercial immovable property', 
                                    3:'Of which Loans collateralised by residential immovable property',
                                    4:'Of which Collateral obtained by taking possession: residential and commercial immovable properties',
                                    5:'Of which Level of energy efficiency (EP score in kWh/m² of collateral) estimated',
                                    6:'Total non-UK area',
                                    7:'Of which Loans collateralised by commercial immovable property',
                                    8:'Of which Loans collateralised by residential immovable property',
                                    9:'Of which Collateral obtained by taking possession: residential and commercial immovable properties',
                                    10:'Of which Level of energy efficiency (EP score in kWh/m² of collateral) estimated'}, 
             ('Total gross carrying amount (in MEUR)', '', ''): {1: df_lnbk_ib_sum.collect()[0][0], 
                                                            2: 0,
                                                            3: df_lnbk_ib_sum.collect()[0][0],
                                                            4: 0,
                                                            5: 0,
                                                            6: 0,
                                                            7: 0,
                                                            8: 0,
                                                            9: 0,
                                                            10:0}}
data_kwh = df_lnbk_kwh_final.collect()
dict_data_kwh={}
for row in data_kwh:
     dict_data_kwh[('Total gross carrying amount (in MEUR)', 'Level of energy efficiency (EP score in kWh/m² of collateral)', row[0])]= {1: row[1], 
                                                            2: 0,
                                                            3: row[1],
                                                            4: 0,
                                                            5: 0,
                                                            6: 0,
                                                            7: 0,
                                                            8: 0,
                                                            9: 0,
                                                            10:0}
data_EPC = df_lnbk_EPC.collect()
dict_data_EPC={}
for row in data_EPC:
     dict_data_EPC[('Total gross carrying amount (in MEUR)', 'Level of energy efficiency (EPC label of collateral)', row[0])]= {1: row[1], 
                                                            2: 0,
                                                            3: row[1],
                                                            4: 0,
                                                            5: 0,
                                                            6: 0,
                                                            7: 0,
                                                            8: 0,
                                                            9: 0,
                                                            10:0}
        
dict_data_wo_EPC= {('Total gross carrying amount (in MEUR)', 'Without EPC label of collateral', 'Of which level of energy efficiency (EP score in kWh/m² of collateral) estimated'): {1: 0, 
                                                            2: 0,
                                                            3: 0,
                                                            4: 0,
                                                            5: 0,
                                                            6: 0,
                                                            7: 0,
                                                            8: 0,
                                                            9: 0,
                                                            10:0}}                                                   
data = {**dict_header, **dict_data_kwh, **dict_data_EPC, **dict_data_wo_EPC}
df = pd.DataFrame.from_dict(data)
final_df = df.style.set_table_styles([{'selector': '*', 'props': [('font-size', '6.5pt'),('border-style','solid'),('border-width','1px')]}])
final_df

#ebaP3Excel= final_df.to_excel("Airbus_EBA_Pillar_III_"+dtStr+".xlsx")
cos = getCOSconfig()
bucket = cos.Bucket("publishedairbusdata")

with io.BytesIO() as output:
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        final_df.to_excel(writer)
    ebaP3Excel = output.getvalue()
bucket.put_object(Key="reports/Airbus_EBA_Pillar_III_"+dtStr+".xlsx",Body=ebaP3Excel)
       
spark.stop()