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
  COS_API_KEY_ID = "trM1JWYO_DAjtqAM58O2IT0iCIfPhV9I2U3GV-yGeypu" # eg "W00YixxxxxxxxxxMB-odB-2ySfTrFBIQQWanc--P3byk"
  COS_INSTANCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/f2d7386c3c18406b9e2eed413aa7d007:ef1bfb54-2d01-45a0-bd5e-8f32a506cb9f::" # eg "crn:v1:bluemix:public:cloud-object-storage:global:a/3bf0d9003xxxxxxxxxx1c3e97696b71c:d6f04d83-6c4f-4a62-a165-696756d63903::"
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
itemList = getBucketContents(cos,"publishedgammadata",[dtStr,"csv"])
print(itemList)
loanBookData = "cos://publishedgammadata.Gamma/"itemList[0]
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

df_loanBookData_schema=StructType().add("ecad_id",IntegerType(),True).add("building_id",IntegerType(),True).add("address_line_1",StringType(),True).add("address_line_2",StringType(),True).add("address_line_3",StringType(),True).add("address_line_4",StringType(),True).add("address_line_5",StringType(),True).add("address_line_6",StringType(),True).add("address_line_7",StringType(),True).add("address_line_8",StringType(),True).add("address_line_9",StringType(),True).add("etrs89_lat",DoubleType(),True).add("etrs89_long",DoubleType(),True).add("ber_rating",StringType(),True).add("ber_rating_kwh",DoubleType(),True).add("co2_emission",DoubleType(),True).add("riverrp",IntegerType(),True).add("rmax20",DoubleType(),True).add("rmax75",DoubleType(),True).add("rmax100",DoubleType(),True).add("rmax200",DoubleType(),True).add("rmax1000",DoubleType(),True).add("sop_ri",IntegerType(),True).add("coastaludrp",IntegerType(),True).add("cudmax75",DoubleType(),True).add("cudmax100",DoubleType(),True).add("cudmax200",DoubleType(),True).add("cudmax1000",DoubleType(),True).add("swaterrp",IntegerType(),True).add("swmax75",DoubleType(),True).add("swmax200",DoubleType(),True).add("swmax1000",DoubleType(),True).add("model_river",StringType(),True).add("model_coastal",StringType(),True).add("model_sw",StringType(),True).add("r20matrix",IntegerType(),True).add("r75matrix",IntegerType(),True).add("r100matrix",IntegerType(),True).add("r200matrix",IntegerType(),True).add("r1000matrix",IntegerType(),True).add("cud75matrix",IntegerType(),True).add("cud100matrix",IntegerType(),True).add("cud200matrix",IntegerType(),True).add("cud1000matrix",IntegerType(),True).add("sw75matrix",IntegerType(),True).add("sw200matrix",IntegerType(),True).add("sw1000matrix",IntegerType(),True).add("river_floodscore_ud",IntegerType(),True).add("coastal_floodscore_ud",IntegerType(),True).add("surfacewater_floodscore_ud",IntegerType(),True).add("river_floodscore_def",IntegerType(),True).add("floodscore_ud",IntegerType(),True).add("floodscore_def",IntegerType(),True).add("unflood_value",IntegerType(),True).add("unflood_heightband",StringType(),True).add("floodability_index_ud",StringType(),True).add("floodability_index_def",StringType(),True).add("Asset_id",IntegerType(),True).add("product",StringType(),True).add("term",IntegerType(),True).add("Rate/Margin",DoubleType(),True).add("Disbursment_Date",StringType(),True).add("Closing_Date",StringType(),True).add("Initial_Borrow_Capital",DoubleType(),True).add("LTV",DoubleType(),True).add("Monthly_Repayment",DoubleType(),True).add("Next_Payment_Due",StringType(),True).add("Monthly_Capital_Repayment",DoubleType(),True).add("Months_Paid",IntegerType(),True).add("Months_Due",IntegerType(),True).add("Property_Valuation",DoubleType(),True).add("Initial_Deposit",DoubleType(),True).add("Mortgage_Outstanding",DoubleType(),True)

df_lnbk_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .schema(df_loanBookData_schema)
    .load(loanBookData)
).cache()

df_lnbk_csv.show(5,False)

# Total EU (Initial_Borrow_Capital)
df_lnbk_ib_sum = df_lnbk_csv.select(sum("Initial_Borrow_Capital"))
df_lnbk_ib_sum.show(5,False)

# BER rating Initial_Borrow_Capital

df_lnbk_BER = df_lnbk_csv.groupBy(col('ber_rating').substr(1,1).alias("ber_rating")).agg(sum("Initial_Borrow_Capital").alias("Initial_Borrow_Capital").cast('Float'),count("*").alias("Total_Count")).sort(col('ber_rating').substr(1,1))
df_lnbk_BER.show(10,False) 

# kwh consumption Initial_Borrow_Capital
df_lnbk_kwh = df_lnbk_csv.selectExpr("ber_rating_kwh","Initial_Borrow_Capital","CASE WHEN ber_rating_kwh <= 100 THEN  '0; <= 100' WHEN ber_rating_kwh > 100 and ber_rating_kwh <= 200 THEN  '> 100; <= 200' WHEN ber_rating_kwh > 200 and ber_rating_kwh <= 300 THEN  '> 200; <= 300' WHEN ber_rating_kwh > 300 and ber_rating_kwh <= 400 THEN  '> 300; <= 400' WHEN ber_rating_kwh > 400 and ber_rating_kwh <= 500 THEN  '> 400; <= 500' ELSE '> 500' END AS ber_rating_new")
df_lnbk_kwh.show(10,False)
df_lnbk_kwh_final = df_lnbk_kwh.groupBy(col('ber_rating_new').alias("ber_rating_category")).agg(sum("Initial_Borrow_Capital").alias("Initial_Borrow_Capital").cast('Float'),count("*").alias("Total_Count")).sort(col('ber_rating_new'))
df_lnbk_kwh_final.show(10,False)

#generate dataframe
dict_header={('Counterparty sector', ' ', ' '): {1:'Total EU area', 
                                    2:'Of which Loans collateralised by commercial immovable property', 
                                    3:'Of which Loans collateralised by residential immovable property',
                                    4:'Of which Collateral obtained by taking possession: residential and commercial immovable properties',
                                    5:'Of which Level of energy efficiency (EP score in kWh/m² of collateral) estimated',
                                    6:'Total non-EU area',
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
data_BER = df_lnbk_BER.collect()
dict_data_BER={}
for row in data_BER:
     dict_data_BER[('Total gross carrying amount (in MEUR)', 'Level of energy efficiency (EPC label of collateral)', row[0])]= {1: row[1], 
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
data = {**dict_header, **dict_data_kwh, **dict_data_BER, **dict_data_wo_EPC}
df = pd.DataFrame.from_dict(data)
final_df = df.style.set_table_styles([{'selector': '*', 'props': [('font-size', '6.5pt'),('border-style','solid'),('border-width','1px')]}])
final_df

#ebaP3Excel= final_df.to_excel("Gamma_EBA_Pillar_III_"+dtStr+".xlsx")
cos = getCOSconfig()
bucket = cos.Bucket("publishedgammadata")
#obj = bucket.Object("reports/Gamma_EBA_Pillar_III_"+dtStr+".xlsx")
#filename = "Gamma_EBA_Pillar_III_"+dtStr+".xlsx"
#with open(filename, 'rb') as z:
#       data = io.BytesIO(z.read())
#       obj.upload_fileobj(data)
with io.BytesIO() as output:
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        final_df.to_excel(writer)
    ebaP3Excel = output.getvalue()
bucket.put_object(Key="reports/Gamma_EBA_Pillar_III_"+dtStr+".xlsx",Body=ebaP3Excel)
       
spark.stop()