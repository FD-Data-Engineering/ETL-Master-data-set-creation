import sys
#from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, to_timestamp, split, explode, sum, count, row_number, desc
from pyspark.sql.types import StructType,TimestampType, StringType, IntegerType, DoubleType
from datetime import datetime


print("Executing merge ")

# Create spark session
def init_spark():
  spark = (SparkSession
    .builder
    .getOrCreate())
  sc = spark.sparkContext
  return spark,sc

def getLogger(spark):
    log4j_logger = spark._jvm.org.apache.log4j
    logger = log4j_logger.LogManager.getRootLogger()
    return logger


def main():
    
    spark,sc = init_spark()
    logger = getLogger(spark)
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
    
    portfolioData = sys.argv[1]
    twinnData = sys.argv[2]
    latLongData = sys.argv[3]
    dtStr = datetime.today().strftime('%Y%m%d')

    logger.info("######################################")
    logger.info("READING INPUT FILES")
    logger.debug("Portfolio Data :: "+portfolioData)
    logger.debug("Twinn Data :: "+twinnData)
    logger.debug("Lat Long Data :: "+latLongData)
    logger.debug("Date :: "+dtStr)
    logger.info("######################################")


    ####################################
    # Read CSV Data
    ####################################
    logger.info("######################################")
    logger.info("READING PORTFOLIO CSV DATA ")
    logger.info(portfolioData)
    logger.info("######################################")

    #df_portfolio_schema = StructType().add("id",IntegerType(),True).add("uprn",IntegerType(),True).add("postcode_flag",StringType(),True).add("easting",DoubleType(),True).add("northing",DoubleType(),True).add("rhighscore",IntegerType(),True).add("currentloan",DoubleType(),True).add("currentltv",DoubleType(),True)
    
    df_portfolio_csv = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        #.schema(df_portfolio_schema)
        .load(portfolioData)
    ).cache()
    
    for col in df_portfolio_csv.columns:
        df_portfolio_csv = df_portfolio_csv.withColumnRenamed(col, col.lower())
    #df_portfolio_csv.columns.map(f -> df_portfolio_csv.withColumnRenamed(f, lower(f)))

    logger.info("######################################")
    logger.info("PORTFOLIO CSV DATA SCHEMA")
    logger.info("######################################")
    
    logger.info(df_portfolio_csv._jdf.schema().treeString())
    df_portfolio_csv.show(5,False)


    logger.info("######################################")
    logger.info("PORTFOLIO CSV DATA READ SUCCESS")
    logger.info("######################################")

    logger.info("######################################")
    logger.info("READ TWINN CSV DATA")
    logger.info(twinnData)
    logger.info("######################################")
    
    #df_twinn_schema = StructType().add("id",IntegerType(),True).add("uprn",IntegerType(),True).add("easting",DoubleType(),True).add("northing",DoubleType(),True).#add("latitude",DoubleType(),True).add("longitude",DoubleType(),True)

    df_twinn_csv = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("inferSchema", "true")
        #.schema(df_twinn_schema)
        .load(twinnData)
    ).cache()
    
    for col in df_twinn_csv.columns:
        df_twinn_csv = df_twinn_csv.withColumnRenamed(col, col.lower())
    
    #df_twinn_csv.columns.map(f -> df_twinn_csv.withColumnRenamed(f, lower(f)))
    
    logger.info("######################################")
    logger.info("TWINN CSV DATA SCHEMA")
    logger.info("######################################")
    
    logger.info(df_twinn_csv._jdf.schema().treeString())
    logger.info(df_twinn_csv.show(5,False))

    logger.info("TWINN CSV DATA READ SUCCESS")
    logger.info("######################################")

    logger.info("######################################")
    logger.info("READ LATLONG CSV DATA")
    logger.info(latLongData)
    logger.info("######################################")
    
    #df_latlong_schema = StructType().add("id",IntegerType(),True).add("uprn",IntegerType(),True)


    df_latlong_csv = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .option("inferSchema", "true")
        .option("inferSchema", "true")
        #.schema(df_latlong_schema)
        .load(latLongData)
    ).cache()
    
    for col in df_latlong_csv.columns:
        df_latlong_csv = df_latlong_csv.withColumnRenamed(col, col.lower())
    
    #df_latlong_csv.columns.map(f -> df_latlong_csv.withColumnRenamed(f, lower(f)))
 
    logger.info("######################################")
    logger.info("LAT LONG CSV DATA SCHEMA")
    logger.info("######################################")
    logger.info(df_latlong_csv._jdf.schema().treeString())
    df_latlong_csv.show(5,False)

 
    logger.info("######################################")
    logger.info("LAT LONG CSV DATA READ SUCCESS")
    logger.info("######################################")


    logger.info("######################################")
    logger.info("CREATING UNIFIED AIRBUS DATA")
    
    logger.info("######################################")
    
    #df_merged_gamma_data = df_ecad.join(df_ber,df_ecad.ecad_id == df_ber.ecad_id,"inner").join(df_flood,df_ecad.building_id == df_flood.ecad_id,"inner").drop(df_ber.ecad_id).drop(df_flood.ecad_id)
    
    df_merged_airbus_data = df_portfolio_csv.join(df_latlong_csv,df_portfolio_csv.id == df_latlong_csv.id,"inner").join(df_twinn_csv,df_portfolio_csv.id == df_twinn_csv.id,"inner").drop(df_latlong_csv.id).drop(df_twinn_csv.id).drop(df_latlong_csv.easting).drop(df_latlong_csv.northing).drop(df_twinn_csv.uprn).drop(df_latlong_csv.uprn)             

    logger.info("######################################")
    logger.info("UNIFIED AIRBUS DATA SCHEMA")
    logger.info("######################################")
    logger.info(df_merged_airbus_data._jdf.schema().treeString())
    df_merged_airbus_data.show(5,False)

    logger.info("######################################")
    logger.info("WRITING UNIFIED AIRBUS DATA")
    logger.debug("TARGET :: "+"/usr/local/spark/resources/data/staging/airbus/dt="+dtStr)
    logger.info("######################################")

    df_merged_airbus_data.coalesce(1).write.option("header",True).mode('overwrite').csv("cos://publishedairbusdata.Airbus/merged_airbus_esg/dt="+dtStr+"/mergedfile.csv")


    spark.stop()
    
if __name__ == '__main__':
  main()