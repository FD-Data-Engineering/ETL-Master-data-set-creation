import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, col, to_timestamp, split, explode, sum, count, row_number, desc
from pyspark.sql.types import StructType,TimestampType, StringType, IntegerType, DoubleType
from datetime import datetime

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
propertyData = sys.argv[1]
berData = sys.argv[2]
floodData = sys.argv[3]
dtStr = datetime.today().strftime('%Y%m%d')
#floodData= sys.argv[3]

#postgres_db = sys.argv[3]
#postgres_user = sys.argv[4]
#postgres_pwd = sys.argv[5]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILES")
print("######################################")


df_property_schema = StructType().add("ecad_id",IntegerType(),True).add("building_id",IntegerType(),True).add("address_line_1",StringType(),True).add("address_line_2",StringType(),True).add("address_line_3",StringType(),True).add("address_line_4",StringType(),True).add("address_line_5",StringType(),True).add("address_line_6",StringType(),True).add("address_line_7",StringType(),True).add("address_line_8",StringType(),True).add("address_line_9",StringType(),True).add("etrs89_lat",DoubleType(),True).add("etrs89_long",DoubleType(),True).add("_corrupt_record", StringType(), True)

df_property_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(df_property_schema)
    .load(propertyData)
).cache()



df_ecad_corrupt = df_property_csv.filter("_corrupt_record is not null")
df_ecad = df_property_csv.filter("_corrupt_record is null").drop("_corrupt_record")

df_ecad.printSchema()
df_ecad.show(100,False)



df_ber_schema = StructType().add("ecad_id",IntegerType(),True).add("ber_rating",StringType(),True).add("ber_rating_kwh",DoubleType(),True).add("co2_emission",DoubleType(),True).add("_corrupt_record", StringType(), True)

df_ber_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(df_ber_schema)
    .load(berData)
).cache()


df_ber_corrupt = df_ber_csv.filter("_corrupt_record is not null")
df_ber = df_ber_csv.filter("_corrupt_record is null").drop("_corrupt_record")


df_ber.printSchema()
df_ber.show(100,False)

df_flood_schema = StructType().add("ecad_id",IntegerType(),True).add("riverrp",IntegerType(),True).add("rmax20",DoubleType(),True).add("rmax75",DoubleType(),True).add("rmax100",DoubleType(),True).add("rmax200",DoubleType(),True).add("rmax1000",DoubleType(),True).add("sop_ri",IntegerType(),True).add("coastaludrp",IntegerType(),True).add("cudmax75",DoubleType(),True).add("cudmax100",DoubleType(),True).add("cudmax200",DoubleType(),True).add("cudmax1000",DoubleType(),True).add("swaterrp",IntegerType(),True).add("swmax75",DoubleType(),True).add("swmax200",DoubleType(),True).add("swmax1000",DoubleType(),True).add("model_river",StringType(),True).add("model_coastal",StringType(),True).add("model_sw",StringType(),True).add("r20matrix",IntegerType(),True).add("r75matrix",IntegerType(),True).add("r100matrix",IntegerType(),True).add("r200matrix",IntegerType(),True).add("r1000matrix",IntegerType(),True).add("cud75matrix",IntegerType(),True).add("cud100matrix",IntegerType(),True).add("cud200matrix",IntegerType(),True).add("cud1000matrix",IntegerType(),True).add("sw75matrix",IntegerType(),True).add("sw200matrix",IntegerType(),True).add("sw1000matrix",IntegerType(),True).add("river_floodscore_ud",IntegerType(),True).add("coastal_floodscore_ud",IntegerType(),True).add("surfacewater_floodscore_ud",IntegerType(),True).add("river_floodscore_def",IntegerType(),True).add("floodscore_ud",IntegerType(),True).add("floodscore_def",IntegerType(),True).add("unflood_value",IntegerType(),True).add("unflood_heightband",StringType(),True).add("floodability_index_ud",StringType(),True).add("floodability_index_def",StringType(),True).add("_corrupt_record", StringType(), True)

df_flood_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(df_flood_schema)
    .load(floodData)
).cache()




df_flood_corrupt = df_flood_csv.filter("_corrupt_record is not null")
df_flood = df_flood_csv.filter("_corrupt_record is null").drop("_corrupt_record")

df_flood.printSchema()
df_flood.show(100,False)

df_merged_gamma_data = df_ecad.join(df_ber,df_ecad.ecad_id == df_ber.ecad_id,"left_outer").join(df_flood,df_ecad.building_id == df_flood.ecad_id,"left_outer").drop(df_ber.ecad_id).drop(df_flood.ecad_id)
#               
#
df_merged_gamma_data.printSchema()
df_merged_gamma_data.show(100,False)

df_merged_gamma_data.coalesce(1).write.option("header",True).mode('overwrite').csv("/usr/local/spark/resources/data/staging/gamma/dt="+dtStr+"/")


#write bad records
df_ecad_corrupt.coalesce(1).write.option("header",True).mode('overwrite').csv("/usr/local/spark/resources/data/staging/gamma_rejected/dt="+dtStr+"/ecad")

df_ber_corrupt.coalesce(1).write.option("header",True).mode('overwrite').csv("/usr/local/spark/resources/data/staging/gamma_rejected/dt="+dtStr+"/energy_rating")

df_flood_corrupt.coalesce(1).write.option("header",True).mode('overwrite').csv("/usr/local/spark/resources/data/staging/gamma_rejected/dt="+dtStr+"/flood_index")

#read and rewrite with different name using python3

spark.stop()