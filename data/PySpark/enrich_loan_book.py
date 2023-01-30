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

log4j_logger = spark._jvm.org.apache.log4j  # noqa
logger = log4j_logger.LogManager.getRootLogger()

####################################
# Parameters
####################################
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
dtStr = datetime.today().strftime('%Y%m%d')

gammaData = "/usr/local/spark/resources/data/staging/gamma/dt="+dtStr+"/part*"
loanBookData = sys.argv[1]
logger.info("######################################")
logger.info("READING INPUT FILES")
logger.debug("GAMMA :: "+gammaData)
logger.debug("LOAN BOOK :: "+loanBookData)
logger.debug("Date :: "+dtStr)
logger.info("######################################")


####################################
# Read CSV Data
####################################
logger.info("######################################")
logger.info("READING GAMMA CSV DATA ")
logger.info("######################################")


df_gamma_schema=StructType().add("ecad_id",IntegerType(),True).add("building_id",IntegerType(),True).add("address_line_1",StringType(),True).add("address_line_2",StringType(),True).add("address_line_3",StringType(),True).add("address_line_4",StringType(),True).add("address_line_5",StringType(),True).add("address_line_6",StringType(),True).add("address_line_7",StringType(),True).add("address_line_8",StringType(),True).add("address_line_9",StringType(),True).add("etrs89_lat",DoubleType(),True).add("etrs89_long",DoubleType(),True).add("ber_rating",StringType(),True).add("ber_rating_kwh",DoubleType(),True).add("co2_emission",DoubleType(),True).add("riverrp",IntegerType(),True).add("rmax20",DoubleType(),True).add("rmax75",DoubleType(),True).add("rmax100",DoubleType(),True).add("rmax200",DoubleType(),True).add("rmax1000",DoubleType(),True).add("sop_ri",IntegerType(),True).add("coastaludrp",IntegerType(),True).add("cudmax75",DoubleType(),True).add("cudmax100",DoubleType(),True).add("cudmax200",DoubleType(),True).add("cudmax1000",DoubleType(),True).add("swaterrp",IntegerType(),True).add("swmax75",DoubleType(),True).add("swmax200",DoubleType(),True).add("swmax1000",DoubleType(),True).add("model_river",StringType(),True).add("model_coastal",StringType(),True).add("model_sw",StringType(),True).add("r20matrix",IntegerType(),True).add("r75matrix",IntegerType(),True).add("r100matrix",IntegerType(),True).add("r200matrix",IntegerType(),True).add("r1000matrix",IntegerType(),True).add("cud75matrix",IntegerType(),True).add("cud100matrix",IntegerType(),True).add("cud200matrix",IntegerType(),True).add("cud1000matrix",IntegerType(),True).add("sw75matrix",IntegerType(),True).add("sw200matrix",IntegerType(),True).add("sw1000matrix",IntegerType(),True).add("river_floodscore_ud",IntegerType(),True).add("coastal_floodscore_ud",IntegerType(),True).add("surfacewater_floodscore_ud",IntegerType(),True).add("river_floodscore_def",IntegerType(),True).add("floodscore_ud",IntegerType(),True).add("floodscore_def",IntegerType(),True).add("unflood_value",IntegerType(),True).add("unflood_heightband",StringType(),True).add("floodability_index_ud",StringType(),True).add("floodability_index_def",StringType(),True).add("_corrupt_record", StringType(), True)

df_gamma_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(df_gamma_schema)
    .load(gammaData)
).cache()


df_gamma_corrupt = df_gamma_csv.filter("_corrupt_record is not null")
df_gamma = df_gamma_csv.filter("_corrupt_record is null").drop("_corrupt_record")

logger.info("######################################")
logger.info("GAMMA CSV DATA SCHEMA")
logger.info("######################################")
logger.info(df_gamma._jdf.schema().treeString())
df_gamma.show(100,False)

if df_gamma_corrupt.count() > 0 : 
    logger.info("######################################")
    logger.info("GAMMA BAD RECORD COUNTS : "+ str(df_gamma_corrupt.count()))
    logger.info("######################################")

logger.info("######################################")
logger.info("GAMMA CSV DATA SUCCESS")
logger.info("######################################")

logger.info("######################################")
logger.info("READING LOAN BOOK CSV DATA ")
logger.info("######################################")

df_lnbk_schema=StructType().add("ecad_id",IntegerType(),True).add("Asset_id",IntegerType(),True).add("product",StringType(),True).add("term",IntegerType(),True).add("Rate/Margin",DoubleType(),True).add("Disbursment_Date",StringType(),True).add("Closing_Date",StringType(),True).add("Initial_Borrow_Capital",DoubleType(),True).add("LTV",DoubleType(),True).add("Monthly_Repayment",DoubleType(),True).add("Next_Payment_Due",StringType(),True).add("Monthly_Capital_Repayment",DoubleType(),True).add("Months_Paid",IntegerType(),True).add("Months_Due",IntegerType(),True).add("Property_Valuation",DoubleType(),True).add("Initial_Deposit",DoubleType(),True).add("Mortgage_Outstanding",DoubleType(),True).add("_corrupt_record", StringType(), True)

df_lnbk_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(df_lnbk_schema)
    .load(loanBookData)
).cache()

df_lnbk_corrupt = df_lnbk_csv.filter("_corrupt_record is not null")
df_lnbk = df_lnbk_csv.filter("_corrupt_record is null").drop("_corrupt_record")

logger.info("######################################")
logger.info("LOAN BOOK CSV DATA SCHEMA")
logger.info("######################################")
logger.info(df_lnbk._jdf.schema().treeString())
df_lnbk.show(100,False)

if df_lnbk_corrupt.count() > 0 : 
    logger.info("######################################")
    logger.info("LOAN BOOK BAD RECORD COUNTS : "+ str(df_lnbk_corrupt.count()))
    df_lnbk_corrupt.coalesce(1).write.option("header",True).mode('overwrite').csv("/usr/local/spark/resources/data/staging/loan_book_rejected/dt="+dtStr+"/")
    logger.info("######################################")

logger.info("######################################")
logger.info("LOAN BOOK CSV DATA SUCCESS")
logger.info("######################################")


logger.info("######################################")
logger.info("ENRICH LOAN BOOK DATA WITH GAMMA DATA")
logger.info("######################################")
df_enriched_lnbk = df_gamma.join(df_lnbk,df_lnbk.ecad_id == df_gamma.ecad_id,"left_outer").drop(df_lnbk.ecad_id)
               
logger.info("######################################")
logger.info("ENRICHED LOAN BOOK DATA SCHEMA")
logger.info("######################################")
logger.info(df_enriched_lnbk._jdf.schema().treeString())
df_enriched_lnbk.show(100,False)

logger.info("######################################")
logger.info("DATA ENRICHMENT SUCCESS")
logger.info("######################################")

logger.info("######################################")
logger.info("WRITING ENRICHED LOAN BOOK CSV")
logger.info("######################################")

df_enriched_lnbk.coalesce(1).write.option("header",True).mode('overwrite').csv("/usr/local/spark/resources/data/published/enriched_loan_book/dt="+dtStr+"/")

logger.info("######################################")
logger.info("ENRICHED LOAN BOOK CSV WRITE COMPLETE")
logger.info("######################################")

spark.stop()