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

gammaData = "cos://transformedgammadata.Gamma/merged_gamma_esg/dt="+dtStr+""
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

df_gamma = (
    spark.read
    .parquet(gammaData)
).cache()



logger.info("######################################")
logger.info("GAMMA CSV DATA SCHEMA")
logger.info("######################################")
logger.info(df_gamma._jdf.schema().treeString())
df_gamma.show(100,False)

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

df_enriched_lnbk.coalesce(1).write.option("header",True).mode('overwrite').csv("cos://publishedgammadata.Gamma/enriched_loanbook_esg_gamma/dt="+dtStr)

logger.info("######################################")
logger.info("ENRICHED LOAN BOOK CSV WRITE COMPLETE")
logger.info("######################################")

spark.stop()