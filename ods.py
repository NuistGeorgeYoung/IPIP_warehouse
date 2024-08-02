import time

from pyspark.sql import SparkSession, Window
import os

from pyspark.sql.functions import row_number, lit, col, when

os.environ['PYSPARK_PYTHON'] = 'D:\\Python\\Python312\\python.exe'

spark = (SparkSession.builder
         .appName("mbti")
         .master("local[*]")
         .config("spark.sql.debug.maxToStringFields", "150")
         .enableHiveSupport()
         .config("hive.metastore.uris", "thrift://single01:9083")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.default.parallelism", "10")
         .config("spark.sql.parquet.writeLegacyFormat", "true")
         .getOrCreate()
         )


# 建表
# spark.sql("create database ipip_ods;")
# spark.sql("""
# CREATE TABLE IF NOT EXISTS ipip_ods.ipip_data (
#     EXT1 INT,
#     EXT2 INT,
#     EXT3 INT,
#     EXT4 INT,
#     EXT5 INT,
#     EXT6 INT,
#     EXT7 INT,
#     EXT8 INT,
#     EXT9 INT,
#     EXT10 INT,
#     EST1 INT,
#     EST2 INT,
#     EST3 INT,
#     EST4 INT,
#     EST5 INT,
#     EST6 INT,
#     EST7 INT,
#     EST8 INT,
#     EST9 INT,
#     EST10 INT,
#     AGR1 INT,
#     AGR2 INT,
#     AGR3 INT,
#     AGR4 INT,
#     AGR5 INT,
#     AGR6 INT,
#     AGR7 INT,
#     AGR8 INT,
#     AGR9 INT,
#     AGR10 INT,
#     CSN1 INT,
#     CSN2 INT,
#     CSN3 INT,
#     CSN4 INT,
#     CSN5 INT,
#     CSN6 INT,
#     CSN7 INT,
#     CSN8 INT,
#     CSN9 INT,
#     CSN10 INT,
#     OPN1 INT,
#     OPN2 INT,
#     OPN3 INT,
#     OPN4 INT,
#     OPN5 INT,
#     OPN6 INT,
#     OPN7 INT,
#     OPN8 INT,
#     OPN9 INT,
#     OPN10 INT,
#     EXT1_E INT,
#     EXT2_E INT,
#     EXT3_E INT,
#     EXT4_E INT,
#     EXT5_E INT,
#     EXT6_E INT,
#     EXT7_E INT,
#     EXT8_E INT,
#     EXT9_E INT,
#     EXT10_E INT,
#     EST1_E INT,
#     EST2_E INT,
#     EST3_E INT,
#     EST4_E INT,
#     EST5_E INT,
#     EST6_E INT,
#     EST7_E INT,
#     EST8_E INT,
#     EST9_E INT,
#     EST10_E INT,
#     AGR1_E INT,
#     AGR2_E INT,
#     AGR3_E INT,
#     AGR4_E INT,
#     AGR5_E INT,
#     AGR6_E INT,
#     AGR7_E INT,
#     AGR8_E INT,
#     AGR9_E INT,
#     AGR10_E INT,
#     CSN1_E INT,
#     CSN2_E INT,
#     CSN3_E INT,
#     CSN4_E INT,
#     CSN5_E INT,
#     CSN6_E INT,
#     CSN7_E INT,
#     CSN8_E INT,
#     CSN9_E INT,
#     CSN10_E INT,
#     OPN1_E INT,
#     OPN2_E INT,
#     OPN3_E INT,
#     OPN4_E INT,
#     OPN5_E INT,
#     OPN6_E INT,
#     OPN7_E INT,
#     OPN8_E INT,
#     OPN9_E INT,
#     OPN10_E INT,
#     dateload STRING,
#     screenw INT,
#     screenh INT,
#     introelapse INT,
#     testelapse INT,
#     endelapse INT,
#     IPC INT,
#     country STRING,
#     lat_appx_lots_of_err DOUBLE,
#     long_appx_lots_of_err DOUBLE
# )
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY '\t'
# stored as textfile
# LOCATION 'hdfs://single01:9000/IPIP_FFM'
# TBLPROPERTIES("skip.header.line.count"="1");
# """)

# 根据是否为美国进行分区，这样两个分区大小相近
# (spark.table("ipip_ods.ipip_data")
#  .withColumn(
#     "lat_range",
#     when(col("lat_appx_lots_of_err") < 0, lit("<0"))
#     .when((col("lat_appx_lots_of_err") >= 0) & (col("lat_appx_lots_of_err") < 35), lit("0-35"))
#     .when((col("lat_appx_lots_of_err") >= 35) & (col("lat_appx_lots_of_err") < 40), lit("35-40"))
#     .when((col("lat_appx_lots_of_err") >= 40) & (col("lat_appx_lots_of_err") < 45), lit("40-45"))
#     .otherwise(lit(">45"))
# )
#  .withColumn("is_us",
#              when(col("country").isin(["US"]), lit(1)).otherwise(lit(0))
#              )
#  .withColumn("test_id", row_number()
#              .over(Window.orderBy("dateload")))
#  .write
#  .partitionBy("lat_range", "is_us")
#  .mode("overwrite")
#  .saveAsTable("ipip_ods.data")
#  )

# spark.sql("""
# SELECT
#   cast(COUNT(CASE WHEN lat_appx_lots_of_err between 45 and 90 THEN 1  END) / COUNT(*) as float)AS us_ratio
# FROM
#   ipip_ods.data;
# """).show()
avgtime1 = 0
avgtime2 = 0

# spark.sql("select count(*) from ipip_ods.ipip_data limit 10").show
# spark.sql("select * from rsda_dwd.customer limit 20;").show()
# time.sleep(5)
for k in range(1,4):
    st = time.time()
    spark.sql("select count(*) from ipip_ods.ipip_data where  country='US';")
    end = time.time()
    avgtime2+=(end-st)
    spark.stop()

print("不分区的平均时间:"+str(avgtime2/3))

spark.stop()
time.sleep(3)
for i in range(1,4):
    st=time.time()
    spark.sql("select count(*) from ipip_ods.data where  is_us = 1;")
    end=time.time()
    avgtime1 += (end - st)
    spark.stop()
print("分区的平均时间:" + str(avgtime1/3))

spark.stop()
