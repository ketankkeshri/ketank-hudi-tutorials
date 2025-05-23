################
###### Step:1, # Log into pyspark, either pyspark shell or create a session

##pyspark shell
## This command needs to be run on terminal where pyspark is installed and JAVA_HOME is set
"""

$ pyspark \
 --conf spark.jars=./jars/hudi-spark3.5-bundle_2.12-1.0.0.jar,./jars/spark-avro_2.12-3.5.0.jar \
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer

"""

## Or create a spark session

from pyspark.sql import SparkSession
JARS = "./jars/hudi-spark3.5-bundle_2.12-1.0.0.jar,./jars/spark-avro_2.12-3.5.0.jar"
spark = SparkSession.builder \
    .master("local") \
    .appName("Ketank Hudi tutorials") \
    .config("spark.jars", JARS) \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

#############

###### Step: 2
### Read / Load a .csv file and create a df

### Step: 2a
import pyspark.sql.functions as F

# Update the data folder location accordingly
df = spark.read.csv('./data/bihar_road_stats_ndap.csv', header=True)

# Added id column to use it as a primary key or record key in hudi table

### Step: 2b
## Rename columns and add, an auto increment id column for unique values

df = df.withColumn('id', F.monotonically_increasing_id()) \
    .withColumnRenamed('Road category', 'road_category') \
    .withColumnRenamed('Road organized by', 'road_organized_by') \
    .withColumnRenamed('Road type', 'road_type')

### Step: 2c
### Selecting only limited columns
df = df.select('id', 'road_category', 'road_organized_by', 'road_type')

df.printSchema()

#### Step:3

#### Step:3a, Set hudi common configs

hudi_common_config = {
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.parquet.compression.codec": "snappy",
}

#### Step:3b, Set table params

### LOAD type 'INSERT'
DB_NAME = 'data_pays'
TABLE_NAME = 'bihar_road_stats_ndap'
INSERT_TYPE = 'insert'
P_KEY = 'id'

hudi_table_params = {
    "hoodie.database.name": DB_NAME,
    "hoodie.table.name": TABLE_NAME,
    "hoodie.datasource.write.operation": INSERT_TYPE,
    "hoodie.datasource.write.recordkey.field": P_KEY,
}

#### Step:3c, Set table path location where the HUDI table will be stored
# set this location according to your system's file path, must be absolute path

TARGET_PATH = f"/data/out/{DB_NAME}/{TABLE_NAME}"

#### Step:4
## Write df to the target_path, as HUDI format
df.write.format("hudi") \
    .options(**hudi_common_config) \
    .options(**hudi_table_params) \
    .mode("append") \
    .save(TARGET_PATH)

## Step: 5
## Load / Read the HUDI table and check count and data

hudi_table = spark.read.format('hudi').load(TARGET_PATH)
hudi_table.printSchema()
hudi_table.count()
hudi_table.show(truncate=False)

import boto3
athena_client=boto3.client('athena', region_name="ap-south-1")

response = athena_client.get_table_metadata(
    CatalogName="AwsDataCatalog",
    DatabaseName='mobinew',
    TableName='member')
print(response)

print(response['TableMetadata']['Parameters']['location'])
print(response['TableMetadata']['PartitionKeys'][0]['Name'])
