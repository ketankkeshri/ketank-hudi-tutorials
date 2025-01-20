import sys
from sys import audit

from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import DoubleType, \
    StringType, TimestampType, StructType, StructField, IntegerType, DecimalType, BooleanType
from awsglue.utils import getResolvedOptions
import json
import pyspark.sql.functions as F
import boto3
import logging as log

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


#################################


'''
 docker run -it k3k_hudi pyspark --conf spark.jars=./jars/hudi-spark3.5-bundle_2.12-1.0.0.jar,./jars/spark-avro_2.12-3.5.0.jar --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false

'''

import pyspark.sql.functions as F

df=spark.read.csv('/data/bihar_road_stats_ndap.csv', header=True)
#Renamed index column as id to use it as primary key as record key

df = df.withColumn('id', F.monotonically_increasing_id()) \
      .withColumnRenamed('Road category','road_category') \
      .withColumnRenamed('Road organized by','road_organized_by') \
        .withColumnRenamed('Road type','road_type')

df=df.select('id','road_category','road_organized_by','road_type')
#Added ts column  to use it as audit column / pre-combine field
df = df.withColumn("ts", F.current_timestamp())

hudi_common_config = {
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.parquet.compression.codec": "snappy",
    }

### LOAD type 'INSERT'
db_name='data_pays'
table_name='bihar_road_stats_ndap'
insert_type='insert'
p_key='id'

hudi_table_params = {
        "hoodie.database.name": db_name,
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.operation": insert_type,
        "hoodie.datasource.write.recordkey.field": p_key,
}

target_path=f"/data/out/{db_name}/{table_name}"
df.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)

tbl=spark.read.format('hudi').load(target_path)
tbl.count()

tbl.select(p_key).count()
132
tbl.select(p_key).distinct().count()
33

df2 = df.withColumn("ts", F.when(df.id<4, F.current_timestamp()).otherwise(df.ts)).limit(2,3)
df2.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)



target_path=f"/data/out/{db_name}/{table_name}_wots"
df.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)

# One additional entry
df3=df.limit(1).withColumn('id',F.lit(39));

df3.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)

## Spark mode overwrite with Hudi insert type, delete directory and creates new
df3=df.limit(1).withColumn('id',F.lit(40))
df3.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("overwrite") \
        .save(target_path)

## Spark mode ignore with Hudi insert type, throws exception
df3=df.limit(1).withColumn('id',F.lit(41))
df3.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("ignore") \
        .save(target_path)

## Spark mode error with Hudi insert type, throws exception
df3=df.limit(1).withColumn('id',F.lit(41))
# .mode("error") default mode!
target_path=f"/data/out/{db_name}/{table_name}_wots_ow"
df3.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .save(target_path)





######### Hudi mode: bulk_insert with mode append
target_path=f"/data/out/{db_name}/{table_name}"
tbl=spark.read.format('hudi').load(target_path)
tbl.count()

insert_type="bulk_insert"
hudi_table_params.update({
        "hoodie.datasource.write.operation": insert_type,
})

df2 = df.withColumn("ts", F.when(df.id<4, F.current_timestamp()).otherwise(df.ts)).limit(2)

df2.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)

tbl.select('id','ts').orderBy('id','ts').show(truncate=False)
tbl.count()
tbl.select('id').distinct().count()


######### Hudi mode: delete with mode append
target_path=f"/data/out/{db_name}/{table_name}"
tbl=spark.read.format('hudi').load(target_path)
tbl.count()

insert_type="delete"
hudi_table_params.update({
        "hoodie.datasource.write.operation": insert_type,
})

df2 = df.withColumn("ts", F.when(df.id>4, F.current_timestamp()).otherwise(df.ts)).limit(2)

df2 = df.filter('id>4').limit(2)

target_path=f"/data/out/{db_name}/{table_name}"
tbl=spark.read.format('hudi').load(target_path)
tbl.select('id','ts').orderBy('id','ts').show(truncate=False)
tbl.count()

df2.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)



######### Hudi mode: upsert with mode append

target_path=f"/data/out/{db_name}/{table_name}"
tbl=spark.read.format('hudi').load(target_path)
tbl.select('id','ts').orderBy('id','ts').show(truncate=False)
tbl.count()
tbl.select('id').distinct().count()


insert_type="upsert"
hudi_table_params.update({
        "hoodie.datasource.write.operation": insert_type,
})

df2 = df.filter('id>4').limit(2)


df2.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)

tbl=spark.read.format('hudi').load(target_path)
tbl.select('id','ts').orderBy('id','ts').show(truncate=False)
tbl.count()
tbl.select('id').distinct().count()

df2 = df.filter('id>14').limit(3).withColumn('id',F.lit('102'))

df2.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)


#####UPSERT drop duplicates


hudi_table_params.update({
        "hoodie.combine.before.insert": "true",
})

insert_type="insert"
hudi_table_params.update({
        "hoodie.datasource.write.operation": insert_type,
})


#################
############## Partiotioning data based on DAY
#>>> df.withColumn('day', F.date_format('ts','yyyyMMdd')).select('ts','day').show()


hudi_common_config = {
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.parquet.compression.codec": "snappy",
    }
#"hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",

### LOAD type 'INSERT'
db_name='data_pays'
table_name='bihar_road_stats_ndap_part'
insert_type='upsert'
p_key='id'
partition_col='day'
target_path=f"/data/out/{db_name}/{table_name}"

hudi_table_params = {
        "hoodie.database.name": db_name,
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.operation": insert_type,
        "hoodie.datasource.write.recordkey.field": p_key,
        "hoodie.datasource.write.partitionpath.field": partition_col,
}


df = df.withColumn('day', F.date_format('ts','yyyyMMdd'))
df2 = df.withColumn('day', '20250114')

df.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)



tbl=spark.read.format('hudi').load(target_path)
tbl.select('id','ts','day').orderBy('id','ts').show(truncate=False)
tbl.count()
tbl.select('id').distinct().count()


df2 = df.withColumn('day', F.lit('20250114'))

df2.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)

tbl=spark.read.format('hudi').load(target_path)
tbl.select('id','ts','day').orderBy('id','ts').show(truncate=False)
tbl.count()
tbl.select('id').distinct().count()

tbl.groupBy('day').agg(F.countDistinct('id'), F.count('id')).show()


#Partitioned Insert,

insert_type='insert'
p_key='id'
partition_col='day'
target_path=f"/data/out/{db_name}/{table_name}"

hudi_table_params = {
        "hoodie.database.name": db_name,
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.operation": insert_type,
        "hoodie.datasource.write.recordkey.field": p_key,
        "hoodie.datasource.write.partitionpath.field": partition_col,
}

df2.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)



tbl=spark.read.format('hudi').load(target_path)
tbl.select('id','ts','day').orderBy('id','ts').show(truncate=False)
tbl.count()
tbl.select('id').distinct().count()

tbl=spark.read.format('hudi').load(target_path)
tbl.select('day','id').groupBy('day').count().show()

##########


tbl.filter('id=5').select('id','ts').orderBy('id','ts').show(truncate=False)

tbl.select('id','ts').groupBy('id').count().orderBy('id').show(truncate=False)

tbl.filter('id=0').select('id','ts').show(truncate=False)
'''
+---+--------------------------+
|id |ts                        |
+---+--------------------------+
|0  |2025-01-11 13:48:27.988022|
|0  |2025-01-11 13:48:27.988022|
|0  |2025-01-11 13:48:27.988022|
|0  |2025-01-11 13:48:27.988022|
|0  |2025-01-12 15:53:49.306396|
|0  |2025-01-12 16:18:13.313914|
|0  |2025-01-12 16:09:49.58444 |
|0  |2025-01-12 16:07:29.078446|
|0  |2025-01-12 16:02:12.355957|
|0  |2025-01-12 15:59:19.850051|
+---+--------------------------+
'''
#### Time travel queries

tbl2 = spark.read.format("hudi"). \
  option("as.of.instant", "2025-01-12 15:53:49.306"). \
  load(target_path)


tbl2 = spark.read.format("hudi"). \
  option("as.of.instant", "20250112155349333"). \
  load(target_path)

tbl2.filter('id=5').select('id','ts').show(truncate=False)

tbl3 = spark.read.format("hudi"). \
  option("as.of.instant", "20250111134828027"). \
  load(target_path)

tbl3.filter('id=5').select('id','ts').show(truncate=False)



'''25/01/12 16:28:18 WARN HoodieFileIndex: Data skipping requires Metadata Table and at least one of the indices 
to be enabled! (isMetadataTableEnabled = true, isColumnStatsIndexEnabled = false, 
isRecordIndexApplicable = false, isExpressionIndexEnabled = false, 
isBucketIndexEnable = false, isPartitionStatsIndexEnabled = false), 
isBloomFiltersIndexEnabled = false)
'''
##############
'''
hoodie.table.keygenerator.type=NON_PARTITION
hoodie.table.type=COPY_ON_WRITE
hoodie.table.precombine.field=ts
hoodie.timeline.layout.version=2
hoodie.timeline.history.path=history
hoodie.table.checksum=1076423444
hoodie.datasource.write.drop.partition.columns=false
hoodie.record.merge.strategy.id=eeb8d96f-b1e4-49fd-bbf8-28ac514178e5
hoodie.datasource.write.hive_style_partitioning=false
hoodie.table.metadata.partitions.inflight=
hoodie.database.name=data_pays
hoodie.datasource.write.partitionpath.urlencode=false
hoodie.record.merge.mode=EVENT_TIME_ORDERING
hoodie.table.version=8
hoodie.compaction.payload.class=org.apache.hudi.common.model.DefaultHoodieRecordPayload
hoodie.table.initial.version=8
hoodie.table.metadata.partitions=files
hoodie.table.partition.fields=
hoodie.archivelog.folder=history
hoodie.table.cdc.enabled=false
hoodie.table.multiple.base.file.formats.enable=false
hoodie.table.timeline.timezone=LOCAL
hoodie.table.name=bihar_road_stats_ndap
hoodie.table.recordkey.fields=id
hoodie.timeline.path=timeline
hoodie.partition.metafile.use.base.format=false
hoodie.populate.meta.fields=true
hoodie.table.base.file.format=PARQUET

'''


hudi_common_config = {
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "hoodie.schema.on.read.enable": "true",
        "hoodie.datasource.write.reconcile.schema": "true",  # For handling schema changes
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.parquet.compression.codec": "snappy",
        "hoodie.datasource.hive_sync.enable": "true",

    }

hudi_table_params = {
        "hoodie.database.name": target_db_name,
        "hoodie.table.name": target_table_name,
        "hoodie.datasource.write.operation": insert_type,
        "hoodie.datasource.write.recordkey.field": p_key,
        "hoodie.datasource.write.precombine.field": audit_col,
        "hoodie.datasource.hive_sync.database": target_db_name,
        "hoodie.datasource.hive_sync.table": target_table_name,
        "hoodie.datasource.write.partitionpath.field": partition_col,
        "hoodie.datasource.write.insert.drop.duplicates": "true",
        "hoodie.combine.before.insert": "true",
    }

### overwrite not working after Insert type
target_path=f"/data/out/{db_name}/{table_name}_wots2"
df.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)


df2 = df.withColumn("ts", F.when(df.id<4, F.current_timestamp()).otherwise(df.ts)).limit(5)
df2.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)



df.select('id','road_category','road_organized_by','road_type')\
        .write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)



##Different Load type 'upsert'
df = df.withColumn("ts", F.when(df.id<4, F.current_timestamp()).otherwise(df.ts))
insert_type='upsert'
audit_col='ts'

hudi_table_params.update({
        "hoodie.datasource.write.operation": insert_type,
        "hoodie.datasource.write.precombine.field": audit_col,
})

target_path=f"/data/out/{db_name}/{table_name}"
df.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)


########

hudi_table_params = {
        "hoodie.database.name": db_name,
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.operation": insert_type,
        "hoodie.datasource.write.recordkey.field": p_key,
        "hoodie.datasource.write.precombine.field": audit_col,
}

### Partition field


#################################



def check_data_exists(bucket_name, Prefix):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name,
                                  Prefix=Prefix, MaxKeys=1)
    if response['KeyCount'] > 0:
        return True
    else:
        return False


def get_schema(target_db_name, target_table_name, assets_bucket):
    s3 = boto3.resource('s3')
    try:
        prefix = f"schema/{target_db_name}/{target_table_name}.json"
        print(prefix)
        obj = s3.Object(assets_bucket, prefix)
        if obj is None:
            print(f"Schema not available for: {target_db_name}.{target_table_name}")
            raise Exception(f"Schema not available for: {target_db_name}.{target_table_name}")
        # Put null check
        schema_json = obj.get()['Body'].read().decode('utf-8')
        schema = StructType.fromJson(json.loads(schema_json))
        return schema
    except Exception as e:
        print(e)
        raise Exception(f"Schema not available for: {target_db_name}.{target_table_name}")


import sys


def start_application():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'params'])
    params = json.loads(args['params'])

    log = glueContext.get_logger()
    s3 = boto3.resource('s3')

    log.info(f"Parameters are: {params}")
    print(f"Parameters are: {params}")

    landing_bucket = params['landing_bucket']
    data_path = params['data_path']

    target_bucket = params['target_bucket']
    target_db_name = params['target_db_name'].lower()
    target_table_name = params['target_table_name'].lower()

    p_key = params['p_key']
    # If primary keys are multiple, pass the values in an array and use all these
    if isinstance(p_key, list):
        p_key = ','.join([col.lower() for col in p_key])

    p_key = p_key.lower()  # ""  # <primary_keys>
    audit_col = params['audit_col']
    audit_col = audit_col.lower()  # "<updatedat>"
    insert_type = params['insert_type']  # "upsert"

    partition_src_col = params['partition_src_col']  # "<created_at>"
    partition_col = params['partition_col']  # "<day>"

    print(landing_bucket, data_path, target_bucket, target_db_name, target_table_name, p_key, audit_col, insert_type,
          partition_src_col, partition_col)

    source_path = f"s3://{landing_bucket}/{data_path}/{target_db_name}/{target_table_name}/"
    target_path = f"s3://{target_bucket}/silver/{target_db_name}/{target_table_name}"  # TODO

    log.info(f"Source Path is: {source_path}")
    log.info(f"Target Path is: {target_path}")

    assets_bucket = "mbk-datalake-assets"

    if not check_data_exists(landing_bucket, f"{data_path}/{target_db_name}/{target_table_name}"):
        print(f"Data doesn't exist at path: {source_path}")
        spark.stop()
        return

    new_data = spark.read.parquet(source_path)
    schema = get_schema(target_db_name, target_table_name, assets_bucket)

    # Fill na for audit column
    # Case where Audit column is null, we will populate it with the max value of that column.
    max_audit_value = new_data.agg({audit_col: "max"}).collect()[0][0]
    log.info(f"Max audit value is: {max_audit_value}, filling this as na")

    new_data = new_data.fillna(max_audit_value, subset=[audit_col])

    for field in schema:
        col_name = field.name.lower()
        data_type = field.dataType
        new_data = new_data.withColumn(col_name, F.col(col_name).cast(data_type))

    new_data = new_data.withColumn(partition_col, F.date_format(partition_src_col, "yyyyMMdd"))
    new_data = new_data.fillna('20000101', subset=[partition_col])

    new_data.printSchema()

    hudi_common_config = {
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "hoodie.schema.on.read.enable": "true",
        "hoodie.datasource.write.reconcile.schema": "true",  # For handling schema changes
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.parquet.compression.codec": "snappy",
        "hoodie.datasource.hive_sync.enable": "true",

    }

    hudi_table_params = {
        "hoodie.database.name": target_db_name,
        "hoodie.table.name": target_table_name,
        "hoodie.datasource.write.operation": insert_type,
        "hoodie.datasource.write.recordkey.field": p_key,
        "hoodie.datasource.write.precombine.field": audit_col,
        "hoodie.datasource.hive_sync.database": target_db_name,
        "hoodie.datasource.hive_sync.table": target_table_name,
        "hoodie.datasource.write.partitionpath.field": partition_col,
        "hoodie.datasource.write.insert.drop.duplicates": "true",
        "hoodie.combine.before.insert": "true",
    }
    print(hudi_table_params, schema)
    new_data.write.format("org.apache.hudi") \
        .options(**hudi_common_config) \
        .options(**hudi_table_params) \
        .mode("append") \
        .save(target_path)

    # Delete replicated files
    glueContext.purge_s3_path(source_path, {"retentionPeriod": 0})

    log.info(f'Data loaded and deleted from source path: {source_path}')
    log.info('Job Completed')
    return


start_application()