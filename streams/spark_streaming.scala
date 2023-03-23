import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

val df_fhv = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "fhv")
  .option("startingOffsets", "earliest")
  .load()

val fhv_schema = new StructType().add("dispatching_base_num", StringType, false).add("pickup_datetime", StringType, false).add("dropOff_datetime", StringType, false).add("PULocationID", LongType).add("DOLocationID", LongType).add("SR_Flag", StringType).add("Affiliated_base_number", StringType)

val df_fhv_string = df_fhv.selectExpr("CAST(value AS STRING)")

val y = df_fhv_string.select(get_json_object($"value", "$.payload").alias("payload"))
val z = y.select(from_json($"payload", fhv_schema).alias("data"))

val fhv_locations = z.select($"data.PULocationID").filter(col("data.PULocationID").isNotNull)
/*
fhv_locations.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate","false")
	  .trigger(Trigger.Once())
    .start()
    .awaitTermination()
*/

val df_green = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "green")
  .option("startingOffsets", "earliest")
  .load()

val green_schema = new StructType()
    .add("VendorID",LongType,false)
    .add("lpep_pickup_datetime",StringType,false)
    .add("lpep_dropoff_datetime",StringType,false)
    .add("store_and_fwd_flag",StringType)
    .add("RatecodeID",StringType)
    .add("PULocationID",LongType)
    .add("DOLocationID",LongType)
    .add("passenger_count",LongType)
	  .add("trip_distance",StringType)
    .add("fare_amount",StringType)
    .add("extra",StringType)
    .add("mta_tax",StringType)
    .add("tip_amount",StringType)
    .add("tolls_amount",StringType)
	  .add("ehail_fee",StringType)
	  .add("improvement_surcharge",StringType)
	  .add("total_amount",StringType)
	  .add("payment_type",StringType)
	  .add("trip_type",StringType)
	  .add("congestion_surcharge",StringType)

val df_green_string = df_green.selectExpr("CAST(value AS STRING)")

val y = df_green_string.select(get_json_object($"value", "$.payload").alias("payload"))
val z = y.select(from_json($"payload", green_schema).alias("data"))

val green_locations = z.select($"data.PULocationID").filter(col("data.PULocationID").isNotNull)
/*
green_locations.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate","false")
	  .trigger(Trigger.Once())
    .start()
    .awaitTermination()
*/

fhv_locations
    .selectExpr("CAST(PULocationID AS STRING) as key", "CAST(PULocationID as STRING) as value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "pickup_locations")
    .option("checkpointLocation","/mnt/c/work/data-camp/week6/kafka_cp/")
    .trigger(Trigger.Once())
    .start()
    .awaitTermination()

    
green_locations
    .selectExpr("CAST(PULocationID AS STRING) as key", "CAST(PULocationID as STRING) as value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "pickup_locations")
    .option("checkpointLocation","/mnt/c/work/data-camp/week6/kafka_cp/")
    .trigger(Trigger.Once())
    .start()
    .awaitTermination()

val df_all = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "pickup_locations")
  .option("startingOffsets", "earliest")
  .load()

val df_all_string = df_all.selectExpr("CAST(value AS STRING)")
val df_all_values = df_all_string.selectExpr("CAST(value AS LONG)")
val result = df_all_values.groupBy("value").count().orderBy(col("count").desc)

result.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate","false")
	  .trigger(Trigger.Once())
    .start()
    .awaitTermination()
