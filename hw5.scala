import org.apache.spark.sql.types._
import org.apache.spark.sql.function.{to_date,col,lit,unix_timestamp}

print(spark.version)

val trips_schema = StructType(Array(StructField("dispatching_base_num", StringType, true), StructField("pickup_datetime", TimestampType, true), StructField("dropoff_datetime", TimestampType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("SR_Flag", StringType, true), StructField("Affiliated_base_number", StringType, true)))
val dftrips = spark.read.option("header",true).schema(trips_schema).csv("c:/work/data-camp/week5/fhvhv_tripdata_2021-06.csv.gz")

dftrips.repartition(12).write.parquet("c:/work/data-camp/week5/pq")
val trips = dftrips.repartition(8)

import org.apache.spark.sql.functions.{year, month}

trips.withColumn("pickup_date", to_date(col("pickup_datetime"), "yyyy-MM-dd")).
        filter(col("pickup_date") === lit("2021-06-15")).
        count()

val zones_schema = StructType(Array(StructField("LocationID", IntegerType, true), StructField("Borough", StringType, true), StructField("Zone", StringType, true), StructField("service_zone", StringType, true)))

trips.withColumn("pickup_date", to_date(col("pickup_datetime"), "yyyy-MM-dd")).withColumn("trip_time_hours", (unix_timestamp(col("dropoff_datetime"))-unix_timestamp(col("pickup_datetime"))).cast("bigint")/3600.0).groupBy("pickup_date").agg(max($"trip_time_hours")).orderBy(max($"trip_time_hours").desc).take(1)

val zdf = spark.read.option("header",true).schema(zones_schema).csv("c:/work/data-camp/week5/taxi_zone_lookup.csv")

trips.createOrReplaceTempView("trips")
zdf.createOrReplaceTempView("zones")

spark.sql("""
  select Zone from trips inner join zones on trips.PULocationID=zones.LocationID
  where year(pickup_datetime)=2021 and month(pickup_datetime)=6
  group by Zone
  order by count(1) desc
""").take(1)
