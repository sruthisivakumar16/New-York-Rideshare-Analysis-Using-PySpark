import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, year, month, concat, desc, lit, avg, col, dayofmonth, sum

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("nyc")\
        .getOrCreate()
    
    s3_data_repository_bucket = os.environ.get('DATA_REPOSITORY_BUCKET')
    s3_endpoint_url = os.environ.get('S3_ENDPOINT_URL') + ':' + os.environ.get('BUCKET_PORT')
    s3_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    s3_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    s3_bucket = os.environ.get('BUCKET_NAME')

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    #TASK 1 - MERGING DATASETS

    #1. Load datasets
    rideshare_data_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv", header=True, inferSchema=True)
    taxi_zone_lookup_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv", header=True, inferSchema=True)

    #2. Join datasets
    #step 1 - based on pickup_location
    joined_df = rideshare_data_df.join(taxi_zone_lookup_df.withColumnRenamed("LocationID", "pickup_location") \
                                  .withColumnRenamed("Borough", "Pickup_Borough") \
                                  .withColumnRenamed("Zone", "Pickup_Zone") \
                                  .withColumnRenamed("service_zone", "Pickup_service_zone"),
                                  on="pickup_location", how="left")

    #step 2 - based on dropoff_location
    joined_df = joined_df.join(taxi_zone_lookup_df.withColumnRenamed("LocationID", "dropoff_location") \
                               .withColumnRenamed("Borough", "Dropoff_Borough") \
                               .withColumnRenamed("Zone", "Dropoff_Zone") \
                               .withColumnRenamed("service_zone", "Dropoff_service_zone"),
                               on="dropoff_location", how="left")

    #3. Convert the date field to "yyyy-MM-dd" format
    joined_df = joined_df.withColumn("date", from_unixtime(joined_df["date"], "yyyy-MM-dd"))

    #4: Print the number of rows and schema
    print("Number of Rows:", joined_df.count())
    joined_df.printSchema()
#--------------------------------------------------End of task 1-----------------------------------------------------
    #TASK 2 - AGGREGATION OF DATA
    # Convert date field to month format
    joined_df = joined_df.withColumn("month", month("date"))

    # #1. Count the number of trips for each business in each month
    trips_count_df = joined_df.groupBy("business", "month").count()
    trips_count_df.coalesce(1).write.mode("overwrite").csv("s3a://" + s3_bucket + "/task2/trips_count", header=True)

    #2. Calculate platform's profits for each business in each month
    platform_profits_df = joined_df.groupBy("business", "month").sum("rideshare_profit")
    platform_profits_df.coalesce(1).write.csv("s3a://" + s3_bucket + "/task2/platform_profits", header=True)

    #3. Calculate driver's earnings for each business in each month
    driver_earnings_df = joined_df.groupBy("business", "month").sum("driver_total_pay")
    driver_earnings_df.coalesce(1).write.csv("s3a://" + s3_bucket + "/task2/driver_earnings", header=True)
    
#--------------------------------------------------End of task 2----------------------------------------------------- 
    # TASK 3 - TOP K-PROCESSING
    
    #1. Top 5 popular pickup boroughs each month
    top_pickup_boroughs_df = joined_df.groupBy("Pickup_Borough", "month").count().orderBy("month", desc("count")).withColumnRenamed("count", "trip_count")
    top_pickup_boroughs_df.show(5)
    
    #2. Top 5 popular dropoff boroughs each month
    top_dropoff_boroughs_df = joined_df.groupBy("Dropoff_Borough", "month").count().orderBy("month", desc("count")).withColumnRenamed("count", "trip_count")
    top_dropoff_boroughs_df.show(5)
    
    #3. Top 30 earning routes
    top_earning_routes_df = joined_df.groupBy("Pickup_Borough", "Dropoff_Borough").sum("driver_total_pay").orderBy(desc("sum(driver_total_pay)")).withColumnRenamed("sum(driver_total_pay)", "total_profit")
    top_earning_routes_df = top_earning_routes_df.withColumn("Route", concat(col("Pickup_Borough"), lit(" to "), col("Dropoff_Borough")))
    top_earning_routes_df = top_earning_routes_df.select("Route", "total_profit")
    top_earning_routes_df.show(30, truncate=False)
#--------------------------------------------------End of task 3-----------------------------------------------------
    # #TASK 4 - AVERAGE OF DATA
    #1. Calculate average 'driver_total_pay' during different 'time_of_day' periods
    average_pay_df = joined_df.groupBy("time_of_day").agg(avg("driver_total_pay").alias("average_driver_total_pay")).orderBy(desc("average_driver_total_pay"))
    average_pay_df.show()
    
    #2. Calculate average 'trip_length' during different 'time_of_day' periods
    average_trip_length_df = joined_df.groupBy("time_of_day").agg(avg("trip_length").alias("average_trip_length")).orderBy(desc("average_trip_length"))
    average_trip_length_df.show()
    
    #3. Calculate average earned per mile for each 'time_of_day' period
    average_earning_per_mile_df = average_pay_df.join(average_trip_length_df, "time_of_day")
    average_earning_per_mile_df = average_earning_per_mile_df.withColumn("average_earning_per_mile", col("average_driver_total_pay") / col("average_trip_length"))
    average_earning_per_mile_df = average_earning_per_mile_df.select("time_of_day", "average_earning_per_mile")
    average_earning_per_mile_df.show()
#--------------------------------------------------End of task 4-----------------------------------------------------
    #TASK 5 - FINDING ANOMALIES
    #1. Calculate average waiting time for january
    january_data_df = joined_df.filter(month("date") == 1)
    average_waiting_time_df = january_data_df.groupBy(dayofmonth("date").alias("day")).agg(avg("request_to_pickup").alias("average_waiting_time"))
                                                                                                                          
    average_waiting_time_df.coalesce(1).write.mode("overwrite").csv("s3a://" + s3_bucket + "/task5/avg_waiting_time", header=True)
#--------------------------------------------------End of task 5-----------------------------------------------------
    #TASK 6 - FILTERING DATA
     #1. Trip counts greater than 0 and less than 1000 for different 'Pickup_Borough' at different 'time_of_day'
    filtered_df = joined_df.filter((col("pickup_location").isNotNull()) & (col("dropoff_location").isNotNull())) \
                    .groupBy("Pickup_Borough", "time_of_day") \
                    .count() \
                    .filter(col("count") > 0) \
                    .filter(col("count") < 1000)
    
    filtered_df.show() 

    
    #2. Calculate the number of trips for each 'Pickup_Borough' in the evening time
    evening_trips_df = joined_df.filter(col("time_of_day") == "evening") \
                          .groupBy("Pickup_Borough", "time_of_day") \
                          .count()
    
    evening_trips_df.show()
    
    #3. Calculate the number of trips that started in Brooklyn and ended in Staten Island
    brooklyn_to_staten_island_df = joined_df.filter((col("Pickup_Borough") == "Brooklyn") & (col("Dropoff_Borough") == "Staten Island")) \
                                      .select("Pickup_Borough", "Dropoff_Borough", "Pickup_Zone") \
                                      .limit(10)
    
    brooklyn_to_staten_island_df.show(truncate=False)
#--------------------------------------------------End of task 6-----------------------------------------------------
    #TASK 7 - ROUTES ANALYSIS
    joined_df = joined_df.withColumn("Route", concat(col("Pickup_Zone"), lit(" to "), col("Dropoff_Zone")))
    route_counts_df = joined_df.groupBy("Route") \
        .agg(sum((col("business") == "Uber").cast("int")).alias("uber_count"),
             sum((col("business") == "Lyft").cast("int")).alias("lyft_count"))
    
    route_counts_df = route_counts_df.withColumn("total_count", route_counts_df["uber_count"] + route_counts_df["lyft_count"])
    
    top_routes_df = route_counts_df.orderBy(col("total_count").desc()).limit(10)

    top_routes_df.show(truncate=False)
#--------------------------------------------------End of task 7-----------------------------------------------------
    spark.stop()