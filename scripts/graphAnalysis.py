import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
from graphframes import GraphFrame

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

    rideshare_data_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv", header=True, inferSchema=True)
    taxi_zone_lookup_df = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv", header=True, inferSchema=True)

    #1. Define the structType
    vertexSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])
    
    edgeSchema = StructType([
        StructField("src", IntegerType(), True),
        StructField("dst", IntegerType(), True)
    ])
    
    #2.  Construct the dataframe for edges & vertices
    edges_df = rideshare_data_df.select(col("pickup_location").alias("src"),
                                        col("dropoff_location").alias("dst"))
    
    vertices_df = taxi_zone_lookup_df.select(col("LocationID").alias("id"), 
                                             col("Borough"), 
                                             col("Zone"), 
                                             col("service_zone"))
    
    edges_df.show(10, truncate=False)
    vertices_df.show(10, truncate=False)

    #3. Create a graph using vertices & edges
    graph = GraphFrame(vertices_df, edges_df)
    
    graph_edges = edges_df.alias("e") \
        .join(vertices_df.alias("src"), col("e.src") == col("src.id")) \
        .join(vertices_df.alias("dst"), col("e.dst") == col("dst.id")) \
        .selectExpr("struct(src.id, src.Borough, src.Zone, src.service_zone) as src",
                    "struct(src.id,dst.id) as edge",
                    "struct(dst.id, dst.Borough, dst.Zone, dst.service_zone) as dst")

    graph_edges.show(10, truncate=False)

    #4. count connected vertices with the same Borough and same service_zone
    connected_vertices_count = graph_edges.join(vertices_df, col("src.id") == col("id")) \
            .filter((col("src.Borough") == col("dst.Borough")) & (col("src.service_zone") == col("dst.service_zone"))) \
            .select(col("src.id").alias("src_id"), 
                    col("dst.id").alias("dst_id"), 
                    col("src.Borough").alias("Borough"), 
                    col("src.service_zone").alias("service_zone")) \
            .groupBy("src_id", "dst_id", "Borough", "service_zone").count()
    
    connected_vertices_count.show(10)

    # #5. Perform page ranking
    page_rank_results = graph.pageRank(resetProbability=0.17, tol=0.01)
    top_page_rank_vertices = page_rank_results.vertices.orderBy(col("pagerank").desc())
    top_page_rank_vertices.show(5)
    
    spark.stop()