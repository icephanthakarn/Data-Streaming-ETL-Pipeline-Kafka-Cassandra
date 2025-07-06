# app/consumer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

def write_to_cassandra(write_df, table_name, keyspace_name):
    # ฟังก์ชันนี้ถูกต้องแล้ว ไม่ต้องแก้ไข
    write_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table=table_name, keyspace=keyspace_name) \
        .save()
    print(f"Data written to Cassandra table {table_name}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MovieStreamConsumer") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # --- กำหนด Schema ที่ถูกต้อง ---
    # Schema สำหรับข้อมูลหนังจาก movies_metadata.csv
    movies_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("vote_average", FloatType(), True),
        StructField("vote_count", IntegerType(), True)
    ])

    # Schema สำหรับข้อมูลเรตติ้งจาก ratings_small.csv
    ratings_schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True)
    ])

    # --- ประมวลผล movies_metadata topic ---
    movies_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "movies_metadata") \
        .option("startingOffsets", "earliest") \
        .load()

    movies_transformed_df = movies_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), movies_schema).alias("data")) \
        .select("data.*") \
        .withColumnRenamed("id", "movie_id") # เปลี่ยนชื่อคอลัมน์ให้ตรงกับใน Cassandra

    movies_query = movies_transformed_df.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_cassandra(df, "movies", "movie_data")) \
        .outputMode("update") \
        .start()

    # --- ประมวลผล movie_ratings topic ---
    ratings_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "movie_ratings") \
        .option("startingOffsets", "earliest") \
        .load()

    ratings_transformed_df = ratings_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), ratings_schema).alias("data")) \
        .select("data.*") \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("movieId", "movie_id") # เปลี่ยนชื่อคอลัมน์ให้ตรงกับใน Cassandra

    ratings_query = ratings_transformed_df.writeStream \
        .foreachBatch(lambda df, epoch_id: write_to_cassandra(df, "ratings", "movie_data")) \
        .outputMode("update") \
        .start()
        
    spark.streams.awaitAnyTermination()