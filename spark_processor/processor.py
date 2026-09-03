import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, coalesce, lit, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml import PipelineModel

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
MODEL_PATH = "/app/model/spark_fake_news_detection_pipeline"

PG_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/news_db")
PG_USER = os.getenv("POSTGRES_USER", "admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "adminpassword")
PG_TABLE = "classified_news"

spark = SparkSession.builder \
        .appName("FakeNewsStreaming") \
        .master("local[*]") \
        .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")


# input JSON schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("source", StringType(), True)
])


@udf(returnType=FloatType())
def extract_confidence(probability_vector):
    if probability_vector is not None:
        return(float(max(probability_vector)))
    return 0.0



# Reading from Kafka stream
raw_stream = spark.readStream \
                  .format("kafka") \
                  .option("kafka.bootstrap.servers", KAFKA_SERVER) \
                  .option("subscribe", "raw-news") \
                  .option("startingOffsets", "latest") \
                  .option("kafka.consumer.request.timeout.ms", "120000") \
                  .option("kafka.consumer.session.timeout.ms", "60000") \
                  .option("kafka.request.timeout.ms", "120000") \
                  .option("failOnDataLoss", "false") \
                  .load()


parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

final_stream = parsed_stream.withColumn(
    "full_text",
    concat_ws(" ", coalesce(col("headline"), lit("")), coalesce(col("text"), lit("")))
)


# Loading Trained Model
model = PipelineModel.load(MODEL_PATH)
print("Model successfully loaded.")


# Live Inference
predictions = model.transform(final_stream)

processed_df = predictions.select(
    col("id"),
    coalesce(col("source"), lit("Sconosciuto")).alias("source"),
    col("headline"),
    col("prediction").cast("integer").alias("is_fake"),
    extract_confidence(col("probability")).alias("confidence"),
    current_timestamp().alias("processed_at")
)


# Results

def write_batch_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        record_count = batch_df.count()
        batch_df.write \
            .format("jdbc") \
            .option("url", PG_URL) \
            .option("dbtable", PG_TABLE) \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"[STREAM] Batch #{batch_id} saved to PostgreSQL. News Count: {record_count}")


query = processed_df.writeStream \
    .foreachBatch(write_batch_to_postgres) \
    .start()
    

print(f"[STREAM] Waiting for data from 'raw-news' Kafka topic...")
query.awaitTermination()