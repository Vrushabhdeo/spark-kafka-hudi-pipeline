import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

def read_yaml_from_s3a(spark, path):
    rdd = spark.sparkContext.textFile(path)
    yaml_content = "\n".join(rdd.collect())
    return yaml.safe_load(yaml_content)

def build_spark_session(spark_conf):
    builder = SparkSession.builder.appName(spark_conf.get("appName", "KafkaSparkApp"))
    if spark_conf.get("master"):
        builder = builder.master(spark_conf["master"])
    for key, value in spark_conf.get("properties", {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(spark_conf.get("sparkLogLevel", "WARN"))
    return spark

def check_kafka_connectivity(spark, kafka_options):
    try:
        # Try to read a single message from Kafka
        df = (
            spark.read
            .format("kafka")
            .options(**kafka_options)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        count = df.count()
        print(f"Kafka connectivity check: Success. Messages available: {count}")
        return True
    except Exception as e:
        print(f"Kafka connectivity check: Failed.\nError: {e}")
        return False

def main(common_config_path, job_config_path):
    # Minimal Spark to read configs first
    base_spark = SparkSession.builder.appName("InitLoader").getOrCreate()
    common_config = read_yaml_from_s3a(base_spark, common_config_path)
    job_config = read_yaml_from_s3a(base_spark, job_config_path)
    base_spark.stop()

    spark_conf = common_config.get("sparkConfig", {})
    kafka_props = common_config["kafkaConfigs"]["kafkaProducerProperties"]
    spark = build_spark_session(spark_conf)

    kafka_options = {f"kafka.{k}": str(v) for k, v in kafka_props.items()}
    kafka_options.update({
        "subscribe": ",".join(job_config.get("kafka-topic", [])),
    })

    # Check Kafka connectivity
    if not check_kafka_connectivity(spark, kafka_options):
        print("Exiting due to Kafka connectivity failure.")
        sys.exit(1)

    # Read from Kafka as batch
    df = (
        spark.read
        .format("kafka")
        .options(**kafka_options)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # Filter for the specific offset and print it
    filtered = df.filter(df.offset == int(112766710))
    filtered.select("offset", "value").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit kafka_stream_job.py <common-config.yaml> <job-config.yaml>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])

"""

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.0 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.threads.keepalivetime=60000 \
  --conf spark.hadoop.fs.s3a.connection.timeout=30000 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=30000 \
  --conf spark.hadoop.fs.s3a.socket.timeout=30000 \
  --conf spark.hadoop.fs.s3a.multipart.purge.age=86400000 \
  --conf spark.hadoop.fs.s3a.multipart.purge.interval=86400000 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /app/scripts/kafka_stream.py s3a://pe-skull-data/pe/configs/common-config.yaml s3a://pe-skull-data/pe/configs/job-config.yaml


spark-submit   --master spark://spark-master:7077   --deploy-mode client   --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.0   --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000   --conf spark.hadoop.fs.s3a.access.key=minioadmin   --conf spark.hadoop.fs.s3a.secret.key=minioadmin   --conf spark.hadoop.fs.s3a.path.style.access=true   --conf spark.hadoop.fs.s3a.threads.keepalivetime=60000   --conf spark.hadoop.fs.s3a.connection.timeout=30000   --conf spark.hadoop.fs.s3a.connection.establish.timeout=30000   --conf spark.hadoop.fs.s3a.socket.timeout=30000   --conf spark.hadoop.fs.s3a.multipart.purge.age=86400000   --conf spark.hadoop.fs.s3a.multipart.purge.interval=86400000   --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider   /app/scripts/kafka_stream.py s3a://pe-skull-data/pe/configs/common-config.yaml s3a://pe-skull-data/pe/configs/job-config.yaml



minio_accessibility

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.threads.keepalivetime=60000 \
  --conf spark.hadoop.fs.s3a.connection.timeout=30000 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=30000 \
  --conf spark.hadoop.fs.s3a.socket.timeout=30000 \
  --conf spark.hadoop.fs.s3a.multipart.purge.age=86400000 \
  --conf spark.hadoop.fs.s3a.multipart.purge.interval=86400000 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /app/scripts/minio_accessibility.py


/Users/vrushabh.deokar/PycharmProjects/dev-repo/spark_local
docker-compose build --no-cache
docker-compose down
docker-compose up
nc -zv localhost 7077

http://localhost:9001/browser/pe-skull-data

"""