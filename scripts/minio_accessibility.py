from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.text("s3a://pe-skull-data/pe/configs/common-config.yaml")
df.show(truncate=False)