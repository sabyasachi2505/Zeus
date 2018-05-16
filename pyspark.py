from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Python Spark SQL").getOrCreate()

def read_data(location, type):
    """
    This helps us read various filetypes directly into pyspark datframes. This is an allround solution.
    """
    
    ftype = {
      'avro' : "com.databricks.spark.avro",
      'parquet' : "parquet",
      'csv' : "com.databricks.spark.csv",
      'json' : "json" 
    }
    
    return spark.read \
    .format(ftype[''.join(e for e in type if e.isalnum())]) \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(location)