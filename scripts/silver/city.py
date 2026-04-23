from pyspark.sql.functions import *
import pyspark.pipelines as dp

# ==========================================================================================================
# City Table Configuration :
# ==========================================================================================================

@dp.materialized_view(
    name = 'transportation.silver.city' ,
    comment = 'Silver City Table' ,
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)


def silver_city() :
    df = spark.read.table('transportation.bronze.city')

    df = df.select(
        col("city_id").alias("city_id"),
        col("city_name").alias("city_name"),
        col("ingest_datetime").alias("bronze_ingest_timestamp")
    
    )

    df = df.withColumn(
        'silver_processed_timestamp' ,
        current_timestamp()
    )

    return df
