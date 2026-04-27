import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="silver_artist",
    comment="Silver layer - Artist com tipos corretos"
)
def silver_artist():
    return spark.read \
        .format("delta") \
        .table("projeto_engenharia.bronze.artist") \
        .select(
            F.col("ArtistId").cast("int").alias("artist_id"),
            F.coalesce(F.trim(F.col("Name")), F.lit("Desconhecido")).alias("artist_nome"),
            F.current_timestamp().alias("silver_processed_at")
        ) \
        .filter(F.col("artist_id").isNotNull())