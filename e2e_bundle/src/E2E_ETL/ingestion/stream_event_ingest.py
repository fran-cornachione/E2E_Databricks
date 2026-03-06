from pyspark import pipelines as dp

volume = "/Volumes/e2e_databricks/raw/storage/events"
schema_location = "/Volumes/e2e_databricks/bronze/stream/metadata"
checkpoint_location = "/Volumes/e2e_databricks/bronze/stream/metadata/checkpoint"

@dp.table(
    name="events"
)
def read_events():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(volume)
    )