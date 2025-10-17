from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql import functions as F

def write_to_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
    merge_schema: bool = True,
    partition_by: list[str] = None,
    path: str = None,
    save_as_table: bool = True
) -> None:
    """
    Generalised Delta write helper for bronze layer.

    Parameters:
    - df (DataFrame): Spark DataFrame to write.
    - table_name (str): Name of the Delta table (used if save_as_table=True).
    - mode (str): Write mode ('overwrite', 'append', 'ignore', 'error', etc.).
    - merge_schema (bool): Whether to merge schema on write.
    - partition_by (list[str], optional): List of columns to partition by.
    - path (str, optional): Path to save the Delta table (used if save_as_table=False).
    - save_as_table (bool): If True, saves as managed table; else saves to path.

    Raises:
    - ValueError: If neither save_as_table nor path is properly specified.
    """

    df_with_ts = df.withColumn("last_updated", F.current_timestamp())

    writer = df_with_ts.write.format("delta").mode(mode)

    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    elif mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if save_as_table:
        writer.saveAsTable(table_name)
    elif path:
        writer.save(path)
    else:
        raise ValueError("Either save_as_table must be True or a path must be provided.")

def detect_schema_drift(new_df: DataFrame, table_name: str, spark: SparkSession) -> bool:
    """
    Detects schema drift between a new DataFrame and an existing Delta table.

    Parameters:
    - new_df (DataFrame): The new DataFrame to compare.
    - table_name (str): The name of the existing Delta table.
    - spark (SparkSession): The active Spark session.

    Returns:
    - bool: True if schema drift is detected, False otherwise.
    """
    try:
        existing_df = spark.table(table_name)
        existing_fields = set(field.name for field in existing_df.schema.fields if field.name != "last_updated")
        new_fields = set(field.name for field in new_df.schema.fields if field.name != "last_updated")

        added = new_fields - existing_fields
        removed = existing_fields - new_fields

        if added or removed:
            print(f"Schema drift detected in {table_name}")
            if added:
                print(f"Added fields: {added}")
            if removed:
                print(f"Removed fields: {removed}")
            return True
        return False
    except Exception:
        print(f"ℹ No existing table found for {table_name}. Assuming first write.")
        return False

def merge_to_table(
    df: DataFrame,
    table_name: str,
    merge_condition: str,
    spark: SparkSession,
    partition_by: list[str] = None
) -> None:
    """
    Performs an upsert (merge) into a Delta table.

    Parameters:
    - df (DataFrame): Incoming DataFrame to merge.
    - table_name (str): Target Delta table name.
    - merge_condition (str): SQL condition for matching rows.
    - spark (SparkSession): Active Spark session.
    - partition_by (list[str], optional): Columns to partition by on initial write.

    If the table does not exist, it will be created using write_to_table.
    """
    df_with_ts = df.withColumn("last_updated", F.current_timestamp())

    if not spark.catalog.tableExists(table_name):
        write_to_table(
            df=df_with_ts,
            table_name=table_name,
            partition_by=partition_by
        )
    else:
        delta_table = DeltaTable.forName(spark, table_name)
        (
            delta_table.alias("target")
            .merge(
                source=df_with_ts.alias("source"),
                condition=merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )