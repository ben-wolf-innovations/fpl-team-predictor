from pyspark.sql import DataFrame
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