from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from utils.helper_data_utils import write_to_table, merge_to_table, detect_schema_drift

def read_latest_raw_json(
        base_path: str, 
        filename: str, 
        spark: SparkSession, 
        dbutils) -> DataFrame:
    """
    Reads the latest raw JSON file from a folder structure in DBFS.

    Parameters:
    - base_path (str): Base path where folders are stored.
    - filename (str): Name of the JSON file to read.
    - spark (SparkSession): Active Spark session.
    - dbutils: Databricks utility object.

    Returns:
    - DataFrame: Parsed Spark DataFrame from the latest folder.
    """
    folders = dbutils.fs.ls(base_path)
    latest_folder = sorted(folders, key=lambda x: x.name, reverse=True)[0].path
    print(f"Loading folder: {latest_folder}")
    return spark.read.option("multiline", "true").json(f"{latest_folder}/{filename}")

def ingest_entity(
    entity_config: dict,
    bronze_schema: str,
    protocol: str,
    spark: SparkSession
) -> None:
    """
    Ingests a single entity into the bronze layer.

    Parameters:
    - entity_config (dict): Configuration for the entity.
    - bronze_schema (str): Target schema name.
    - protocol (str): Global ingestion protocol ('HIST' or 'INCR').
    - spark (SparkSession): Active Spark session.

    The entity_config must include:
    - name (str): Entity name.
    - df (DataFrame): Source DataFrame.
    - protocol (str): Entity-specific protocol.
    Optional keys:
    - path (str): Column to select or explode.
    - explode (bool): Whether to explode the path.
    - alias (str): Alias for exploded column.
    - merge_key (str): Key to use for merge condition.
    """
    name = entity_config["name"]
    df = entity_config["df"]
    path = entity_config.get("path")
    explode = entity_config.get("explode", False)
    alias = entity_config.get("alias")
    merge_key = entity_config.get("merge_key")
    entity_protocol = entity_config["protocol"]

    # Extract and transform
    if explode and path:
        entity_df = df.select(F.explode(path).alias(alias)).select(f"{alias}.*")
    elif path:
        entity_df = df.select(path)
    else:
        entity_df = df

    # Detect schema drift
    detect_schema_drift(
        new_df=entity_df,
        table_name=f"{bronze_schema}.{name}",
        spark=spark
    )

    # Write or merge
    if protocol == "HIST":
        write_to_table(
            df=entity_df,
            table_name=f"{bronze_schema}.{name}"
        )
        print(f"[HIST] {name} written to {bronze_schema}.{name}.")
    elif entity_protocol == "INCR" and protocol == "INCR":
        merge_to_table(
            df=entity_df,
            table_name=f"{bronze_schema}.{name}",
            merge_condition=f"target.{merge_key} = source.{merge_key}",
            spark=spark
        )
        print(f"[INCR] {name} merged to {bronze_schema}.{name}.")
