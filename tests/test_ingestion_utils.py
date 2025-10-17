import pytest
from pyspark.sql import SparkSession
from ingestion.ingestion_utils import read_latest_raw_json, ingest_entity
from utils.helper_data_utils import write_to_table, detect_schema_drift, merge_to_table

# ----------------------------
# Tests for read_latest_raw_json
# ----------------------------

class MockDbutils:
    class FS:
        def ls(self, path):
            class FileInfo:
                def __init__(self, name, path):
                    self.name = name
                    self.path = path
            return [
                FileInfo("2025-10-15/", f"{path}/2025-10-15"),
                FileInfo("2025-10-16/", f"{path}/2025-10-16"),
                FileInfo("2025-10-17/", f"{path}/2025-10-17")
            ]
    fs = FS()

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .getOrCreate()

def test_read_latest_raw_json(spark, tmp_path):
    # Create mock JSON file in latest folder
    latest_folder = tmp_path / "2025-10-17"
    latest_folder.mkdir()
    file_path = latest_folder / "data.json"
    file_path.write_text('[{"id": 1, "name": "Test"}]')

    mock_dbutils = MockDbutils()
    df = read_latest_raw_json(str(tmp_path), "data.json", spark, mock_dbutils)

    assert df.count() == 1
    assert "id" in df.columns
    assert "name" in df.columns

# ----------------------------
# Tests for ingest_entity
# ----------------------------

def test_ingest_entity_hist(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    config = {
        "name": "hist_entity",
        "df": df,
        "protocol": "HIST"
    }

    ingest_entity(config, bronze_schema="default", protocol="HIST", spark=spark)

    result_df = spark.table("default.hist_entity")
    assert result_df.count() == 2
    assert "last_updated" in result_df.columns

def test_ingest_entity_incr(spark):
    df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    df2 = spark.createDataFrame([(2, "B_updated"), (3, "C")], ["id", "value"])

    config1 = {
        "name": "incr_entity",
        "df": df1,
        "protocol": "INCR",
        "merge_key": "id"
    }

    config2 = {
        "name": "incr_entity",
        "df": df2,
        "protocol": "INCR",
        "merge_key": "id"
    }

    ingest_entity(config1, bronze_schema="default", protocol="INCR", spark=spark)
    ingest_entity(config2, bronze_schema="default", protocol="INCR", spark=spark)

    result_df = spark.table("default.incr_entity").select("id", "value").orderBy("id")
    result = [tuple(row) for row in result_df.collect()]
    assert result == [(1, "A"), (2, "B_updated"), (3, "C")]
