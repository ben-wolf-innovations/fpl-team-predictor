import pytest
from pyspark.sql import SparkSession
from ingest_etl_utils import write_to_table

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestWriteToTable") \
        .master("local[*]") \
        .getOrCreate()

def test_write_to_table_managed(spark, tmp_path):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    table_name = "test_table"

    write_to_table(df, table_name=table_name, save_as_table=True)

    result_df = spark.table(table_name)
    assert result_df.count() == 2
    assert "last_updated" in result_df.columns