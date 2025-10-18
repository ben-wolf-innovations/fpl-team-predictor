import pytest
from pyspark.sql import SparkSession
from utils.helper_data_utils import write_to_table, detect_schema_drift, merge_to_table

# ----------------------------
# Fixtures
# ----------------------------
@pytest.fixture(scope="session")

def spark():
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.13:2.4.0") \
        .getOrCreate()
    
    yield spark
    spark.stop()

# ----------------------------
# Tests for write_to_table
# ----------------------------
def test_write_to_table_managed(spark):
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    table_name = "test_table"

    write_to_table(df, table_name=table_name, save_as_table=True)

    result_df = spark.table(table_name)
    assert result_df.count() == 2
    assert "last_updated" in result_df.columns

# ----------------------------
# Tests for detect_schema_drift
# ----------------------------

def test_detect_schema_drift_added_column(spark):
    existing_df = spark.createDataFrame([(1, "A")], ["id", "name"])
    existing_df.write.format("delta").mode("overwrite").saveAsTable("test_table_drift")

    new_df = spark.createDataFrame([(1, "A", "extra")], ["id", "name", "new_col"])

    result = detect_schema_drift(new_df, "test_table_drift", spark)
    assert result is True

def test_detect_schema_drift_removed_column(spark):
    existing_df = spark.createDataFrame([(1, "A", "extra")], ["id", "name", "new_col"])
    existing_df.write.format("delta").mode("overwrite").saveAsTable("test_table_drift_2")

    new_df = spark.createDataFrame([(1, "A")], ["id", "name"])

    result = detect_schema_drift(new_df, "test_table_drift_2", spark)
    assert result is True

def test_detect_schema_drift_no_change(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "name"])
    df.write.format("delta").mode("overwrite").saveAsTable("test_table_drift_3")

    new_df = spark.createDataFrame([(2, "B")], ["id", "name"])

    result = detect_schema_drift(new_df, "test_table_drift_3", spark)
    assert result is False

def test_detect_schema_drift_first_write(spark):
    new_df = spark.createDataFrame([(1, "A")], ["id", "name"])
    result = detect_schema_drift(new_df, "non_existent_table", spark)
    assert result is False

# ----------------------------
# Tests for merge_to_table
# ----------------------------

def test_merge_to_table_upsert(spark):
    table_name = "test_merge_table"

    # Initial data
    initial_df = spark.createDataFrame([
        (1, "A"),
        (2, "B")
    ], ["id", "value"])

    merge_to_table(
        df=initial_df,
        table_name=table_name,
        merge_condition="target.id = source.id",
        spark=spark
    )

    # New data with update and insert
    new_df = spark.createDataFrame([
        (2, "B_updated"),  # update
        (3, "C")           # insert
    ], ["id", "value"])

    merge_to_table(
        df=new_df,
        table_name=table_name,
        merge_condition="target.id = source.id",
        spark=spark
    )

    result_df = spark.table(table_name).select("id", "value").orderBy("id")
    result = [tuple(row) for row in result_df.collect()]

    assert result == [(1, "A"), (2, "B_updated"), (3, "C")]
