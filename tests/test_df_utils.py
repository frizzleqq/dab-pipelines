"""Unit tests for DataFrame utility functions."""

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from dab_pipelines_etl.machine_data.df_utils import drop_technical_columns, schema_to_hints


class TestSchemaToHints:
    """Tests for schema_to_hints function."""

    def test_simple_schema(self):
        """Test conversion of a simple schema."""
        schema = T.StructType(
            [
                T.StructField("id", T.StringType(), nullable=False),
                T.StructField("name", T.StringType(), nullable=True),
            ]
        )
        result = schema_to_hints(schema)
        assert result == "id string, name string"

    def test_multiple_types(self):
        """Test schema with different data types."""
        schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), nullable=False),
                T.StructField("value", T.DoubleType(), nullable=True),
                T.StructField("timestamp", T.TimestampType(), nullable=True),
                T.StructField("is_active", T.BooleanType(), nullable=True),
            ]
        )
        result = schema_to_hints(schema)
        assert result == "id int, value double, timestamp timestamp, is_active boolean"

    def test_empty_schema(self):
        """Test conversion of empty schema."""
        schema = T.StructType([])
        result = schema_to_hints(schema)
        assert result == ""

    def test_single_field(self):
        """Test schema with a single field."""
        schema = T.StructType([T.StructField("machine_id", T.StringType(), nullable=False)])
        result = schema_to_hints(schema)
        assert result == "machine_id string"

    def test_complex_types(self):
        """Test schema with array and map types."""
        schema = T.StructType(
            [
                T.StructField("tags", T.ArrayType(T.StringType()), nullable=True),
                T.StructField("metadata", T.MapType(T.StringType(), T.StringType()), nullable=True),
            ]
        )
        result = schema_to_hints(schema)
        assert result == "tags array<string>, metadata map<string,string>"


class TestDropTechnicalColumns:
    """Tests for drop_technical_columns function."""

    def test_drop_single_underscore_column(self, spark: SparkSession):
        """Test dropping a single column with underscore prefix."""
        df = spark.createDataFrame([(1, "a", "meta")], ["id", "value", "_metadata"])
        result = drop_technical_columns(df)
        assert result.columns == ["id", "value"]
        assert result.count() == 1

    def test_drop_multiple_underscore_columns(self, spark: SparkSession):
        """Test dropping multiple columns with underscore prefix."""
        df = spark.createDataFrame(
            [(1, "a", "file.json", "path/to/file", 100)],
            ["id", "value", "_file_name", "_file_path", "_file_size"],
        )
        result = drop_technical_columns(df)
        assert result.columns == ["id", "value"]
        assert result.count() == 1

    def test_no_underscore_columns(self, spark: SparkSession):
        """Test DataFrame with no underscore columns."""
        df = spark.createDataFrame([(1, "a", "b")], ["id", "value", "name"])
        result = drop_technical_columns(df)
        assert result.columns == ["id", "value", "name"]
        assert result.count() == 1

    def test_all_underscore_columns(self, spark: SparkSession):
        """Test DataFrame with only underscore columns."""
        df = spark.createDataFrame([(1, "a", "b")], ["_id", "_value", "_name"])
        result = drop_technical_columns(df)
        assert result.columns == []
        assert result.count() == 1

    def test_empty_dataframe(self, spark: SparkSession):
        """Test with empty DataFrame."""
        schema = T.StructType(
            [
                T.StructField("id", T.IntegerType()),
                T.StructField("_metadata", T.StringType()),
            ]
        )
        df = spark.createDataFrame([], schema)
        result = drop_technical_columns(df)
        assert result.columns == ["id"]
        assert result.count() == 0

    def test_preserves_data(self, spark: SparkSession):
        """Test that data is preserved after dropping columns."""
        df = spark.createDataFrame(
            [(1, "alice", "meta1"), (2, "bob", "meta2"), (3, "charlie", "meta3")],
            ["id", "name", "_metadata"],
        )
        result = drop_technical_columns(df)
        assert result.count() == 3
        data = result.collect()
        assert data[0]["id"] == 1
        assert data[0]["name"] == "alice"
        assert data[1]["id"] == 2
        assert data[1]["name"] == "bob"

    def test_column_order_preserved(self, spark: SparkSession):
        """Test that column order is preserved (excluding dropped columns)."""
        df = spark.createDataFrame(
            [(1, "a", "meta1", "b", "meta2")],
            ["id", "first", "_metadata1", "second", "_metadata2"],
        )
        result = drop_technical_columns(df)
        assert result.columns == ["id", "first", "second"]
