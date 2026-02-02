"""DataFrame utility functions for data processing."""

from pyspark.sql import DataFrame
from pyspark.sql import types as T


def schema_to_hints(schema: T.StructType) -> str:
    """Convert StructType schema to cloudFiles.schemaHints format.

    Parameters
    ----------
    schema : T.StructType
        PySpark schema to convert.

    Returns
    -------
    str
        Schema hints string in format: "col1 TYPE, col2 TYPE, ..."

    Examples
    --------
    >>> from pyspark.sql import types as T
    >>> schema = T.StructType(
    ...     [
    ...         T.StructField("id", T.StringType(), nullable=False),
    ...         T.StructField("value", T.IntegerType(), nullable=True),
    ...     ]
    ... )
    >>> schema_to_hints(schema)
    'id string, value int'
    """
    hints = []
    for field in schema.fields:
        type_name = field.dataType.simpleString()
        hints.append(f"{field.name} {type_name}")

    return ", ".join(hints)


def drop_technical_columns(df: DataFrame) -> DataFrame:
    """Drop all columns that start with an underscore.

    Commonly used to remove metadata columns (like _rescued, _file_path, etc.)
    before writing to silver/gold tables.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame that may contain columns with leading underscores.

    Returns
    -------
    DataFrame
        DataFrame with all underscore-prefixed columns removed.

    Examples
    --------
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame([(1, "a", "meta")], ["id", "value", "_metadata"])
    >>> result = drop_technical_columns(df)
    >>> result.columns
    ['id', 'value']
    """
    cols_to_drop = [c for c in df.columns if c.startswith("_")]
    return df.drop(*cols_to_drop)
