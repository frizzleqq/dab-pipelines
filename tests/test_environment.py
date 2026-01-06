from pyspark.sql import types as T


def test_spark_setup(spark):
    schema = T.StructType(
        [
            T.StructField("key", T.IntegerType()),
            T.StructField("value", T.IntegerType()),
        ]
    )
    df = spark.createDataFrame(
        data=[
            (1, 10),
            (2, 20),
            (3, 30),
        ],
        schema=schema,
    )
    assert df.count() == 3
