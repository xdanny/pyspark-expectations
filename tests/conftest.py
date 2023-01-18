import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("some-app-name") \
        .getOrCreate()

    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    return spark_session
