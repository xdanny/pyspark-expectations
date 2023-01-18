import pytest

from expectations.decorator import Expectations, is_not_null, is_unique

pytestmark = pytest.mark.usefixtures("spark")

def test_unique_condition(spark):
    df_1 = spark.createDataFrame([("row1", 1), ("row1", 2), ("row3", 3)], ["row", "row_number"])
    expectation = Expectations(spark, rsd=0.01)

    @expectation.expect_or_drop([is_unique("row")])
    def read_dataframe(df):
        return df

    result = read_dataframe(df_1)
    print(result.collect())
    print(expectation.metrics.collect())
    expectation.plot_pie_with_total("DATASET 1")

def test_not_null_condition(spark):
    df_1 = spark.createDataFrame([("row1", 1), ("row1", 2), (None, 3)], ["row", "row_number"])
    expectation = Expectations(spark)

    @expectation.expect_or_drop([is_not_null("row")])
    def read_dataframe(df):
        return df

    result = read_dataframe(df_1)
    print(result.collect())
    print(expectation.metrics.collect())

def test_multiple_conditions(spark):
    df_1 = spark.createDataFrame([("row1", 1), ("row1", 2), (None, 3)], ["row", "row_number"])
    expectation = Expectations(spark)

    @expectation.expect_or_drop([is_not_null("row"), is_unique("row")])
    def read_dataframe(df):
        return df

    result = read_dataframe(df_1)
    print(result.collect())
    print(expectation.metrics.collect())
    expectation.plot_pie_with_total()
