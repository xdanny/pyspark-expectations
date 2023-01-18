from abc import ABC, abstractmethod
from functools import reduce, wraps
from typing import List

import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

foldl = lambda func, acc, xs: reduce(func, xs, acc)


class ColumnCondition(ABC):
    @abstractmethod
    def get_cols(self):
        pass


class UniqueCondition(ColumnCondition):

    def __init__(self, col):
        self.col = col

    def get_cols(self):
        return self.col


class FilterCondition(ColumnCondition):

    def __init__(self, left_col, right_col):
        self.left_col = left_col
        self.right_col = right_col

    def get_cols(self):
        return self.left_col, self.right_col


def is_not_null(col):
    return FilterCondition(col + " is not null", col + " is null")


def is_between(col, left: str, right: str):
    statement = " BETWEEN " + left + " AND " + right
    return FilterCondition(col + statement, col + " NOT " + statement)


def is_unique(col):
    return UniqueCondition(col)


class Expectations:

    def __init__(self, spark: SparkSession, rsd=0.05):
        self.spark = spark
        self.schema = StructType([StructField("condition", StringType(), True),
                                  StructField("dropped_records", IntegerType(), True),
                                  StructField("clean_records", IntegerType(), True)])
        emptyRDD = spark.sparkContext.emptyRDD()
        self.metrics = spark.createDataFrame(emptyRDD, schema=self.schema)
        self.rsd = rsd

    def expect_or_drop(self, conditions: List[FilterCondition]):
        def decorator(function):
            @wraps(function)
            def wrapper(*args, **kwargs):
                retval = function(*args, **kwargs)
                return foldl(self.apply_condition, retval, conditions)
            return wrapper
        return decorator

    def apply_condition(self, dataframe, condition):
        if isinstance(condition, FilterCondition):
            return self.filter_condition(dataframe, condition.get_cols())
        elif isinstance(condition, UniqueCondition):
            return self.is_unique_extend(dataframe, condition.get_cols())
        return dataframe

    def filter_condition(self, dataframe: DataFrame, left_right) -> DataFrame:
        left, right = left_right
        total_records = dataframe.count()
        dropped_records = dataframe.filter(right).count()
        df = self.spark.createDataFrame([(left, dropped_records, (total_records - dropped_records))], schema=self.schema)
        self.metrics = self.metrics.unionAll(df)
        return dataframe.filter(left)

    def is_unique_extend(self, dataframe: DataFrame, col) -> DataFrame:
        total_records = dataframe.select(F.col(col)).count()
        distinct_records = dataframe.select(F.approx_count_distinct(col, self.rsd)).collect()[0][0]
        dropped_records = total_records - distinct_records
        df = self.spark.createDataFrame([(col + " is unique", dropped_records, distinct_records)], schema=self.schema)
        self.metrics = self.metrics.unionAll(df)
        return dataframe.dropDuplicates([col])

    ## plot seaborn pie chart
    def plot_pie(self, title, figsize=(10, 10)):
        import matplotlib.pyplot as plt
        import seaborn as sns
        sns.set(style="whitegrid")
        fig, ax = plt.subplots(figsize=figsize)
        ax.set_title(title)
        ax.set_ylabel('Count')
        ax.set_xlabel('Condition')
        sns.barplot(x="condition", y="dropped_records", data=self.metrics.toPandas(), ax=ax)
        plt.show()

    ## plot seaborn pie chart with total and dropped records
    def plot_pie_with_total(self, figsize=(10, 10)):
        labels = ["clean_records", "dropped_records"]
        df = self.metrics.toPandas()
        size = len(df.index)
        if size == 1:
            fig, axs = plt.subplots(1)
            axs.pie(df.iloc[0][labels], labels=labels, autopct='%1.1f%%')
            axs.set_title(df.iloc[0]["condition"])
        else:
            fig, axs = plt.subplots(1, size, figsize=figsize)
            for i in range(size):
                axs[i].pie(df.iloc[i][labels], labels=labels, autopct='%1.1f%%')
                axs[i].set_title(df.iloc[i]["condition"])
        plt.show()
