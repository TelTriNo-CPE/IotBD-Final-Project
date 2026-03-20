import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.validations import filter_valid_transactions


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Testing").getOrCreate()


# TODO: Add a test for filter_valid_transactions — removes null amounts
# Hint: Include rows with None amount, verify they are removed

def test_filter_remove_empty(spark):
    data = [
        ("TXN1", "2020-01-01", "M1", "item", "C1", "MER1", "", "cash"),
        ("TXN2", "2020-01-01", "M1", "item", "C1", "MER1", "10", "cash"),
    ]

    columns = [
        "transaction_id","date","member_id","item_name",
        "category_id","merchant_id","amount","payment_method"
    ]

    df = spark.createDataFrame(data, columns)

    result = filter_valid_transactions(df)

    assert result.count() == 1

def test_filter_remove_null(spark):
    data = [
        ("TXN1", "2020-01-01", "M1", "item", "C1", "MER1", None, "cash"),
        ("TXN2", "2020-01-01", "M1", "item", "C1", "MER1", "20", "cash"),
    ]

    columns = [
        "transaction_id","date","member_id","item_name",
        "category_id","merchant_id","amount","payment_method"
    ]

    df = spark.createDataFrame(data, columns)

    result = filter_valid_transactions(df)

    assert result.count() == 1




# TODO: Add a test for filter_valid_transactions — removes out-of-range dates
# Hint: Include a row with date "2010-05-15", verify it is removed
def test_filter_date_range(spark):
    data = [
        ("TXN1", "2010-05-15", "M1", "item", "C1", "MER1", "10", "cash"),  # ❌
        ("TXN2", "2020-01-01", "M1", "item", "C1", "MER1", "20", "cash"),  # ✅
    ]

    columns = [
        "transaction_id","date","member_id","item_name",
        "category_id","merchant_id","amount","payment_method"
    ]

    df = spark.createDataFrame(data, columns)

    result = filter_valid_transactions(df)

    assert result.count() == 1



# TODO: Add a test for filter_valid_transactions — keeps refunds (negative amounts)
# Hint: Include a row with amount -25.0, verify it is kept
def test_keep_negative_amount(spark):
    data = [
        ("TXN1", "2020-01-01", "M1", "item", "C1", "MER1", "-25", "cash"),
    ]

    columns = [
        "transaction_id","date","member_id","item_name",
        "category_id","merchant_id","amount","payment_method"
    ]

    df = spark.createDataFrame(data, columns)

    result = filter_valid_transactions(df)

    assert result.count() == 1