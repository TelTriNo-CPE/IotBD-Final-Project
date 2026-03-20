import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.transformations import categorize_spending, enrich_with_lookups


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("Testing") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.port.maxRetries", "100") \
        .getOrCreate()


def test_categorize_spending_basic(spark):
    """This test is provided as a starting point. It will FAIL until you implement categorize_spending."""
    input_data = [(5.0,), (25.0,), (100.0,), (500.0,)]
    input_df = spark.createDataFrame(input_data, ["amount"])

    expected_data = [
        (5.0, "micro"),
        (25.0, "small"),
        (100.0, "medium"),
        (500.0, "large"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["amount", "spending_tier"])

    result_df = categorize_spending(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


# TODO: Add a test for categorize_spending with negative amounts (refunds)
# Hint: -25.0 should be categorized as "small" (based on absolute value)
from src.etl.transformations import categorize_spending

def test_categorize_negative_amount(spark):
    data = [
        ("TXN1", "2020-01-01", "M1", "item", "C1", "MER1", "-25", "cash"),
    ]

    columns = [
        "transaction_id","date","member_id","item_name",
        "category_id","merchant_id","amount","payment_method"
    ]

    df = spark.createDataFrame(data, columns)

    result = categorize_spending(df)

    row = result.collect()[0]
    assert row["spending_tier"] == "small"

# TODO: Add a test for categorize_spending boundary values
# Hint: Test exact boundaries — 10.0, 50.0, 200.0
def test_categorize_boundaries(spark):
    data = [
        ("TXN1", "2020-01-01", "M1", "item", "C1", "MER1", "10", "cash"),   # small
        ("TXN2", "2020-01-01", "M1", "item", "C1", "MER1", "50", "cash"),   # medium
        ("TXN3", "2020-01-01", "M1", "item", "C1", "MER1", "200", "cash"),  # large
    ]

    columns = [
        "transaction_id","date","member_id","item_name",
        "category_id","merchant_id","amount","payment_method"
    ]

    df = spark.createDataFrame(data, columns)

    result = categorize_spending(df).collect()

    assert result[0]["spending_tier"] == "small"
    assert result[1]["spending_tier"] == "medium"
    assert result[2]["spending_tier"] == "large"

# TODO: Add a test for enrich_with_lookups
# Hint: Create small category and merchant DataFrames, join them with transactions,
#       and verify the result includes category_name and merchant_name
from src.etl.transformations import enrich_with_lookups

def test_enrich_with_lookups(spark):
    tx_data = [
        ("TXN1", "2020-01-01", "M1", "item", "C1", "MER1", "10", "cash"),
    ]

    cat_data = [
        ("C1", "Food", "Needs"),
    ]

    mer_data = [
        ("MER1", "Tesco", "Retail"),
    ]

    columns_tx = [
        "transaction_id","date","member_id","item_name",
        "category_id","merchant_id","amount","payment_method"
    ]

    df_tx = spark.createDataFrame(tx_data, columns_tx)
    df_cat = spark.createDataFrame(cat_data, ["category_id","category_name","budget_type"])
    df_mer = spark.createDataFrame(mer_data, ["merchant_id","merchant_name","merchant_type"])

    result = enrich_with_lookups(df_tx, df_cat, df_mer)

    row = result.collect()[0]

    assert row["category_name"] == "Food"
    assert row["merchant_name"] == "Tesco"

# TODO: Add a test for enrich_with_lookups with orphan merchant_ids
# Hint: Include a transaction whose merchant_id is NOT in the merchants table.
#       After a left join, merchant_name should be null for that row.
def test_enrich_with_orphan_merchant(spark):
    tx_data = [
        ("TXN1", "2020-01-01", "M1", "item", "C1", "MER999", "10", "cash"),  # ไม่มีใน merchant
    ]

    cat_data = [
        ("C1", "Food", "Needs"),
    ]

    mer_data = [
        ("MER1", "Tesco", "Retail"),
    ]

    columns_tx = [
        "transaction_id","date","member_id","item_name",
        "category_id","merchant_id","amount","payment_method"
    ]

    df_tx = spark.createDataFrame(tx_data, columns_tx)
    df_cat = spark.createDataFrame(cat_data, ["category_id","category_name","budget_type"])
    df_mer = spark.createDataFrame(mer_data, ["merchant_id","merchant_name","merchant_type"])

    result = enrich_with_lookups(df_tx, df_cat, df_mer)

    row = result.collect()[0]

    assert row["merchant_name"] is None