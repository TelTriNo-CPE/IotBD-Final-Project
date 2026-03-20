from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, abs


def categorize_spending(df: DataFrame) -> DataFrame:
    """Add a spending_tier column based on the transaction amount.

    Tiers:
        - "micro"  if amount < 10
        - "small"  if 10 <= amount < 50
        - "medium" if 50 <= amount < 200
        - "large"  if amount >= 200

    For negative amounts (refunds), categorize by absolute value.

    Args:
        df: DataFrame with an 'amount' column (numeric)

    Returns:
        DataFrame with an additional 'spending_tier' column
    """
    amount = abs(col("amount").cast("double"))

    return df.withColumn(
        "spending_tier",
        when(amount < 10, "micro")
        .when(amount < 50, "small")
        .when(amount < 200, "medium")
        .otherwise("large")
    )


def enrich_with_lookups(
    df: DataFrame, df_categories: DataFrame, df_merchants: DataFrame
) -> DataFrame:
    """Join transactions with category and merchant lookup tables.

    - Left join on category_id to add: category_name, budget_type
    - Left join on merchant_id to add: merchant_name, merchant_type
    - All transactions must be kept (even if merchant_id has no match)

    Args:
        df: Staged transactions DataFrame
        df_categories: Categories lookup DataFrame
        df_merchants: Merchants lookup DataFrame

    Returns:
        Enriched DataFrame with category and merchant info
    """
    return df \
        .join(df_categories, "category_id", "left") \
        .join(df_merchants, "merchant_id", "left")
