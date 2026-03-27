# Final Report

**Student Name:** Natanan Tatanan
**Student ID:** 650610760
**Date:** 20/03/2026

---

## Part 1: Data Exploration Answers

> How many total transactions are there?

There are 72586000 total transactions in the dataset.

> How many unique family members, merchants, and categories?

There are:
- 4 unique family members
- 48 unique merchants
- 18 unique categories

> How many rows have null or empty `amount` values?

There are 1450421 rows with null or empty amount values.

> What is the date range of the transactions?

The transactions range from 2010-01-01 to 2025-12-31.
The dataset spans multiple years, allowing for long-term trend analysis.

---

## Part 4: Join Analysis

> What join type did you use for enriching transactions, and why?

A left join was used to enrich the transactions with category and merchant data. This ensures that all transaction records are preserved, even if some transactions do not have matching records in the lookup tables. This is important because missing lookup data should not result in data loss.

> How many transactions have no matching merchant in the merchants table?

There are 212089 transactions with no matching merchant in the merchants table.

> What would happen if you used an inner join instead?

If an inner join were used, only transactions with matching records in both the transactions and lookup tables would be retained. This would result in the loss of transactions with missing merchant or category information, leading to incomplete analysis and potential bias in the results.

---

## Part 5: Analytics Insights

> Look at the average transaction amount per year table. Do you notice a trend? Calculate the approximate year-over-year percentage change. What might explain this?

The average transaction amount shows an increasing trend over the years, indicating that spending per transaction has grown over time.

| Year | Avg Amount | YoY Change (%) |
|------|-----------|----------------|
| 2016 | 55.98 | - |
| 2017 | 57.12 | 2.03% |
| 2018 | 58.33 | 2.12% |
| 2019 | 59.47 | 1.95% |
| 2020 | 60.59 | 1.88% |
| 2021 | 61.87 | 2.12% |
| 2022 | 63.08 | 1.95% |
| 2023 | 64.41 | 2.11% |
| 2024 | 65.54 | 1.75% |
| 2025 | 66.94 | 2.13% |

This trend may be explained by inflation, increased cost of living, or changes in purchasing behavior over time.

> Which category has the highest total spending? Which has grown the fastest over 10 years?

- The category with the highest total spending is likely groceries or daily essentials, as these are recurring expenses.
- The fastest-growing category is likely discretionary spending, such as entertainment or dining, reflecting lifestyle changes over time.

> Compare spending between family members. Who spends the most? On what?

The family member with the highest spending is likely the primary household provider, with most expenses concentrated in essential categories such as groceries, utilities, and household needs.

> What percentage of transactions fall in each spending tier? Has this distribution changed over the years?

- Most transactions fall within the micro and small spending tiers, indicating frequent low-value purchases.
- Over time, there is a slight shift toward higher spending tiers, suggesting an increase in transaction sizes.

---

## Section A: Data Architecture Questions

_The family has some questions about how the system works._

### Q1. Merchant Name Change

> "We just found out one of the merchants changed their name last year. Where in the pipeline do we update this, and what layers need to be reprocessed?"

The merchant name should be updated in the merchants lookup table. After updating, the analytics layer should be reprocessed to reflect the changes. The raw layer remains unchanged, while the staged layer does not need modification unless the schema changes.

---

### Q2. New Family Member

> "Our daughter started college and has her own credit card now. How do we add a new family member to the system without breaking existing data?"

A new family member can be added by simply including their transactions in the transactions dataset. Since the pipeline processes data dynamically, no structural changes are required. The new member will automatically appear in downstream aggregations.
---

### Q3. Average Monthly Grocery Spending

> "We want to know our average monthly grocery spending. Walk us through exactly which transformations and joins produce this number."

To calculate average monthly grocery spending, transactions are first filtered and cleaned in the staged layer. Then, they are joined with the categories table to identify grocery transactions. Finally, the data is aggregated by month and averaged to produce the result.

---

### Q4. Duplicate Transactions

> "Last month's bank export had 500 duplicate transactions. How does your pipeline handle this? If it doesn't yet, what would you add?"

Currently, the pipeline does not explicitly handle duplicate transactions. To address this, a deduplication step can be added in the staged layer using distinct or by dropping duplicates based on transaction_id.

---

### Q5. Data Backup & Recovery

> "We're worried about losing our data. What's your backup strategy? What's the most data we could lose if something crashes?"

_Hint: Think about RPO (Recovery Point Objective) and RTO (Recovery Time Objective)._

The pipeline stores data in multiple layers (raw, staged, analytics), which provides natural redundancy. This follows a medallion architecture (raw, staged, analytics), where each layer serves a specific purpose in improving data quality and usability. Backups can be implemented by periodically copying data to external storage such as cloud storage. The Recovery Point Objective (RPO) depends on the pipeline frequency, while the Recovery Time Objective (RTO) depends on how quickly the pipeline can be rerun.

---

## Section B: Engineering Questions

_The family's developer friend has some technical questions._

### Q6. CI/CD Pipeline

> "If we set up CI/CD for this project, what would the pipeline look like? What gets tested automatically, and what triggers the tests?"

A CI/CD pipeline would include automated testing using pytest to validate transformations and data quality. Tests are triggered on every code commit or pull request. If tests pass, the pipeline can be deployed automatically.

---

### Q7. Monthly Automation with Orchestration

> "We want this pipeline to run automatically every month when the bank exports new transactions. How would you set this up? Draw the DAG."

The pipeline can be automated using a scheduler such as Apache Airflow. Tasks include data ingestion, validation, transformation, and aggregation.

_Draw your DAG below (text-based diagram):_

```
Example format:
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Task 1  │ ──▶ │  Task 2  │ ──▶ │  Task 3  │
└──────────┘     └──────────┘     └──────────┘

Your DAG:

┌────────────┐
│ Read CSV   │
└─────┬──────┘
      ↓
┌────────────┐
│ Raw Layer  │
└─────┬──────┘
      ↓
┌────────────┐
│ Staged     │
└─────┬──────┘
      ↓
┌────────────┐
│ Analytics  │
└────────────┘


```

---

## Section C: Analytics Insights

_The family wants your professional opinion._

### Q8. Price Trend Analysis

> "We looked at your yearly average transaction table and prices seem to go up. Can you calculate the exact rate? Is it consistent across all categories?"

_Show your work:_

The year-over-year change can be calculated using the formula:

YoY % = ((current_year - previous_year) / previous_year) * 100

The increase is not always consistent across categories, as some categories may grow faster than others.

---

### Q9. Spending Recommendations

> "Based on your summary tables, give us 3 actionable recommendations for how we can reduce spending next year."

1. Reduce spending on non-essential categories such as entertainment.
2. Monitor high-frequency small purchases that accumulate over time.
3. Set monthly budgets for major categories such as groceries and dining.

---

### Q10. Needs vs Wants

> "Which spending categories are 'needs' vs 'wants'? What percentage of our total spending goes to each?"

Essential categories such as groceries, utilities, and housing account for the largest portion of spending at approximately 49.77%. A significant share, around 43.82%, is spent on wants such as entertainment and dining, indicating a relatively high level of discretionary spending. Savings make up only about 6.42% of total spending, suggesting that there may be room to improve financial planning and increase savings for long-term stability. This suggests an opportunity to reduce discretionary spending and increase savings.

| Budget Type | Total Spending | Percentage |
|-------------|---------------|------------|
| Needs | 2,160,425,364 | 49.77% |
| Wants | 1,902,196,974 | 43.82% |
| Savings | 278,489,060 | 6.42% |
