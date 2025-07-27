import functions_framework
from google.cloud import bigquery, firestore
from datetime import datetime

bq_client = bigquery.Client()
fs_client = firestore.Client()

INSIGHTS_TABLE = "agenticai-467004.receipts.raw_receipts"

@functions_framework.cloud_event
def run_insights(cloud_event):
    # 1. Monthly Spend per Category
    query1 = f"""
    SELECT
      category,
      FORMAT_DATE('%Y-%m', DATE(date)) AS month,
      ROUND(SUM(total), 2) AS total_spend
    FROM `{INSIGHTS_TABLE}`
    WHERE date IS NOT NULL
    GROUP BY category, month
    ORDER BY month DESC, total_spend DESC
    """

    monthly_spend = bq_client.query(query1).result()
    insights = []
    for row in monthly_spend:
        insights.append({
            "category": row["category"],
            "month": row["month"],
            "total_spend": row["total_spend"],
            "insight_type": "monthly_category_spend",
            "generated_at": datetime.utcnow().isoformat()
        })

    # 2. Top Merchants by Spend
    query2 = f"""
    SELECT merchant, COUNT(receipt_id) AS txn_count, ROUND(SUM(total), 2) AS total_spend
    FROM `{INSIGHTS_TABLE}`
    GROUP BY merchant
    ORDER BY total_spend DESC
    LIMIT 5
    """
    merchant_result = bq_client.query(query2).result()
    for row in merchant_result:
        insights.append({
            "merchant": row["merchant"],
            "txn_count": row["txn_count"],
            "total_spend": row["total_spend"],
            "insight_type": "top_merchants",
            "generated_at": datetime.utcnow().isoformat()
        })

    # 3. Upload insights to Firestore
    for entry in insights:
        doc_id = f"{entry.get('insight_type')}_{entry.get('month', '')}_{entry.get('merchant', entry.get('category', 'unknown'))}"
        fs_client.collection("receipt_insights").document(doc_id).set(entry)

    print(f"✅ Uploaded {len(insights)} insights to Firestore.")

