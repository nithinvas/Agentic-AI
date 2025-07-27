from google.cloud import bigquery
from datetime import datetime
import functions_framework

bq_client = bigquery.Client()
PROJECT_ID = "agenticai-467004"
DATASET = "receipts"
ENRICHED_TABLE = f"{PROJECT_ID}.{DATASET}.enriched_receipts"
PREDICTION_TABLE = f"{PROJECT_ID}.{DATASET}.prediction_results"

def write_predictions_to_bigquery(records):
    errors = bq_client.insert_rows_json(PREDICTION_TABLE, records)
    if errors:
        print("❌ Insert errors:", errors)
    else:
        print(f"✅ Inserted {len(records)} predictions.")

def predict_refund():
    query = f"""
    SELECT receipt_id, total, category, is_subscription, predicted_refund_eligible
    FROM ML.PREDICT(MODEL `{PROJECT_ID}.{DATASET}.ml_refund_predictor`,
      (SELECT receipt_id, total, category, is_subscription
       FROM `{ENRICHED_TABLE}`
       WHERE refund_eligible IS NULL))
    """
    results = bq_client.query(query).result()
    records = [{
        "receipt_id": row["receipt_id"],
        "model_type": "refund",
        "prediction_result": str(row["predicted_refund_eligible"]),
        "created_at": datetime.utcnow().isoformat()
    } for row in results]
    write_predictions_to_bigquery(records)

def predict_subscription():
    query = f"""
    SELECT receipt_id, merchant, total, predicted_is_subscription
    FROM ML.PREDICT(MODEL `{PROJECT_ID}.{DATASET}.ml_subscription_predictor`,
      (SELECT receipt_id, merchant, total
       FROM `{ENRICHED_TABLE}`
       WHERE is_subscription IS NULL))
    """
    results = bq_client.query(query).result()
    records = [{
        "receipt_id": row["receipt_id"],
        "model_type": "subscription",
        "prediction_result": str(row["predicted_is_subscription"]),
        "created_at": datetime.utcnow().isoformat()
    } for row in results]
    write_predictions_to_bigquery(records)

def predict_next_purchase():
    query = f"""
    SELECT user_id, category,
           MAX(DATE(date)) AS last_purchase_date,
           DATE_ADD(MAX(DATE(date)), INTERVAL 30 DAY) AS predicted_next_purchase_date
    FROM `{ENRICHED_TABLE}`
    WHERE date IS NOT NULL
    GROUP BY user_id, category
    """
    results = bq_client.query(query).result()
    records = [{
        "receipt_id": None,
        "user_id": row["user_id"],
        "model_type": "next_purchase",
        "prediction_result": str(row["predicted_next_purchase_date"]),
        "created_at": datetime.utcnow().isoformat()
    } for row in results]
    write_predictions_to_bigquery(records)

def cluster_user_spend():
    query = f"""
    SELECT user_id, total_spent, spend_cluster
    FROM ML.PREDICT(MODEL `{PROJECT_ID}.{DATASET}.ml_spend_cluster`,
      (SELECT user_id, SUM(total) AS total_spent
       FROM `{ENRICHED_TABLE}`
       GROUP BY user_id))
    """
    results = bq_client.query(query).result()
    records = [{
        "receipt_id": None,
        "user_id": row["user_id"],
        "model_type": "spend_cluster",
        "prediction_result": str(row["spend_cluster"]),
        "created_at": datetime.utcnow().isoformat()
    } for row in results]
    write_predictions_to_bigquery(records)

# ---------- HTTP Entry Point ----------
@functions_framework.http
def run_all_predictions(request):
    if request.method != "GET":
        return ("Method Not Allowed", 405)

    try:
        predict_refund()
        predict_subscription()
        predict_next_purchase()
        cluster_user_spend()
        return ("✅ ML predictions executed and saved to BigQuery.", 200)
    except Exception as e:
        return (f"❌ Error: {str(e)}", 500)
