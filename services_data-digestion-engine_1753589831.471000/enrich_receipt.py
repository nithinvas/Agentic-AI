from vertexai import init as vertexai_init
from vertexai.preview.generative_models import GenerativeModel
from google.cloud import bigquery
from datetime import datetime
import json
import re


# Initialize Vertex AI
vertexai_init(project="agenticai-467004", location="us-central1")
model = GenerativeModel("gemini-2.0-flash")

def sanitize_dict(input_dict, allowed_keys):
    return {k: v for k, v in input_dict.items() if k in allowed_keys}

def normalize_row_for_bigquery(raw):
    merchant = raw.get("merchant", {})
    if isinstance(merchant, str):
        try:
            merchant = json.loads(merchant)
        except json.JSONDecodeError:
            merchant = {}

    merchant_profile = merchant.get("profile", {})
    if isinstance(merchant_profile, str):
        try:
            merchant_profile = json.loads(merchant_profile)
        except json.JSONDecodeError:
            merchant_profile = {}

    clean = {
        "receipt_id": raw.get("receipt_id"),
        "user_id": raw.get("user_id"),
        "merchant_name": merchant.get("name") or raw.get("merchant_name"),
        "merchant_category": merchant.get("category") or raw.get("merchant_category"),
        "merchant_profile": raw.get("merchant_profile", {
            "website": merchant_profile.get("website"),
            "country": merchant_profile.get("country"),
            "tags": merchant_profile.get("tags")
        }),
        "amount": raw.get("amount"),
        "currency": raw.get("currency"),
        "payment_method": raw.get("payment_method"),
        "phone": raw.get("phone"),
        "purchase_date": raw.get("purchase_date") or raw.get("date"),
        "timestamp": raw.get("timestamp"),
        "ingestion_timestamp": raw.get("ingestion_timestamp"),
        "enriched_timestamp": raw.get("enriched_timestamp"),
        "subscription": raw.get("subscription"),
        "refund_eligible": raw.get("refund_eligible"),
        "user_spend_level": raw.get("user_spend_level"),
        "category": raw.get("category"),
        "store_address": raw.get("store_address"),
        "notes": raw.get("notes"),
        "items": []
    }

    clean["merchant_profile"] = sanitize_dict(merchant_profile, {"website", "country", "tags"})

    for item in raw.get("items", []):
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except json.JSONDecodeError:
                continue  # skip invalid entries
        clean["items"].append({
            "item_name": item.get("item_name") or item.get("name"),
            "quantity": item.get("quantity") or item.get("qty"),
            "price": item.get("price")
        })

    return clean


def enrich_receipt(raw_receipt: dict) -> dict:
    prompt = """
    You are an intelligent agent that enriches receipt data to support smarter financial decision-making.

    Given the raw receipt JSON, return an enriched version with these additional fields:
    - day_of_week
    - merchant_category
    - payment_method (if you can infer it)
    - user_spend_level (Low/Medium/High based on total)
    - actions_suggested (JSON list with action type, reason, and confidence)
    - location (mock or inferred from store_address if possible)
    - merchant_profile (mocked with rating, verified, maps_url)

    ❗ Format: Return ONLY valid JSON. No markdown, no code blocks.
    ❗ Do NOT include comments or explanations.
    ❗ Return valid JSON that can be parsed using json.loads().
    """

    raw_json_str = json.dumps(raw_receipt)
    result = model.generate_content([prompt, raw_json_str])
    output = result.text.strip()

    if output.startswith("```"):
        output = re.sub(r"```json|```", "", output).strip()

    try:
        enriched = json.loads(output)
        enriched["receipt_id"] = raw_receipt.get("receipt_id")
        enriched["enriched_timestamp"] = datetime.utcnow().isoformat()
        if isinstance(enriched, str):
            try:
                enriched = json.loads(enriched)
            except json.JSONDecodeError:
                print("❌ Invalid JSON string passed to enrich_and_push")
                return
        return normalize_row_for_bigquery(enriched)
    except json.JSONDecodeError as e:
        print("Gemini failed to return valid JSON.")
        print("Raw output:", output)
        raise e

def push_to_bigquery(enriched_receipt: dict):
    client = bigquery.Client()
    table_id = "agenticai-467004.receipts.enriched_receipts"
    errors = client.insert_rows_json(table_id, [enriched_receipt])
    if errors:
        print("BigQuery insertion failed:", errors)
        raise Exception("Failed to insert enriched receipt")
    else:
        print("Enriched receipt inserted to BigQuery successfully.")

def enrich_and_push(raw_receipt):
    if isinstance(raw_receipt, str):
        try:
            raw_receipt = json.loads(raw_receipt)
        except json.JSONDecodeError:
            print("❌ Invalid JSON string passed to enrich_and_push")
            return

    enriched = enrich_receipt(raw_receipt)
    push_to_bigquery(enriched)
