import functions_framework
import os
import tempfile
import json
import re
import cv2
from datetime import datetime
from google.cloud import storage, firestore, bigquery
from vertexai import init as vertexai_init
from vertexai.preview.generative_models import GenerativeModel, Part
from enrich_receipt import enrich_and_push

# Initialize Vertex AI (at runtime only)
vertexai_init(project="agenticai-467004", location="us-central1")

def is_video_file(file_name):
    return file_name.lower().endswith((".mp4", ".mov", ".avi", ".mkv"))

def extract_frames_from_video(video_path, timestamps=[1.0, 2.5, 4.0]):
    cap = cv2.VideoCapture(video_path)
    frames = []
    for ts in timestamps:
        cap.set(cv2.CAP_PROP_POS_MSEC, ts * 1000)
        success, frame = cap.read()
        if success:
            _, buffer = cv2.imencode('.jpg', frame)
            frames.append(buffer.tobytes())
        else:
            print(f"Failed to extract frame at {ts}s")
    cap.release()
    return frames

def push_to_bigquery(receipt_json):
    bq = bigquery.Client()
    table_id = "agenticai-467004.receipts.raw_receipts"

    # Add ingestion timestamp
    receipt_json["timestamp"] = datetime.utcnow().isoformat()

    # Normalize item fields
    if "items" in receipt_json:
        for item in receipt_json["items"]:
            item["qty"] = float(item.get("qty", 1))
            item["price"] = float(item.get("price", 0))

    # Normalize date
    if "date" in receipt_json:
        try:
            receipt_json["date"] = datetime.strptime(receipt_json["date"], "%m-%d-%Y").date().isoformat()
        except Exception:
            receipt_json["date"] = None

    # Push
    errors = bq.insert_rows_json(table_id, [receipt_json])
    if errors:
        print("BigQuery errors:", errors)
    else:
        print("Inserted into BigQuery:", receipt_json.get("receipt_id", "no_id"))

@functions_framework.cloud_event
def process_receipt(cloud_event):
    try:
        bucket_name = cloud_event.data["bucket"]
        file_name = cloud_event.data["name"]
        print(f"Received file: {file_name} from bucket: {bucket_name}")

        # Download
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            blob.download_to_filename(tmp.name)
            temp_path = tmp.name

        # Read bytes
        with open(temp_path, "rb") as f:
            file_bytes = f.read()

        # Gemini model
        gemini = GenerativeModel("gemini-2.0-flash")
        db = firestore.Client()

        prompt = """
        You are an AI system extracting structured financial data from receipts.

        Extract all details from the receipt and return only a **valid JSON** object in the following format:

        {
        "merchant": "Name of the merchant or business",
        "phone": "Phone number (if found)",
        "date": "MM-DD-YYYY",
        "time": "HH:MM AM/PM",
        "items": [
            {
            "name": "Item name",
            "qty": Number,
            "price": Number,
            "category": "Food / Grocery / Transport / Utility / Medicine / etc."
            }
        ],
        "subtotal": Number,
        "tax": Number,
        "total": Number,
        "currency": "INR / USD / EUR / etc.",
        "receipt_id": "Transaction or invoice number (if available)",
        "store_address": "Postal address (if found)",
        "category": "Top-level inferred category for the whole receipt (e.g. Grocery, Dining, Travel)",
        "is_subscription": true or false (if the receipt suggests recurring service)"
        }

        ❗ Instructions:
        - if input is in a language other than english , give the output in english
        - Return ONLY the JSON. No markdown, no comments, no pre-text, no code block formatting.
        - Ensure it is valid and parsable by `json.loads()`.
        - If any field is missing, omit it (do not return null or placeholder text).
        - if total field is not available in the receipt, populate total by adding the price of all items
        """

        # File Type Handling
        if file_name.lower().endswith(".pdf"):
            part = Part.from_data(data=file_bytes, mime_type="application/pdf")
            result = gemini.generate_content([prompt, part])

        elif file_name.lower().endswith((".jpg", ".jpeg", ".png")):
            part = Part.from_data(data=file_bytes, mime_type="image/png")
            result = gemini.generate_content([prompt, part])

        elif file_name.lower().endswith(".html"):
            html_text = file_bytes.decode("utf-8")
            result = gemini.generate_content([prompt, html_text])

        elif is_video_file(file_name):
            print("Extracting from video...")
            frames = extract_frames_from_video(temp_path)
            parts = [Part.from_data(data=frame, mime_type="image/jpeg") for frame in frames]
            result = gemini.generate_content([prompt] + parts)

        else:
            print("Unsupported file type:", file_name)
            return "Unsupported file type", 400

        # Parse JSON
        cleaned_text = result.text.strip()
        if cleaned_text.startswith("```"):
            cleaned_text = re.sub(r"```json|```", "", cleaned_text).strip()

        try:
            receipt_json = json.loads(cleaned_text)
            print("Extracted JSON:", receipt_json)
        except json.JSONDecodeError as e:
            print("JSON error:", e)
            print("Gemini output:", repr(result.text))
            return "JSON parse error", 500

        # Store to Firestore
        doc_ref = db.collection("receipts").document()
        doc_ref.set(receipt_json)
        print("Stored in Firestore with ID:", doc_ref.id)

        # Store to BigQuery
        push_to_bigquery(receipt_json)

        print("enrich and push")
        enrich_and_push(receipt_json)

        os.remove(temp_path)
        return "Success", 200

    except Exception as e:
        print("Error:", str(e))
        return f"Internal error: {str(e)}", 500
