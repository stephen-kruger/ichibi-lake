import requests
import json
import uuid
import time

# --- Configuration ---
# better to set "10.10.168.35    ichibi-lake" in /etc/hosts
# must be in AI_LAB network to access ichibi-lake
BASE_URL = "http://10.10.168.35:3000"
API_KEY = "ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13"#"ICHIBI_LAKE_RESEARCH_035a2116-c9d0-4603-9f12-da1fb57294d1"
HEADERS = {"x-api-key": API_KEY}

def print_section(title):
    print(f"\n{'='*20} {title} {'='*20}")

def run_demo():
    print(f"Starting ichibi-lake API Demo on {BASE_URL}...")
    
    # --- 1. JSON Batch Upload ---
    print_section("JSON Batch Upload")
    table_name = f"python_demo_{int(time.time())}"
    data = [
        {"id": "rec_1", "name": "Apple", "type": "fruit", "rating": 5},
        {"id": "rec_2", "name": "Carrot", "type": "vegetable", "rating": 4},
        {"id": "rec_3", "name": "Banana", "type": "fruit", "rating": 5}
    ]
    
    resp = requests.post(f"{BASE_URL}/upload/{table_name}", headers=HEADERS, json=data)
    print(f"POST /upload/{table_name}: {resp.status_code}")
    if resp.status_code != 200:
        print(f"ERROR: {resp.text}")
        return

    # --- 2. Discovery: List Tables ---
    print_section("Data Discovery: List Tables")
    resp = requests.get(f"{BASE_URL}/tables", headers=HEADERS)
    print(f"GET /tables: {resp.status_code}")
    if resp.status_code == 200:
        tables = resp.json().get('tables', [])
        print(f"Found {len(tables)} tables")
        if table_name in tables:
            print(f"SUCCESS: {table_name} exists in catalog.")

    # --- 3. Discovery: Table Schema ---
    print_section("Data Discovery: Table Schema")
    resp = requests.get(f"{BASE_URL}/tables/{table_name}/schema", headers=HEADERS)
    print(f"GET /tables/{table_name}/schema: {resp.status_code}")
    if resp.status_code == 200:
        schema = resp.json().get('schema', [])
        for col in schema:
            print(f" - {col['column_name']} ({col['data_type']})")

    # --- 4. Data Access: Query with Filters, Sort, Pagination ---
    print_section("RESTful Querying")
    params = {"type": "fruit", "sort": "-rating", "limit": 2}
    resp = requests.get(f"{BASE_URL}/tables/{table_name}", headers=HEADERS, params=params)
    print(f"GET /tables/{table_name}?filter...: {resp.status_code}")
    if resp.status_code == 200:
        results = resp.json().get('rows', [])
        for row in results:
            print(f" - {row['name']} (Rating: {row['rating']})")

    # --- 5. Atomic Metadata Update (PATCH) ---
    print_section("Atomic Metadata Update")
    update_data = {"status": "archived", "priority": "high", "notes": "Updated via Python"}
    resp = requests.patch(f"{BASE_URL}/tables/{table_name}/records/rec_1", headers=HEADERS, json=update_data)
    print(f"PATCH /records/rec_1: {resp.status_code}")
    
    if resp.status_code == 200:
        # Verify update
        verify_resp = requests.get(f"{BASE_URL}/tables/{table_name}?id=rec_1", headers=HEADERS)
        if verify_resp.status_code == 200:
            rows = verify_resp.json().get('rows', [])
            if rows:
                updated_rec = rows[0]
                print(f"Verified Record 1 Status: {updated_rec.get('status')} - Notes: {updated_rec.get('notes')}")

    # --- 6. Binary BLOB Management: Upload ---
    print_section("BLOB Management: Upload")
    blob_id = str(uuid.uuid4())
    binary_content = b"INTERNAL-PDF-BINARY-DATA-MOCKUP"
    resp = requests.post(
        f"{BASE_URL}/tables/{table_name}/blobs/{blob_id}/document?owner=python_agent&doc_type=report",
        headers={**HEADERS, "Content-Type": "application/octet-stream"},
        data=binary_content
    )
    print(f"POST .../blobs/{blob_id}/document: {resp.status_code}")

    # --- 7. Binary BLOB Management: Download (Streaming) ---
    print_section("BLOB Management: Download")
    resp = requests.get(f"{BASE_URL}/tables/{table_name}/blobs/{blob_id}/document", headers=HEADERS, stream=True)
    print(f"GET .../blobs/{blob_id}/document: {resp.status_code}")
    if resp.status_code == 200:
        downloaded_data = resp.content
        if downloaded_data == binary_content:
            print(f"SUCCESS: Binary data integrity verified ({len(downloaded_data)} bytes).")

    # --- 8. Anonymous BLOB Upload (Auto-Generated ID) ---
    print_section("Anonymous BLOB Upload")
    resp = requests.post(
        f"{BASE_URL}/blobs/screenshot",
        headers={**HEADERS, "Content-Type": "application/octet-stream"},
        data=b"SCREENSHOT-BYTE-STREAM"
    )
    if resp.status_code == 200:
        gen_id = resp.json().get('id')
        print(f"POST /blobs/screenshot: {resp.status_code} - Generated ID: {gen_id}")
    else:
        print(f"POST /blobs/screenshot: {resp.status_code}")

    # --- 9. Raw SQL Execution ---
    print_section("Raw SQL Execution")
    sql_query = {"sql": f"SELECT type, AVG(rating) as avg_rating FROM {table_name} GROUP BY 1"}
    resp = requests.post(f"{BASE_URL}/query", headers=HEADERS, json=sql_query)
    print(f"POST /query: {resp.status_code}")
    if resp.status_code == 200:
        for row in resp.json().get('rows', []):
            print(f" - {row['type']}: {row['avg_rating']}")

    # --- 10. Kafka Sink Ingestion ---
    print_section("Kafka Sink Ingestion")
    kafka_messages = [
        {"key": "k1", "value": {"event": "click", "ui_element": "btn_buy"}},
        {"key": "k2", "value": {"event": "scroll", "depth": "50%"}}
    ]
    resp = requests.post(
        f"{BASE_URL}/kafka-sink",
        headers={**HEADERS, "x-kafka-topic": f"{table_name}_events"},
        json=kafka_messages
    )
    print(f"POST /kafka-sink: {resp.status_code}")

    print_section("Demo Completed")

if __name__ == "__main__":
    try:
        run_demo()
    except Exception as e:
        print(f"ERROR: {e}")
