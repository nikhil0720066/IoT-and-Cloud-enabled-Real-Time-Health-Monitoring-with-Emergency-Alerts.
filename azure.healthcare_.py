import csv
import json
import os
import textwrap
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta
from azure.cosmos import CosmosClient
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from twilio.rest import Client

SCOPES = [
    "https://www.googleapis.com/auth/fitness.heart_rate.read",
    "https://www.googleapis.com/auth/fitness.oxygen_saturation.read",
]

CLIENT_SECRET = os.getenv("CLIENT_SECRET_PATH", "client_secret.json")
TOKEN_FILE = os.getenv("TOKEN_FILE", "token.json")
CSV_FILE = os.getenv("CSV_FILE", "fit_last_1day.csv")

HR_LOW_THRESHOLD = float(os.getenv("HR_LOW_THRESHOLD", "50"))
SPO2_LOW_THRESHOLD = float(os.getenv("SPO2_LOW_THRESHOLD", "90"))

HR_HIGH_THRESHOLD = os.getenv("HR_HIGH_THRESHOLD")
HR_HIGH_THRESHOLD = float(HR_HIGH_THRESHOLD) if HR_HIGH_THRESHOLD else None

TWILIO_SID = os.getenv("TWILIO_SID", "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
TWILIO_PHONE = os.getenv("TWILIO_PHONE", "+XXXXXXXXXXX")
EMERGENCY_CONTACTS = [
    c.strip() for c in os.getenv("EMERGENCY_CONTACTS", "+91XXXXXXXXXX,+91XXXXXXXXXX").split(",") if c.strip()
]

COSMOS_CONN_STR = os.getenv("COSMOS_CONN_STR", "connection_string")
COSMOS_DB_NAME = os.getenv("COSMOS_DB_NAME", "FitVitals")
COSMOS_COLL = os.getenv("COSMOS_COLL", "Vitals")

TRIAL_ACCOUNT = os.getenv("TRIAL_ACCOUNT", "true").lower() in ("1", "true", "yes")
SMS_CHAR_LIMIT = int(os.getenv("SMS_CHAR_LIMIT", "140"))

# Timezones
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc

def get_credentials():
    """Resolve Google credentials from env, token file, or interactive flow."""
    token_env = os.getenv("GOOGLE_TOKEN")
    if token_env:
        try:
            info = json.loads(token_env)
            return Credentials.from_authorized_user_info(info, SCOPES)
        except Exception as e:
            print("⚠️ GOOGLE_TOKEN present but could not parse:", e)

    if Path(TOKEN_FILE).exists():
        try:
            return Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            print("⚠️ Failed reading token file:", e)

    if Path(CLIENT_SECRET).exists():
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET, SCOPES)
        creds = flow.run_local_server(port=8282, access_type="offline", prompt="consent")
        Path(TOKEN_FILE).write_text(creds.to_json(), encoding="utf-8")
        return creds

    raise RuntimeError(
        "No Google credentials: set GOOGLE_TOKEN env or provide token.json and client_secret.json."
    )

def get_dataset(service, data_type_keyword: str, days: int = 1):
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)
    dataset_id = f"{int(start_time.timestamp() * 1e9)}-{int(end_time.timestamp() * 1e9)}"

    try:
        ds_list = service.users().dataSources().list(userId="me").execute()
    except Exception as e:
        print("❌ Failed to list data sources:", e)
        return []

    target_source = None
    for ds in ds_list.get("dataSource", []):
        ds_id = ds.get("dataStreamId", "").lower()
        dt_name = ds.get("dataType", {}).get("name", "").lower()
        if data_type_keyword.lower() in ds_id or data_type_keyword.lower() in dt_name:
            target_source = ds.get("dataStreamId")
            break

    if not target_source:
        print(f"❌ No data source found for {data_type_keyword}")
        return []

    try:
        dataset_response = (
            service.users()
            .dataSources()
            .datasets()
            .get(userId="me", dataSourceId=target_source, datasetId=dataset_id)
            .execute()
        )
    except Exception as e:
        print("❌ Failed to fetch dataset:", e)
        return []

    points = []
    for point in dataset_response.get("point", []):
        ts = int(point.get("startTimeNanos", 0)) / 1e9
        utc_time = datetime.fromtimestamp(ts, timezone.utc)
        ist_time = utc_time.astimezone(IST)
        val = None
        v = point.get("value", [])
        if v:
            val = v[0].get("fpVal") if "fpVal" in v[0] else v[0].get("intVal")
        if val is not None:
            points.append((ist_time.isoformat(), val))  # return IST ISO string
    return points

def save_to_csv(heart_data, spo2_data):
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp (IST)", "Heart Rate (bpm)", "SpO₂ (%)"])
        all_times = sorted(set([t for t, _ in heart_data] + [t for t, _ in spo2_data]))
        for t in all_times:
            hr = next((v for ts, v in heart_data if ts == t), "")
            sp = next((v for ts, v in spo2_data if ts == t), "")
            writer.writerow([t, hr, sp])
    print(f"✅ Data saved to {CSV_FILE}")

def get_cosmos_collection():
    if not COSMOS_CONN_STR:
        print("ℹ️ COSMOS_CONN_STR not set; skipping DB save.")
        return None
    try:
        client = CosmosClient.from_connection_string(COSMOS_CONN_STR)
        database = client.get_database_client(COSMOS_DB_NAME)
        container = database.get_container_client(COSMOS_COLL)
        try:
            _ = container.read()
        except Exception:
            pass
        return container
    except Exception as e:
        print("⚠️ Cosmos DB init failed; skipping DB save:", e)
        return None


def save_to_cosmos(heart_data, spo2_data, user_id="user123"):
    container = get_cosmos_collection()
    if not container:
        return

    docs = []

    for t, hr in heart_data:
        try:
            docs.append(
                {
                    "id": str(uuid.uuid4()),
                    "userId": user_id,
                    "timestamp": datetime.fromisoformat(t).astimezone(UTC).isoformat(),
                    "heart_rate": float(hr),
                    "ingested_at": datetime.now(UTC).isoformat(),
                }
            )
        except Exception:
            continue

    for t, sp in spo2_data:
        try:
            docs.append(
                {
                    "id": str(uuid.uuid4()),
                    "userId": user_id,
                    "timestamp": datetime.fromisoformat(t).astimezone(UTC).isoformat(),
                    "spo2": float(sp),
                    "ingested_at": datetime.now(UTC).isoformat(),
                }
            )
        except Exception:
            continue

    for doc in docs:
        try:
            container.create_item(doc)
        except Exception as e:
            print("⚠️ Failed to insert doc:", e)

    print(f"✅ Inserted {len(docs)} records into Cosmos DB for user {user_id}")

def shorten_message(latest_hr, latest_spo2, timestamp, hospitals_link):
    base = f"HR:{latest_hr}bpm SpO₂:{latest_spo2}% Time:{timestamp} "
    links = f"H:{hospitals_link}"

    message = f"{base}{links}"
    if len(message) > SMS_CHAR_LIMIT:
        excess = len(message) - SMS_CHAR_LIMIT
        if excess < len(links):
            links = links[:-excess]
        message = f"{base}{links}"
    return message


def create_alert_message(latest_hr, latest_spo2, hospitals_link):
    timestamp = datetime.now(IST).strftime("%H:%M:%S %d-%m")
    if TRIAL_ACCOUNT:
        return shorten_message(latest_hr, latest_spo2, timestamp, hospitals_link)
    return (
        f"Alert\nPatient: XXXXXX\nHeart Rate: {latest_hr}bpm\n"
        f"SpO₂: {latest_spo2}%\nTime: {timestamp}\n"
        f"Hospitals: {hospitals_link}"
    )


def send_twilio_alert(message):
    if not (TWILIO_SID and TWILIO_AUTH_TOKEN and TWILIO_PHONE and EMERGENCY_CONTACTS):
        print("⚠️ Twilio config incomplete; skipping SMS.")
        return
    try:
        client = Client(TWILIO_SID, TWILIO_AUTH_TOKEN)
    except Exception as e:
        print("⚠️ Twilio client init failed:", e)
        return

    chunks = textwrap.wrap(message, SMS_CHAR_LIMIT, break_long_words=False, replace_whitespace=False)
    for i, chunk in enumerate(chunks, 1):
        for number in EMERGENCY_CONTACTS:
            try:
                body = f"Part {i}/{len(chunks)}: {chunk}" if len(chunks) > 1 else chunk
                msg = client.messages.create(body=body, from_=TWILIO_PHONE, to=number)
                print(f"✅ Message sent to {number}, SID: {msg.sid}")
            except Exception as e:
                print(f"❌ Failed to send message to {number}: {e}")

def evaluate_alerts(heart_data, spo2_data):
    alert_needed = False
    reasons = []

    latest_hr = heart_data[-1][1] if heart_data else "N/A"
    latest_spo2 = spo2_data[-1][1] if spo2_data else "N/A"

    for ts, hr_val in heart_data:
        try:
            if float(hr_val) <= HR_LOW_THRESHOLD:
                reasons.append(f"Low HR {hr_val} bpm at {ts} (≤ {HR_LOW_THRESHOLD})")
                alert_needed = True
        except Exception:
            continue

    if HR_HIGH_THRESHOLD is not None:
        for ts, hr_val in heart_data:
            try:
                if float(hr_val) >= HR_HIGH_THRESHOLD:
                    reasons.append(f"High HR {hr_val} bpm at {ts} (≥ {HR_HIGH_THRESHOLD})")
                    alert_needed = True
            except Exception:
                continue

    for ts, sp_val in spo2_data:
        try:
            if float(sp_val) <= SPO2_LOW_THRESHOLD:
                reasons.append(f"Low SpO₂ {sp_val}% at {ts} (≤ {SPO2_LOW_THRESHOLD})")
                alert_needed = True
        except Exception:
            continue

    return alert_needed, reasons, latest_hr, latest_spo2

def main(days: int = 1, hospitals_link: str = "https://maps.google.com/?q=hospitals+near+me"):
    creds = get_credentials()
    service = build("fitness", "v1", credentials=creds)

    heart_data = get_dataset(service, "heart_rate", days=days)
    spo2_data = get_dataset(service, "oxygen_saturation", days=days)

    print("\n📊 Heart Rate Data:")
    for ts, v in heart_data:
        print(f"{ts} → {v} bpm")
    print("\n📊 SpO₂ Data:")
    for ts, v in spo2_data:
        print(f"{ts} → {v} %")

    save_to_csv(heart_data, spo2_data)
    save_to_cosmos(heart_data, spo2_data)

    alert_needed, reasons, latest_hr, latest_spo2 = evaluate_alerts(heart_data, spo2_data)

    if alert_needed:
        for r in reasons:
            print("[ALERT DEBUG]", r)
        alert_message = create_alert_message(latest_hr, latest_spo2, hospitals_link)
        send_twilio_alert(alert_message)
        print("✅ Emergency alert sent to contacts!")
    else:
        print("✅ Vital signs are within safe limits for the last period.")


if __name__ == "__main__":
    days_env = int(os.getenv("DAYS", "1"))
    main(days=days_env)
