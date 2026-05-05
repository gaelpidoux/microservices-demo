import os
import json
import threading
from datetime import datetime, timezone

import redis
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1, tasks_v2, storage

# --- INIT ---
app = Flask(__name__)

# --- ENV ---
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SUBSCRIPTION_NAME = os.environ.get("SUBSCRIPTION_NAME")

REGION = os.environ.get("REGION")
TASK_QUEUE = os.environ.get("TASK_QUEUE")
PROCESSOR_URL = os.environ.get("PROCESSOR_URL")

SNAPSHOT_BUCKET = os.environ.get("SNAPSHOT_BUCKET")

SERVER_ID = os.environ.get("K_SERVICE", "local")

# --- REDIS ---
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True
)

# --- GCP CLIENTS ---
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
tasks_client = tasks_v2.CloudTasksClient()
storage_client = storage.Client()

# --- PATHS ---
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME) if TOPIC_NAME else None
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME) if SUBSCRIPTION_NAME else None

# --- PUBSUB LISTENER ---
def callback(message):
    key = message.data.decode()
    value = r.get(key)

    if value:
        print("EVENT RECEIVED:", value)

    message.ack()

def listen_pubsub():
    if subscription_path:
        future = subscriber.subscribe(subscription_path, callback=callback)
        print("Listening Pub/Sub...")
        future.result()

# ⚠️ désactivé au début si besoin
threading.Thread(target=listen_pubsub, daemon=True).start()

# --- CLOUD TASK ---
def create_task(redis_key):
    if not (PROJECT_ID and REGION and TASK_QUEUE and PROCESSOR_URL):
        return

    parent = tasks_client.queue_path(PROJECT_ID, REGION, TASK_QUEUE)

    payload = json.dumps({"redis_key": redis_key}).encode()

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{PROCESSOR_URL}/process",
            "headers": {"Content-Type": "application/json"},
            "body": payload,
        }
    }

    tasks_client.create_task(request={"parent": parent, "task": task})

# --- ROUTES ---
@app.route("/")
def home():
    return "OK"

@app.route("/health")
def health():
    try:
        r.ping()
        return jsonify({"status": "healthy"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503

@app.route("/publish", methods=["POST"])
def publish():
    data = request.get_json()

    message = data.get("message") or data.get("action")
    if not message:
        return jsonify({"error": "message manquant"}), 400

    entry = {
        "message": message,
        "server_id": SERVER_ID,
        "published_at": datetime.now(timezone.utc).isoformat()
    }

    key = f"event:{SERVER_ID}:{entry['published_at']}"

    # Redis
    r.setex(key, 300, json.dumps(entry))

    # Pub/Sub
    if topic_path:
        publisher.publish(topic_path, key.encode())

    # Cloud Tasks
    create_task(key)

    return jsonify({"status": "published", "data": entry})

@app.route("/data")
def data():
    result = {}
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, match="event:*", count=100)
        for key in keys:
            value = r.get(key)
            if value:
                result[key] = json.loads(value)
        if cursor == 0:
            break

    return jsonify(result)

@app.route("/process", methods=["POST"])
def process():
    body = request.get_json()
    key = body.get("redis_key")

    data = r.get(key)
    if not data:
        return jsonify({"status": "skipped"}), 200

    # récupérer état complet
    game_state = {}
    cursor = 0

    while True:
        cursor, keys = r.scan(cursor=cursor, match="event:*")
        for k in keys:
            v = r.get(k)
            if v:
                game_state[k] = json.loads(v)
        if cursor == 0:
            break

    snapshot = {
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
        "event_count": len(game_state),
        "state": game_state,
    }

    if SNAPSHOT_BUCKET:
        bucket = storage_client.bucket(SNAPSHOT_BUCKET)
        blob = bucket.blob(f"snapshots/{int(datetime.now().timestamp())}.json")
        blob.upload_from_string(json.dumps(snapshot), content_type="application/json")

    return jsonify({"status": "processed"})

# --- MAIN ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)