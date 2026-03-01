"""
Sparks Sports Academy - Message Ingestion & Funnel Processing System

Deployable as Docker container or standalone script.
Connects to Qiscus Omnichannel webhooks and APIs to build a
Lead → Booking → Transaction funnel for the Growth Team.
"""

import os
import json
import re
import logging
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import httpx
import gspread
from google.oauth2.service_account import Credentials


# CONFIG
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

QISCUS_APP_ID = os.getenv("QISCUS_APP_ID", "")
QISCUS_SECRET_KEY = os.getenv("QISCUS_SECRET_KEY", "")
QISCUS_BASE_URL = os.getenv("QISCUS_BASE_URL", "https://multichannel.qiscus.com")

GOOGLE_SHEETS_CRED_PATH = os.getenv("GOOGLE_SHEETS_CRED_PATH")
KEYWORD_SHEET_ID = os.getenv("KEYWORD_SHEET_ID", "")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sparks_ingestion")

app = FastAPI(title="Sparks Sports Academy - Message Ingestion")



# DATABASE HELPERS
def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def init_database():
    """
    Create the raw, staging, and gold tables.
    Following a standard data warehouse layering approach:
      - Raw: exact data from source, append-only
      - Staging: cleaned, enriched, deduplicated
      - Gold: business-ready funnel metrics
    """
    ddl = """
    -- RAW LAYER 
    CREATE SCHEMA IF NOT EXISTS raw;

    CREATE TABLE IF NOT EXISTS raw.messages (
        id              BIGSERIAL PRIMARY KEY,
        qiscus_msg_id   BIGINT,
        room_id         VARCHAR(50),
        sender_email    VARCHAR(255),
        sender_name     VARCHAR(255),
        sender_type     VARCHAR(20),    -- 'customer', 'agent', 'system'
        message_text    TEXT,
        message_type    VARCHAR(50),    -- 'text', 'file_attachment', 'buttons', etc.
        channel         VARCHAR(50),    -- 'wa', 'ig', 'fb', 'line', 'livechat'
        timestamp       TIMESTAMPTZ,
        payload_json    JSONB,
        ingested_at     TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.rooms (
        id              BIGSERIAL PRIMARY KEY,
        room_id         VARCHAR(50) UNIQUE,
        room_name       VARCHAR(255),
        channel         VARCHAR(50),
        customer_phone  VARCHAR(50),
        customer_name   VARCHAR(255),
        is_resolved     BOOLEAN DEFAULT FALSE,
        first_msg_id    BIGINT,
        last_msg_id     BIGINT,
        source          VARCHAR(50),    -- 'ads', 'campaign_blast', 'organic', 'direct'
        room_options    JSONB,
        created_at      TIMESTAMPTZ,
        resolved_at     TIMESTAMPTZ,
        ingested_at     TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.keyword_config (
        id              SERIAL PRIMARY KEY,
        keyword         VARCHAR(255),
        category        VARCHAR(100),   -- 'opening', 'booking_intent', 'transaction'
        is_active       BOOLEAN DEFAULT TRUE,
        synced_at       TIMESTAMPTZ DEFAULT NOW()
    );

    --  STAGING LAYER 
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.rooms_classified (
        room_id             VARCHAR(50) PRIMARY KEY,
        channel             VARCHAR(50),
        customer_phone      VARCHAR(50),
        customer_name       VARCHAR(255),
        lead_date           DATE,
        opening_keyword     VARCHAR(255),
        first_message_text  TEXT,
        source              VARCHAR(50),
        is_resolved         BOOLEAN,
        agent_name          VARCHAR(255),
        processed_at        TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS staging.messages_tagged (
        qiscus_msg_id   BIGINT PRIMARY KEY,
        room_id         VARCHAR(50),
        sender_type     VARCHAR(20),
        message_text    TEXT,
        detected_intent VARCHAR(50),   -- 'opening', 'booking_intent', 'booking_confirmed', 'transaction'
        channel         VARCHAR(50),
        timestamp       TIMESTAMPTZ,
        processed_at    TIMESTAMPTZ DEFAULT NOW()
    );

    --  GOLD LAYER 
    CREATE SCHEMA IF NOT EXISTS gold;

    CREATE TABLE IF NOT EXISTS gold.funnel_leads (
        room_id             VARCHAR(50) PRIMARY KEY,
        lead_date           DATE NOT NULL,
        channel             VARCHAR(50),
        phone_number        VARCHAR(50),
        customer_name       VARCHAR(255),
        opening_keyword     VARCHAR(255),
        source              VARCHAR(50),
        booking_date        DATE,
        transaction_date    DATE,
        transaction_value   NUMERIC(15, 2),
        funnel_stage        VARCHAR(50),   -- 'lead', 'booked', 'transacted'
        agent_name          VARCHAR(255),
        updated_at          TIMESTAMPTZ DEFAULT NOW()
    );

    -- Indexes for common query patterns
    CREATE INDEX IF NOT EXISTS idx_raw_messages_room ON raw.messages(room_id);
    CREATE INDEX IF NOT EXISTS idx_raw_messages_ts ON raw.messages(timestamp);
    CREATE INDEX IF NOT EXISTS idx_gold_funnel_lead_date ON gold.funnel_leads(lead_date);
    CREATE INDEX IF NOT EXISTS idx_gold_funnel_channel ON gold.funnel_leads(channel);
    CREATE INDEX IF NOT EXISTS idx_gold_funnel_stage ON gold.funnel_leads(funnel_stage);
    """
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    conn.close()
    logger.info("Database schema initialized successfully.")



# KEYWORD MANAGEMENT (Google Sheets)
def sync_keywords_from_sheet():
    """
    The growth team manages opening keywords in a Google Sheet.
    We pull them periodically to keep our matching logic up to date.
    Sheet format expected: | keyword | category | is_active |
    """
    try:
        creds = Credentials.from_service_account_file(
            GOOGLE_SHEETS_CRED_PATH,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(KEYWORD_SHEET_ID).sheet1
        rows = sheet.get_all_records()

        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                # Truncate and reload — simple and reliable for small config tables
                cur.execute("TRUNCATE raw.keyword_config;")
                for row in rows:
                    cur.execute("""
                        INSERT INTO raw.keyword_config (keyword, category, is_active)
                        VALUES (%s, %s, %s)
                    """, (
                        row.get("keyword", "").strip().lower(),
                        row.get("category", "opening").strip().lower(),
                        row.get("is_active", True)
                    ))
        conn.close()
        logger.info(f"Synced {len(rows)} keywords from Google Sheet.")
    except Exception as e:
        logger.error(f"Failed to sync keywords: {e}")


def get_active_keywords() -> list[dict]:
    conn = get_db_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT keyword, category FROM raw.keyword_config
            WHERE is_active = TRUE
        """)
        results = [{"keyword": r[0], "category": r[1]} for r in cur.fetchall()]
    conn.close()
    return results



# QISCUS API CLIENT
class QiscusClient:
    """
    Thin wrapper around Qiscus Omnichannel Public API.
    Used for batch polling (backfill) alongside real-time webhooks.

    Reference endpoints from the Postman collection:
    - GET  /api/v2/customer_rooms       → list rooms with filters
    - GET  /api/v2/customer_rooms/{id}  → room detail
    - GET  /api/v1/export/rooms         → bulk export rooms data
    - POST /api/v2/customer_rooms/{room_id}/messages → get messages in room
    """

    def __init__(self):
        self.base_url = QISCUS_BASE_URL
        self.headers = {
            "QISCUS_APP_ID": QISCUS_APP_ID,
            "QISCUS_SECRET_KEY": QISCUS_SECRET_KEY,
            "Content-Type": "application/json"
        }

    async def get_rooms(self, status: str = "resolved", page: int = 1, limit: int = 100):
        """
        Fetch customer rooms from Omnichannel API.
        status: 'resolved', 'unresolved', or 'all'
        """
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/api/v2/customer_rooms",
                headers=self.headers,
                params={"status": status, "page": page, "limit": limit}
            )
            resp.raise_for_status()
            return resp.json().get("data", {}).get("customer_rooms", [])

    async def get_room_messages(self, room_id: str):
        """
        Fetch all messages in a specific room.
        We use this for backfilling or when webhook data is incomplete.
        """
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/api/v2/customer_rooms/{room_id}/messages",
                headers=self.headers
            )
            resp.raise_for_status()
            return resp.json().get("data", {}).get("messages", [])

    async def get_room_detail(self, room_id: str):
        """Get detailed room info including channel and customer data."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.base_url}/api/v2/customer_rooms/{room_id}",
                headers=self.headers
            )
            resp.raise_for_status()
            return resp.json().get("data", {})


qiscus_client = QiscusClient()



# WEBHOOK HANDLERS (Real-time Ingestion)
def parse_channel_from_options(options_str: str) -> str:
    """
    Qiscus stores channel info in room.options as a JSON string.
    Example: {"channel":"wa","source":"wa","is_resolved":false,...}
    """
    try:
        opts = json.loads(options_str) if isinstance(options_str, str) else options_str
        return opts.get("channel", opts.get("source", "unknown"))
    except (json.JSONDecodeError, TypeError):
        return "unknown"


def extract_phone_from_email(email: str) -> str:
    """
    In Qiscus, the customer 'email' field often contains
    the phone number (e.g., '628123456789' for WhatsApp users).
    """
    digits = re.sub(r"\D", "", email)
    if len(digits) >= 10:
        return digits
    return email


@app.post("/webhooks/new-session")
async def handle_new_session(request: Request):
    """
    Triggered when a customer starts a new conversation (first message
    or first message after room was resolved).

    This is our primary entry point for detecting new leads.
    The opening message will be matched against the keyword spreadsheet.

    Qiscus New Session Webhook payload structure:
    {
        "is_new_session": true,
        "payload": {
            "from": { "email": "628xxx", "name": "Customer Name" },
            "message": { "text": "...", "timestamp": "..." },
            "room": { "id": "xxx", "options": "{...}" }
        }
    }
    """
    body = await request.json()
    payload = body.get("payload", {})

    room_data = payload.get("room", {})
    message_data = payload.get("message", {})
    from_data = payload.get("from", {})

    room_id = str(room_data.get("id", ""))
    channel = parse_channel_from_options(room_data.get("options", "{}"))
    customer_email = from_data.get("email", "")
    customer_phone = extract_phone_from_email(customer_email)
    customer_name = from_data.get("name", "")
    message_text = message_data.get("text", "")
    timestamp_str = message_data.get("timestamp", "")

    try:
        msg_ts = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        msg_ts = datetime.now(timezone.utc)

    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            # Insert into raw.rooms
            cur.execute("""
                INSERT INTO raw.rooms (room_id, room_name, channel, customer_phone,
                    customer_name, source, room_options, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (room_id) DO UPDATE SET
                    is_resolved = FALSE,
                    ingested_at = NOW()
            """, (
                room_id, room_data.get("name", ""),
                channel, customer_phone, customer_name,
                channel,  # source defaults to channel, enriched later
                json.dumps(room_data.get("options", {})),
                msg_ts
            ))

            # Insert the opening message into raw.messages
            cur.execute("""
                INSERT INTO raw.messages (qiscus_msg_id, room_id, sender_email,
                    sender_name, sender_type, message_text, message_type,
                    channel, timestamp, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                message_data.get("id"),
                room_id, customer_email, customer_name,
                "customer", message_text,
                message_data.get("type", "text"),
                channel, msg_ts,
                json.dumps(payload)
            ))
    conn.close()

    logger.info(f"New session ingested: room={room_id}, channel={channel}")
    return JSONResponse({"status": "ok", "room_id": room_id})


@app.post("/webhooks/post-message")
async def handle_post_message(request: Request):
    """
    Triggered on every new message in any active room.
    We capture all messages to track conversation progression
    (booking confirmations, transaction info, etc.).

    Qiscus SDK Webhook payload:
    {
        "type": "post_comment_mobile" | "post_comment_rest",
        "payload": {
            "from": { "id": 1, "email": "...", "name": "..." },
            "room": { "id": 1, "topic_id": 1, "type": "group" },
            "message": { "type": "text", "text": "..." }
        }
    }
    """
    body = await request.json()
    payload = body.get("payload", {})

    room_data = payload.get("room", {})
    msg_data = payload.get("message", {})
    from_data = payload.get("from", {})

    room_id = str(room_data.get("id", ""))
    sender_email = from_data.get("email", "")
    sender_name = from_data.get("name", "")

    # Determine if sender is customer, agent, or system
    # Agents typically have an @qismo.com or company domain email
    sender_type = "customer"
    if "@qismo.com" in sender_email or "admin" in sender_email.lower():
        sender_type = "agent"
    elif sender_email.startswith("system"):
        sender_type = "system"

    message_text = msg_data.get("text", "")
    channel = parse_channel_from_options(room_data.get("options", "{}"))

    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw.messages (qiscus_msg_id, room_id, sender_email,
                    sender_name, sender_type, message_text, message_type,
                    channel, timestamp, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                msg_data.get("id"),
                room_id, sender_email, sender_name,
                sender_type, message_text,
                msg_data.get("type", "text"),
                channel,
                datetime.now(timezone.utc),
                json.dumps(payload)
            ))
    conn.close()

    logger.info(f"Message ingested: room={room_id}, sender={sender_type}")
    return JSONResponse({"status": "ok"})


@app.post("/webhooks/mark-resolved")
async def handle_mark_resolved(request: Request):
    """
    Triggered when an agent/admin resolves (closes) a room.
    We update the room status so the funnel processor knows
    the conversation is complete.

    Payload structure:
    {
        "service": {
            "room_id": "xxx", "is_resolved": true,
            "first_comment_id": xxx, "last_comment_id": xxx,
            "source": "wa"
        },
        "resolved_by": { "name": "Agent Name", "type": "agent" }
    }
    """
    body = await request.json()
    service = body.get("service", {})
    resolved_by = body.get("resolved_by", {})

    room_id = str(service.get("room_id", ""))

    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE raw.rooms SET
                    is_resolved = TRUE,
                    resolved_at = NOW(),
                    first_msg_id = %s,
                    last_msg_id = %s
                WHERE room_id = %s
            """, (
                service.get("first_comment_id"),
                service.get("last_comment_id"),
                room_id
            ))
    conn.close()

    logger.info(f"Room resolved: {room_id} by {resolved_by.get('name', 'unknown')}")
    return JSONResponse({"status": "ok"})


# FUNNEL PROCESSING LOGIC
def process_funnel():
    """
    Core processing logic that transforms raw messages into the
    growth team's funnel:

    1. LEAD DETECTION: Match the first customer message in a room
       against opening keywords from the spreadsheet.

    2. BOOKING DETECTION: Look for booking-related patterns
       (date mentions, "jadwal", "daftar", "booking" in messages).

    3. TRANSACTION DETECTION: Look for payment/transaction patterns
       (nominal amounts, "bayar", "transfer", "invoice").

    This runs as a scheduled job (e.g., every 15 minutes via cron
    or Airflow) and processes unprocessed rooms.
    """
    keywords = get_active_keywords()
    opening_keywords = [k["keyword"] for k in keywords if k["category"] == "opening"]
    booking_keywords = [k["keyword"] for k in keywords if k["category"] == "booking_intent"]
    transaction_keywords = [k["keyword"] for k in keywords if k["category"] == "transaction"]

    # Fallback patterns if spreadsheet categories are limited
    booking_patterns = booking_keywords or [
        "booking", "daftar", "jadwal", "coba kelas", "trial",
        "mau ikut", "kapan bisa mulai", "schedule", "class"
    ]
    transaction_patterns = transaction_keywords or [
        "bayar", "transfer", "invoice", "pembayaran", "paid",
        "lunas", "payment", "receipt"
    ]

    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            # STEP 1: Get rooms that haven't been processed yet
            cur.execute("""
                SELECT r.room_id, r.channel, r.customer_phone, r.customer_name,
                       r.source, r.is_resolved, r.created_at
                FROM raw.rooms r
                LEFT JOIN gold.funnel_leads f ON r.room_id = f.room_id
                WHERE f.room_id IS NULL
                   OR f.updated_at < r.ingested_at
                ORDER BY r.created_at DESC
                LIMIT 1000
            """)
            rooms_to_process = cur.fetchall()

            for room_row in rooms_to_process:
                room_id, channel, phone, name, source, is_resolved, created_at = room_row

                # Get all messages for this room, ordered chronologically
                cur.execute("""
                    SELECT qiscus_msg_id, sender_type, message_text, timestamp
                    FROM raw.messages
                    WHERE room_id = %s
                    ORDER BY timestamp ASC
                """, (room_id,))
                messages = cur.fetchall()

                if not messages:
                    continue

                # STEP 2: Detect opening keyword (Lead)
                first_customer_msg = None
                opening_keyword_found = None

                for msg in messages:
                    msg_id, sender_type, text, ts = msg
                    if sender_type == "customer" and text:
                        first_customer_msg = msg
                        text_lower = text.strip().lower()
                        for kw in opening_keywords:
                            if kw in text_lower:
                                opening_keyword_found = kw
                                break
                        break  # only check the first customer message

                if not first_customer_msg:
                    continue

                lead_date = first_customer_msg[3].date() if first_customer_msg[3] else (
                    created_at.date() if created_at else None
                )

                # STEP 3: Detect booking intent
                booking_date = None
                for msg in messages:
                    _, sender_type, text, ts = msg
                    if text:
                        text_lower = text.lower()
                        if any(bp in text_lower for bp in booking_patterns):
                            booking_date = ts.date() if ts else None
                            break

                # STEP 4: Detect transaction
                transaction_date = None
                transaction_value = None
                for msg in messages:
                    _, sender_type, text, ts = msg
                    if text:
                        text_lower = text.lower()
                        if any(tp in text_lower for tp in transaction_patterns):
                            transaction_date = ts.date() if ts else None
                            # Try to extract amount (Indonesian format: Rp 500.000)
                            amount_match = re.search(
                                r'(?:rp\.?\s*|idr\s*)([\d.,]+)',
                                text_lower
                            )
                            if amount_match:
                                amount_str = amount_match.group(1).replace(".", "").replace(",", "")
                                try:
                                    transaction_value = float(amount_str)
                                except ValueError:
                                    pass
                            break

                # Determine funnel stage
                funnel_stage = "lead"
                if transaction_date:
                    funnel_stage = "transacted"
                elif booking_date:
                    funnel_stage = "booked"

                # STEP 5: Upsert into gold layer
                cur.execute("""
                    INSERT INTO gold.funnel_leads (
                        room_id, lead_date, channel, phone_number,
                        customer_name, opening_keyword, source,
                        booking_date, transaction_date, transaction_value,
                        funnel_stage, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (room_id) DO UPDATE SET
                        booking_date = EXCLUDED.booking_date,
                        transaction_date = EXCLUDED.transaction_date,
                        transaction_value = EXCLUDED.transaction_value,
                        funnel_stage = EXCLUDED.funnel_stage,
                        updated_at = NOW()
                """, (
                    room_id, lead_date, channel, phone,
                    name, opening_keyword_found, source,
                    booking_date, transaction_date, transaction_value,
                    funnel_stage
                ))

                # Also insert into staging for audit trail
                cur.execute("""
                    INSERT INTO staging.rooms_classified (
                        room_id, channel, customer_phone, customer_name,
                        lead_date, opening_keyword, first_message_text,
                        source, is_resolved
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (room_id) DO UPDATE SET
                        is_resolved = EXCLUDED.is_resolved,
                        processed_at = NOW()
                """, (
                    room_id, channel, phone, name,
                    lead_date, opening_keyword_found,
                    first_customer_msg[2] if first_customer_msg else None,
                    source, is_resolved
                ))

    conn.close()
    logger.info(f"Funnel processing complete. Processed {len(rooms_to_process)} rooms.")



# BATCH POLLER (Backfill / Catch missed webhooks)
async def batch_poll_rooms():
    """
    Safety net: periodically pull rooms from Qiscus API
    in case any webhook delivery was missed.
    Runs every 15 minutes via scheduler (Airflow/cron).
    """
    try:
        rooms = await qiscus_client.get_rooms(status="all", limit=100)
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                for room in rooms:
                    room_id = str(room.get("id", ""))
                    channel = room.get("channel", {}).get("source", "unknown")
                    customer = room.get("customer", {})

                    cur.execute("""
                        INSERT INTO raw.rooms (room_id, room_name, channel,
                            customer_phone, customer_name, is_resolved,
                            source, room_options, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (room_id) DO UPDATE SET
                            is_resolved = EXCLUDED.is_resolved,
                            ingested_at = NOW()
                    """, (
                        room_id,
                        room.get("name", ""),
                        channel,
                        customer.get("phone", ""),
                        customer.get("name", ""),
                        room.get("is_resolved", False),
                        channel,
                        json.dumps(room),
                        room.get("created_at")
                    ))
        conn.close()
        logger.info(f"Batch poll complete: {len(rooms)} rooms synced.")
    except Exception as e:
        logger.error(f"Batch poll failed: {e}")



# SCHEDULER ENDPOINT (trigger from Airflow/cron)
@app.post("/jobs/sync-keywords")
async def job_sync_keywords():
    """Trigger keyword sync from Google Sheets."""
    sync_keywords_from_sheet()
    return {"status": "ok", "message": "Keywords synced"}


@app.post("/jobs/process-funnel")
async def job_process_funnel():
    """Trigger funnel processing."""
    process_funnel()
    return {"status": "ok", "message": "Funnel processed"}


@app.post("/jobs/batch-poll")
async def job_batch_poll():
    """Trigger batch polling from Qiscus API."""
    await batch_poll_rooms()
    return {"status": "ok", "message": "Batch poll complete"}



# HEALTH CHECK
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}



# STARTUP
@app.on_event("startup")
async def startup():
    logger.info("Starting Sparks Sports Academy Ingestion Service...")
    try:
        init_database()
    except Exception as e:
        logger.warning(f"DB init skipped (may already exist): {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
