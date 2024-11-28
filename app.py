from flask import Flask, request, jsonify, Response
import sqlite3
import os
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone

app = Flask(__name__)

# db file
DB_FILE = os.path.join(os.getcwd(), 'data.db')

# initialize db
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS abandoned_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            caller_id TEXT,
            phone_number TEXT DEFAULT '',
            result TEXT DEFAULT '',
            client_sid TEXT DEFAULT '',
            call_type TEXT DEFAULT '',
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# update schema
def update_db_schema():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE abandoned_calls ADD COLUMN phone_number TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # column already exists
    conn.commit()
    conn.close()

# initialize db and schema
init_db()
update_db_schema()

# Helper function to use SQLite's row_factory
def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Rows will be returned as dictionaries
    return conn

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        # Extract query parameters
        calltype_filter = request.args.get('calltype', 'all')  # Default to "all"

        # Parse the calltype_filter parameter
        if calltype_filter.lower() != "all":
            allowed_call_types = calltype_filter.split(",")
            allowed_call_types = [ct.strip() for ct in allowed_call_types]
        else:
            allowed_call_types = None  # No filter needed for "all"

        # Extract JSON payload
        data = request.json
        if not data:
            return jsonify({"error": "No JSON payload received"}), 400

        # Extract fields from the payload
        result = data.get("Result")
        caller_id = data.get("CallerId")
        client_sid = data.get("ClientSid")
        call_type = data.get("CallType")
        phone_number = data.get("PhoneNumber")

        # Validate required fields
        if not (result and caller_id and client_sid and call_type):
            return jsonify({"error": "Missing required fields"}), 400

        # Check if the call type is allowed
        if allowed_call_types and call_type not in allowed_call_types:
            return jsonify({"status": "dropped", "reason": "Call type not allowed"}), 200

        # Determine the phone number
        dynamic_phone_number = caller_id if call_type not in ["outbound", "manual", "preview"] else phone_number

        # Insert into database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO abandoned_calls (caller_id, phone_number, result, client_sid, call_type)
            VALUES (?, ?, ?, ?, ?)
        """, (caller_id, dynamic_phone_number, result, client_sid, call_type))
        conn.commit()
        conn.close()

        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def get_dynamic_phone_number(row):
    """
    Determine the phone number dynamically based on call type.
    """
    if row['call_type'] in ["outbound", "manual", "preview"]:
        return row['phone_number']
    return row['caller_id']

@app.route('/get_abandoned_calls', methods=['GET'])
def get_abandoned_calls():
    client_sid = request.args.get('ClientSid')
    calltype = request.args.get('calltype', 'all')

    if not client_sid:
        return jsonify({"error": "ClientSid parameter is required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    # Parse calltype parameter
    if calltype.lower() != "all":
        call_types = calltype.split(",")  # Split multiple types
        call_types = [ct.strip() for ct in call_types]  # Clean up whitespace
    else:
        call_types = None  # No filter needed for "all"

    # Build the query dynamically
    query = "SELECT * FROM abandoned_calls WHERE client_sid = ? AND result = 'Answered Linkcall Abandoned'"
    params = [client_sid]

    if call_types:
        query += " AND call_type IN ({})".format(", ".join("?" for _ in call_types))
        params.extend(call_types)

    rows = cursor.execute(query, params).fetchall()

    # Generate CSV content dynamically
    csv_content = "\n".join([get_dynamic_phone_number(row) for row in rows])

    # Delete all calls for this ClientSid
    cursor.execute("DELETE FROM abandoned_calls WHERE client_sid = ?", (client_sid,))
    conn.commit()
    conn.close()

    # Return CSV
    return Response(csv_content, mimetype="text/csv")

@app.route('/get_abandoned_admin', methods=['GET'])
def get_abandoned_admin():
    client_sid = request.args.get('ClientSid')
    if not client_sid:
        return jsonify({"error": "ClientSid parameter is required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    # fetch all abandoned calls for the specific ClientSid
    rows = cursor.execute("""
        SELECT * FROM abandoned_calls
        WHERE result = 'Answered Linkcall Abandoned' AND client_sid = ?
    """, (client_sid,)).fetchall()

    # response
    abandoned_calls = [
        {
            "phone_number": get_dynamic_phone_number(row),
            "result": row['result'],
            "client_sid": row['client_sid'],
            "call_type": row['call_type'],
            "timestamp": row['timestamp']
        }
        for row in rows
    ]

    conn.close()

    return jsonify({"abandoned_calls": abandoned_calls}), 200

@app.route('/get_all_calls', methods=['GET'])
def get_all_calls():
    client_sid = request.args.get('ClientSid')

    conn = get_db_connection()
    cursor = conn.cursor()

    if client_sid:
        # fetch records for a specific ClientSid
        rows = cursor.execute("""
            SELECT * FROM abandoned_calls WHERE client_sid = ?
        """, (client_sid,)).fetchall()
    else:
        # fetch all records
        rows = cursor.execute("SELECT * FROM abandoned_calls").fetchall()

    # response
    calls = [
        {
            "phone_number": get_dynamic_phone_number(row),
            "result": row['result'],
            "client_sid": row['client_sid'],
            "call_type": row['call_type']
        }
        for row in rows
    ]

    conn.close()

    return jsonify({"all_calls": calls}), 200

def clear_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM abandoned_calls")
    conn.commit()
    conn.close()
    print(f"Database cleared at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} GMT")

# initialize scheduler
scheduler = BackgroundScheduler(timezone="UTC")
scheduler.add_job(clear_database, 'cron', hour=0, minute=0)  # Run at midnight UTC
scheduler.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)