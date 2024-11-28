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

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        # extract json payload
        data = request.json
        if not data:
            return jsonify({"error": "No JSON payload received"}), 400

        # extract fields
        result = data.get("Result")
        caller_id = data.get("CallerId")
        client_sid = data.get("ClientSid")
        call_type = data.get("CallType")  # New field
        phone_number = data.get("PhoneNumber")  # New field for outbound calls

        # validate fields
        if not (result and caller_id and client_sid and call_type):
            return jsonify({"error": "Missing required fields"}), 400

        # decide what to store as phone_number
        dynamic_phone_number = caller_id  # Default to CallerId for inbound calls
        if call_type in ["outbound", "manual", "preview"]:
            dynamic_phone_number = phone_number  # Use PhoneNumber for these call types

        # insert into db
        conn = sqlite3.connect(DB_FILE)
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
    # Determine the phone number dynamically based on the call type
    call_type = row[3]
    return row[1] if call_type in ["outbound", "manual", "preview"] else row[0]

@app.route('/get_abandoned_calls', methods=['GET'])
def get_abandoned_calls():
    client_sid = request.args.get('ClientSid')
    if not client_sid:
        return jsonify({"error": "ClientSid parameter is required"}), 400

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # fetch phone numbers for the specific ClientSid where call type is "inbound"
    cursor.execute("""
        SELECT caller_id, phone_number, result, call_type, id
        FROM abandoned_calls
        WHERE result = 'Answered Linkcall Abandoned' AND client_sid = ? AND call_type = 'inbound'
    """, (client_sid,))
    rows = cursor.fetchall()
    
    # generate CSV content dynamically
    csv_content = "\n".join([get_dynamic_phone_number(row) for row in rows]) if rows else ""
    ids = [row[4] for row in rows]
    
    # fetch all calls for ClientSid to be cleared
    cursor.execute("""
        SELECT id FROM abandoned_calls WHERE client_sid = ?
    """, (client_sid,))
    all_ids = [row[0] for row in cursor.fetchall()]
    
    # delete all calls for this ClientSid
    if all_ids:
        cursor.executemany("DELETE FROM abandoned_calls WHERE id = ?", [(id_,) for id_ in all_ids])
        conn.commit()
    
    conn.close()
    
    # return csv
    return Response(csv_content, mimetype="text/csv")

@app.route('/get_abandoned_admin', methods=['GET'])
def get_abandoned_admin():
    client_sid = request.args.get('ClientSid')
    if not client_sid:
        return jsonify({"error": "ClientSid parameter is required"}), 400

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # fetch all abandoned calls for the specific ClientSid
    cursor.execute("""
        SELECT caller_id, phone_number, result, client_sid, call_type, timestamp
        FROM abandoned_calls
        WHERE result = 'Answered Linkcall Abandoned' AND client_sid = ?
    """, (client_sid,))
    rows = cursor.fetchall()

    # response
    abandoned_calls = [
        {
            "phone_number": get_dynamic_phone_number(row),
            "result": row[2],
            "client_sid": row[3],
            "call_type": row[4],
            "timestamp": row[5]
        }
        for row in rows
    ]

    conn.close()

    return jsonify({"abandoned_calls": abandoned_calls}), 200

@app.route('/get_all_calls', methods=['GET'])
def get_all_calls():
    client_sid = request.args.get('ClientSid')

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    if client_sid:
        # fetch records for a specific ClientSid
        cursor.execute("""
            SELECT caller_id, phone_number, result, client_sid, call_type
            FROM abandoned_calls
            WHERE client_sid = ?
        """, (client_sid,))
    else:
        # fetch all records
        cursor.execute("""
            SELECT caller_id, phone_number, result, client_sid, call_type
            FROM abandoned_calls
        """)
    
    rows = cursor.fetchall()
    
    # response
    calls = [
        {
            "phone_number": get_dynamic_phone_number(row),
            "result": row[2],
            "client_sid": row[3],
            "call_type": row[4]
        }
        for row in rows
    ]
    
    conn.close()
    
    return jsonify({"all_calls": calls}), 200

def clear_database():
    conn = sqlite3.connect(DB_FILE)
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