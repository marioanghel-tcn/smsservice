from flask import Flask, request, jsonify, Response
import sqlite3
import os
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone, timedelta

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
            result TEXT DEFAULT '',
            client_sid TEXT DEFAULT '',
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
        cursor.execute("ALTER TABLE abandoned_calls ADD COLUMN result TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # column already exists
    try:
        cursor.execute("ALTER TABLE abandoned_calls ADD COLUMN client_sid TEXT DEFAULT ''")
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

        # validate fields
        if not (result and caller_id and client_sid):
            return jsonify({"error": "Missing required fields"}), 400

        # insert into db
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO abandoned_calls (caller_id, result, client_sid)
            VALUES (?, ?, ?)
        """, (caller_id, result, client_sid))
        conn.commit()
        conn.close()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_abandoned_calls', methods=['GET'])
def get_abandoned_calls():
    client_sid = request.args.get('ClientSid')
    if not client_sid:
        return jsonify({"error": "ClientSid parameter is required"}), 400

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # fetch phone numbers for the specific clientsid
    cursor.execute("""
        SELECT id, caller_id
        FROM abandoned_calls
        WHERE result = 'Answered Linkcall Abandoned' AND client_sid = ?
    """, (client_sid,))
    rows = cursor.fetchall()
    
    # csv content
    csv_content = "\n".join([row[1] for row in rows]) if rows else ""
    ids = [row[0] for row in rows]
    
    # fetch all calls for clientsid to be cleared
    cursor.execute("""
        SELECT id FROM abandoned_calls WHERE client_sid = ?
    """, (client_sid,))
    all_ids = [row[0] for row in cursor.fetchall()]
    
    # delete all calls for this clientsid
    if all_ids:
        cursor.executemany("DELETE FROM abandoned_calls WHERE id = ?", [(id_,) for id_ in all_ids])
        conn.commit()
    
    conn.close()
    
    # return csv
    return Response(csv_content, mimetype="text/csv")

@app.route('/get_all_calls', methods=['GET'])
def get_all_calls():
    client_sid = request.args.get('ClientSid')

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    if client_sid:
        # fetch records for a specific clientsid
        cursor.execute("""
            SELECT caller_id, result, client_sid
            FROM abandoned_calls
            WHERE client_sid = ?
        """, (client_sid,))
    else:
        # fetch all records
        cursor.execute("""
            SELECT caller_id, result, client_sid
            FROM abandoned_calls
        """)
    
    rows = cursor.fetchall()
    
    # response
    calls = [
        {"phone_number": row[0], "result": row[1], "client_sid": row[2]}
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