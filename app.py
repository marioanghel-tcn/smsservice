from flask import Flask, request, jsonify
import sqlite3
import os

app = Flask(__name__)

# Database file
DB_FILE = os.path.join(os.getcwd(), 'data.db')

# Initialize the database
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

# Update schema for new columns
def update_db_schema():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE abandoned_calls ADD COLUMN result TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # Column already exists
    try:
        cursor.execute("ALTER TABLE abandoned_calls ADD COLUMN client_sid TEXT DEFAULT ''")
    except sqlite3.OperationalError:
        pass  # Column already exists
    conn.commit()
    conn.close()

# Initialize the database and schema
init_db()
update_db_schema()

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Endpoint to receive webhook data. Filters for calls with the
    result 'Answered Linkcall Abandoned' and stores their details.
    """
    try:
        data = request.json
        if isinstance(data, list):
            for item in data:
                call_result_json = item.get("callResultJson", {})
                result = call_result_json.get("Result")
                caller_id = call_result_json.get("CallerId")
                client_sid = call_result_json.get("ClientSid")
                
                if result and caller_id and client_sid:
                    # Insert into the database
                    conn = sqlite3.connect(DB_FILE)
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO abandoned_calls (caller_id, result, client_sid)
                        VALUES (?, ?, ?)
                    """, (caller_id, result, client_sid))
                    conn.commit()
                    conn.close()
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"error": "Invalid payload structure"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get_abandoned_calls', methods=['GET'])
def get_abandoned_calls():
    """
    Retrieve abandoned calls for a specific ClientSid.
    Clears data only for that ClientSid after retrieval.
    """
    client_sid = request.args.get('ClientSid')
    if not client_sid:
        return jsonify({"error": "ClientSid parameter is required"}), 400

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Fetch phone numbers for the specific ClientSid
    cursor.execute("""
        SELECT id, caller_id
        FROM abandoned_calls
        WHERE result = 'Answered Linkcall Abandoned' AND client_sid = ?
    """, (client_sid,))
    rows = cursor.fetchall()
    
    # Prepare response
    numbers = [row[1] for row in rows]
    ids = [row[0] for row in rows]
    
    # Delete only the fetched records for this ClientSid
    if ids:
        cursor.executemany("DELETE FROM abandoned_calls WHERE id = ?", [(id_,) for id_ in ids])
        conn.commit()
    
    conn.close()
    
    return jsonify({"phone_numbers": numbers}), 200

@app.route('/get_all_calls', methods=['GET'])
def get_all_calls():
    """
    Retrieve all calls, optionally filtered by ClientSid.
    Does not clear the data.
    """
    client_sid = request.args.get('ClientSid')

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    if client_sid:
        # Fetch records for a specific ClientSid
        cursor.execute("""
            SELECT caller_id, result, client_sid
            FROM abandoned_calls
            WHERE client_sid = ?
        """, (client_sid,))
    else:
        # Fetch all records
        cursor.execute("""
            SELECT caller_id, result, client_sid
            FROM abandoned_calls
        """)
    
    rows = cursor.fetchall()
    
    # Prepare response
    calls = [
        {"phone_number": row[0], "result": row[1], "client_sid": row[2]}
        for row in rows
    ]
    
    conn.close()
    
    return jsonify({"all_calls": calls}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
