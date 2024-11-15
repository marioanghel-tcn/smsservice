from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import sqlite3

app = Flask(__name__)

# SQLite database setup
DB_FILE = 'data.db'

def init_db():
    """
    Initialize the SQLite database with a table for abandoned calls.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS abandoned_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            caller_id TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

# Initialize database
init_db()

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Endpoint to receive webhook data. Filters for calls with the
    result 'ANSWERED_LINKCALL_ABANDONED' and stores their caller ID.
    """
    data = request.json
    if data and data.get("Result") == "ANSWERED_LINKCALL_ABANDONED":
        caller_id = data.get("Callerid")
        if caller_id:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO abandoned_calls (caller_id) VALUES (?)", (caller_id,))
            conn.commit()
            conn.close()
            return jsonify({"status": "success"}), 200
    return jsonify({"status": "ignored"}), 200

@app.route('/get_abandoned_calls', methods=['GET'])
def get_abandoned_calls():
    """
    Endpoint to retrieve all abandoned call phone numbers. Clears the data
    after every GET request to prevent duplicate SMS sends.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Fetch all phone numbers and their IDs
    cursor.execute("SELECT id, caller_id FROM abandoned_calls")
    rows = cursor.fetchall()
    
    # Prepare response with phone numbers
    numbers = [row[1] for row in rows]
    ids = [row[0] for row in rows]
    
    # Delete all fetched records
    if ids:
        cursor.executemany("DELETE FROM abandoned_calls WHERE id = ?", [(id_,) for id_ in ids])
        conn.commit()
    
    conn.close()
    
    return jsonify({"phone_numbers": numbers}), 200

@app.route('/get_all_calls', methods=['GET'])
def get_all_calls():
    """
    Endpoint to retrieve all phone numbers and their results.
    Does not delete the data, only retrieves it.
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Fetch all phone numbers and their results
    cursor.execute("SELECT caller_id, timestamp FROM abandoned_calls")
    rows = cursor.fetchall()
    
    # Prepare the response
    calls = [{"phone_number": row[0], "timestamp": row[1]} for row in rows]
    
    conn.close()
    
    return jsonify({"all_calls": calls}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5040, debug=True)