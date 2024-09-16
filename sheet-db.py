import mysql.connector
import datetime
import time
import json
from mysql.connector import Error
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
from flask import Flask, request, jsonify
from threading import Thread
from pyngrok import ngrok

# MySQL connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'mysql@31-08'
}

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
cached_timestamp = None
chosen_db = None

app = Flask(__name__)

# Existing functions remain unchanged...
def get_google_sheets_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    return sheets_service, drive_service    

def watch_sheet(drive_service, file_id, pubsub_topic):
    request_body = {
        'id': 'your-channel-id',  # Unique channel ID
        'type': 'web_hook',
        'address': ' https://5dcf-49-206-3-78.ngrok-free.app',  # Your endpoint URL
        'params': {
            'ttl': '8640000'  # 10 days
        }
    }
    response = drive_service.files().watch(fileId=file_id, body=request_body).execute()
    return response

def get_revision_id(drive_service, file_id):
    revisions = drive_service.revisions().list(fileId=file_id).execute()
    latest_revision = max(revisions.get('revisions', []), key=lambda r: r['modifiedTime'])
    return latest_revision['id'], latest_revision['modifiedTime']

def load_version_history():
    if os.path.exists('version_history.pkl'):
        with open('version_history.pkl', 'rb') as f:
            return pickle.load(f)
    return {}

def save_version_history(history):
    with open('version_history.pkl', 'wb') as f:
        pickle.dump(history, f)

def get_databases():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        cursor.execute("SHOW DATABASES")
        return [db[0] for db in cursor.fetchall() if db[0] not in ['information_schema', 'mysql', 'performance_schema', 'sys']]
    except Error as e:
        print(f"Error: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def get_tables(database):
    try:
        connection = mysql.connector.connect(**DB_CONFIG, database=database)
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        return [table[0] for table in cursor.fetchall()]
    except Error as e:
        print(f"Error: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def get_table_data(database, table):
    try:
        connection = mysql.connector.connect(**DB_CONFIG, database=database)
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        columns = [column[0] for column in cursor.description]
        data = cursor.fetchall()
        print("Is this data",data)
        return columns, data
    except Error as e:
        print(f"Error: {e}")
        return [], []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def find_or_create_spreadsheet(sheets_service, drive_service, database_name):
    query = f"name = '{database_name}' and mimeType = 'application/vnd.google-apps.spreadsheet'"
    results = drive_service.files().list(q=query).execute()
    files = results.get('files', [])
    print("files",files[0]['id'])
    if files:
        return files[0]['id']
    
    spreadsheet = sheets_service.spreadsheets().create(body={
        'properties': {'title': database_name}
    }).execute()
    return spreadsheet['spreadsheetId']

def get_sheet_revision_id(drive_service, spreadsheet_id):
    file_metadata = drive_service.files().get(fileId=spreadsheet_id, fields='id, version').execute()
    return file_metadata['version']

def     sync_table_to_sheet(sheets_service, spreadsheet_id, table_name, columns, data):
    sheet_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_names = [sheet['properties']['title'] for sheet in sheet_metadata['sheets']]
    
    if table_name not in sheet_names:
        request = sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={
            "requests": [{
                "addSheet": {
                    "properties": {
                        "title": table_name
                    }
                }
            }]
        })
        request.execute()
    
    def serialize_value(value):
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        return value
    
    values = [columns] + [[serialize_value(cell) for cell in row] for row in data]
    
    body = {'values': values}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f'{table_name}!A1',
        valueInputOption='RAW',
        body=body
    ).execute()

def fetch_sheet_data(sheets_service, spreadsheet_id, table_name):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f'{table_name}!A1:Z1000'
    ).execute()
    
    sheet_values = result.get('values', [])
    
    sheet_columns = sheet_values[0] if sheet_values else []
    sheet_data = sheet_values[1:] if len(sheet_values) > 1 else []
    
    return sheet_columns, [tuple(row) for row in sheet_data]

def update_database_from_sheet(database, table_name, columns, data):
    try:
        connection = mysql.connector.connect(**DB_CONFIG, database=database)
        cursor = connection.cursor()
        cursor.execute(f"DELETE FROM {table_name}")  # Clear table before updating
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        cursor.executemany(sql, data)
        connection.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def check_for_sheet_updates_and_sync(sheets_service, drive_service, spreadsheet_id, database, table_name, columns, data):
    
    sheet_columns, sheet_data = fetch_sheet_data(sheets_service, spreadsheet_id, table_name)
    if sheet_columns == columns:
        update_database_from_sheet(database, table_name, sheet_columns, sheet_data)
    else:
        print(f"Column mismatch detected in table: {table_name}. Manual intervention needed.")
        
def get_last_modified_time(drive_service,spreadsheet_id):
    file_metadata = drive_service.files().get(fileId=spreadsheet_id, fields='modifiedTime').execute()
    return file_metadata.get('modifiedTime')

def process_pubsub_message(message):
    data = json.loads(message)
    spreadsheet_id = data['resourceId']  # Extract spreadsheet ID from Pub/Sub message
    print(f"Received notification for file ID: {spreadsheet_id}")
    # Here you could implement your logic to sync or update based on the notification
    # For demonstration purposes, we'll re-fetch data and sync
    sheets_service, drive_service = get_google_sheets_service()
    tables = get_tables('your_database')  # Replace with the actual database name
    for table in tables:
        columns, data = get_table_data('your_database', table)
        check_for_sheet_updates_and_sync(sheets_service, drive_service, spreadsheet_id, 'your_database', table, columns, data)



@app.route('/update', methods=['POST'])
def handle_sheet_update():
    global chosen_db
    data = request.json                     
    sheet_name = data['sheetName']
    row = data['row']
    column = data['columns']
    rowdata = data['rowData']

    print(rowdata)
    print(row)

    try:
        connection = mysql.connector.connect(**DB_CONFIG, database=chosen_db)
        cursor = connection.cursor()

        # Get column names
        cursor.execute(f"SHOW COLUMNS FROM {sheet_name}")
        table_columns = [column[0] for column in cursor.fetchall()]

        update_fields = []
        update_values = []

        for i, column_name in enumerate(column):
            if column_name in table_columns:
                update_fields.append(f"{column_name} = %s")
                update_values.append(rowdata[i])
        
        update_values.append(row) 
        update_query = f"UPDATE {sheet_name} SET {', '.join(update_fields)} WHERE id = %s"
        cursor.execute(update_query, update_values)
        connection.commit()
    except Error as e:
        print(f"Error updating database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    return jsonify({"status": "success"}), 200

def run_flask_with_ngrok():
    # Open a ngrok tunnel to the HTTP server
    public_url = ngrok.connect(5000).public_url
    print(f" * ngrok tunnel \"{public_url}\" -> \"http://127.0.0.1:5000/\"")
    app.config['BASE_URL'] = public_url
    app.run()

def main():
    global cached_timestamp, chosen_db
    sheets_service, drive_service = get_google_sheets_service()
    databases = get_databases()
    print("Available databases:")
    for i, db in enumerate(databases, 1):
        print(f"{i}. {db}")
    
    choice = int(input("Choose a database (enter the number): ")) - 1
    chosen_db = databases[choice]
    
    spreadsheet_id = find_or_create_spreadsheet(sheets_service, drive_service, chosen_db)
    
    tables = get_tables(chosen_db)
    
    # Sync tables to sheets
    for table in tables:
        print(f"Syncing table: {table}")
        columns, data = get_table_data(chosen_db, table)
        sync_table_to_sheet(sheets_service, spreadsheet_id, table, columns, data)
    
    # Start Flask server with ngrok in a separate thread
    flask_thread = Thread(target=run_flask_with_ngrok)
    flask_thread.start()

    print("Flask server started with ngrok. Check the console for the public URL.")
    print("Use this URL in your Apps Script webhook.")

    while True:
        db_tables = get_tables(chosen_db) 
        for table in db_tables:
            current_timestamp = get_last_modified_time(drive_service, spreadsheet_id)
            if current_timestamp != cached_timestamp:
                print("Spreadsheet has been modified.")
                cached_timestamp = current_timestamp
                columns, data = get_table_data(chosen_db, table)
                check_for_sheet_updates_and_sync(sheets_service, drive_service, spreadsheet_id, chosen_db, table, columns, data)
            else:
                print("No changes detected.")
        
        sync_table_to_sheet(sheets_service, spreadsheet_id, table, columns, data)
        print("Sync completed! Waiting for next check...")
        time.sleep(10)  # Sleep for 10 seconds before checking again

if __name__ == '__main__':
    main()