import os
import json
import csv
from datetime import date
import gspread
from google.oauth2.service_account import Credentials

def export_to_sheets(csv_file, spreadsheet_id, custom_name=None):
    # 1. Authenticate
    # We expect the full JSON key content as a string in the environment variable
    creds_json = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
    if not creds_json:
        print("[ERROR] GCP_SERVICE_ACCOUNT_KEY environment variable not set.")
        return

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    try:
        creds_dict = json.loads(creds_json)
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(credentials)
    except Exception as e:
        print(f"[ERROR] Authentication failed: {e}")
        return

    # 2. Open Spreadsheet
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
    except Exception as e:
        print(f"[ERROR] Could not open spreadsheet with ID {spreadsheet_id}: {e}")
        return

    # 3. Read CSV data
    if not os.path.exists(csv_file):
        print(f"[ERROR] CSV file {csv_file} not found.")
        return

    with open(csv_file, 'r', newline='') as f:
        reader = csv.reader(f)
        data = list(reader)

    if not data:
        print("[INFO] No data to export.")
        return

    # 4. Create new Worksheet for the data date
    if custom_name:
        sheet_name = custom_name
    else:
        sheet_name = date.today().strftime("%Y-%m-%d")
    
    # Check if sheet already exists, delete if it does (or just append)
    # User requested "new data should be appended to a new work sheet"
    try:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
    except gspread.exceptions.APIError:
        # If it already exists, let's just get it
        print(f"[INFO] Worksheet {sheet_name} already exists. Updating it.")
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear() # Clear old data if we are running again on the same day

    # 5. Upload data
    try:
        worksheet.update('A1', data)
        print(f"[SUCCESS] Exported {len(data)-1} rows to worksheet '{sheet_name}'.")
    except Exception as e:
        print(f"[ERROR] Failed to upload data: {e}")

if __name__ == "__main__":
    CSV_PATH = "ONE_BY_ONE_FETCH_Export.csv"
    # Placeholder for local testing - on GitHub this will come from Secrets
    SHEET_ID = os.environ.get('GOOGLE_SHEET_ID')
    
    if SHEET_ID:
        export_to_sheets(CSV_PATH, SHEET_ID)
    else:
        print("[ERROR] GOOGLE_SHEET_ID environment variable not set.")
