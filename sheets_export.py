import os
import json
import csv
from datetime import date
import gspread
from google.oauth2.service_account import Credentials


def _to_float(value):
    try:
        if value is None:
            return None
        value_str = str(value).strip()
        if value_str == "":
            return None
        return float(value_str)
    except Exception:
        return None


def apply_gainer_loser_colors(worksheet, data):
    """Color rows green for gainers and red for losers using change/ltp_change column."""
    if len(data) <= 1:
        return

    headers = [str(h).strip().lower() for h in data[0]]
    col_name = None
    for candidate in ("change", "ltp_change"):
        if candidate in headers:
            col_name = candidate
            break

    if not col_name:
        print("[INFO] No change column found. Skipping gainer/loser colors.")
        return

    change_col_index = headers.index(col_name)
    total_cols = len(data[0])
    requests = []

    for row_idx, row in enumerate(data[1:], start=1):
        if change_col_index >= len(row):
            continue

        change_value = _to_float(row[change_col_index])
        if change_value is None or change_value == 0:
            continue

        if change_value > 0:
            bg = {"red": 0.90, "green": 0.98, "blue": 0.90}
            fg = {"red": 0.10, "green": 0.50, "blue": 0.10}
        else:
            bg = {"red": 0.99, "green": 0.90, "blue": 0.90}
            fg = {"red": 0.65, "green": 0.10, "blue": 0.10}

        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": row_idx,
                    "endRowIndex": row_idx + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": total_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": bg,
                        "textFormat": {"foregroundColor": fg, "bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor,textFormat.bold)",
            }
        })

    if requests:
        worksheet.spreadsheet.batch_update({"requests": requests})
        print(f"[SUCCESS] Applied gainer/loser colors to {len(requests)} rows.")
    else:
        print("[INFO] No positive/negative changes found to color.")

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
        apply_gainer_loser_colors(worksheet, data)
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
