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
    """Apply conditional formatting to change columns (green for gainers, red for losers)."""
    if len(data) <= 1:
        return

    headers = [str(h).strip().lower() for h in data[0]]
    sheet_id = worksheet.id
    requests = []

    # 1. Format header row - Blue background with white bold text
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {"red": 0.29, "green": 0.53, "blue": 0.91},
                    "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}, "bold": True},
                    "horizontalAlignment": "CENTER"
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
        }
    })

    # 2. Find change and change_pct columns
    change_col_idx = None
    change_pct_col_idx = None

    for candidate in ("change", "ltp_change"):
        if candidate in headers:
            change_col_idx = headers.index(candidate)
            break

    for candidate in ("change_pct", "change %", "ltp_change_pct"):
        if candidate in headers:
            change_pct_col_idx = headers.index(candidate)
            break

    if change_col_idx is None and change_pct_col_idx is None:
        print("[INFO] No change columns found. Skipping conditional formatting.")
        return

    # 3. Apply conditional formatting to change column (if exists)
    if change_col_idx is not None:
        # Green for positive values
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000, 
                                "startColumnIndex": change_col_idx, "endColumnIndex": change_col_idx + 1}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                        "format": {"backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.83},
                                   "textFormat": {"foregroundColor": {"red": 0.10, "green": 0.50, "blue": 0.10}, "bold": True}}
                    }
                },
                "index": 0
            }
        })

        # Red for negative values
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                                "startColumnIndex": change_col_idx, "endColumnIndex": change_col_idx + 1}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                        "format": {"backgroundColor": {"red": 0.96, "green": 0.8, "blue": 0.8},
                                   "textFormat": {"foregroundColor": {"red": 0.65, "green": 0.10, "blue": 0.10}, "bold": True}}
                    }
                },
                "index": 1
            }
        })

    # 4. Apply conditional formatting to change_pct column (if exists)
    if change_pct_col_idx is not None:
        # Green for positive percentage
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                                "startColumnIndex": change_pct_col_idx, "endColumnIndex": change_pct_col_idx + 1}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                        "format": {"backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.83},
                                   "textFormat": {"foregroundColor": {"red": 0.10, "green": 0.50, "blue": 0.10}, "bold": True}}
                    }
                },
                "index": 2
            }
        })

        # Red for negative percentage
        requests.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1000,
                                "startColumnIndex": change_pct_col_idx, "endColumnIndex": change_pct_col_idx + 1}],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                        "format": {"backgroundColor": {"red": 0.96, "green": 0.8, "blue": 0.8},
                                   "textFormat": {"foregroundColor": {"red": 0.65, "green": 0.10, "blue": 0.10}, "bold": True}}
                    }
                },
                "index": 3
            }
        })

    if requests:
        try:
            worksheet.spreadsheet.batch_update({"requests": requests})
            print(f"[SUCCESS] Applied header formatting and gainer/loser colors to change columns.")
        except Exception as e:
            print(f"[WARNING] Could not apply formatting: {e}")
    else:
        print("[INFO] No formatting rules to apply.")

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
