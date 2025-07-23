import os
import csv
import gspread
from google.oauth2.service_account import Credentials as GoogleCredentials
from googleapiclient.discovery import build

# --- CONFIGURATION ---
SOURCE_DIR = "source"
SPREADSHEET_ID = "1aoT-ponIDn0hLWQ-K8hTYljk9D3P5IgK7ZgvoxE31fU"
CREDENTIALS_FILE = "creds.json"

# --- CONSTANTS ---
START_ROW_INDEX = 2
START_COL = 9 # Always insert at Column J
NUM_DATA_COLS = 6
BLOCK_WIDTH = 9 # The number of columns per date block, including spacing

# --- FORMATTING STYLES ---
COLORS = {
    "red": {"red": 1.0, "green": 0.0, "blue": 0.0},
    "green": {"red": 0.0, "green": 0.6, "blue": 0.1},
    "light_grey_fill": {"red": 0.85, "green": 0.85, "blue": 0.85}
}
BORDER = {"style": "SOLID", "width": 1}

def col_to_a1(col_idx):
    """Converts a 0-indexed column number to A1 notation."""
    a1 = ""
    col_idx += 1
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx - 1, 26)
        a1 = chr(65 + remainder) + a1
    return a1

def convert_cell(val):
    """
    Safely converts a string value to a number, handling commas.
    Returns the original if it fails.
    """
    if val is None or str(val).strip() == '':
        return ''
    # *** FIX: Handle commas in numbers ***
    cleaned_val = str(val).replace(',', '')
    try:
        f = float(cleaned_val)
        return int(f) if f.is_integer() else f
    except (ValueError, TypeError):
        return str(val).strip() # Return original if it's not a number

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    """
    Inserts data on the left and applies direct font coloring
    by comparing to the block on the RIGHT.
    """
    try:
        print(f"--- Processing: {sheet_name} from {csv_path} ---")
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it.")
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="100")

    # --- 1. Parse ONLY the first data block from the CSV ---
    first_block_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            block_segment = row[:10]
            if any(cell.strip() for cell in block_segment):
                first_block_rows.append(block_segment)

    if len(first_block_rows) < 2:
        print(f"⚠️ WARNING: CSV '{csv_path}' has no data rows. Skipping.")
        return

    date_label = first_block_rows[1][0].strip()
    csv_headers = [h.strip() for h in first_block_rows[0][2:2 + NUM_DATA_COLS]]
    csv_data_map = {row[1].strip(): [convert_cell(c) for c in row[2:2 + NUM_DATA_COLS]] for row in first_block_rows[1:] if len(row) > 1 and row[1].strip()}

    # --- 2. Check if Date Already Exists ---
    existing_headers = sheet.get(f"1:1")[0] if sheet.row_count > 0 else []
    if date_label in existing_headers:
        print(f"  -> Date '{date_label}' already exists. Skipping update to avoid duplicates.")
        return

    # --- 3. Insert New Columns on the Left ---
    print(f"  -> Inserting new block for '{date_label}' at column {col_to_a1(START_COL)}.")
    service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [{"insertDimension": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": START_COL, "endIndex": START_COL + BLOCK_WIDTH}, "inheritFromBefore": True}}]}).execute()

    # --- 4. Get Fresh Data and Align CSV Data ---
    # We must re-fetch the data AFTER inserting columns to get the correct module order and reference data
    all_data = sheet.get_all_values()
    master_module_list = [row[0] for row in all_data[START_ROW_INDEX:] if row and row[0]] if len(all_data) > START_ROW_INDEX else []
    
    aligned_data_block = [csv_data_map.get(m, [''] * NUM_DATA_COLS) for m in master_module_list]

    # --- 5. Write New Data to the Freshly Inserted Block ---
    update_body = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": f"{sheet.title}!{col_to_a1(START_COL)}1", "values": [[date_label]]},
            {"range": f"{sheet.title}!{col_to_a1(START_COL)}2", "values": [csv_headers]},
            {"range": f"{sheet.title}!{col_to_a1(START_COL)}{START_ROW_INDEX + 1}", "values": aligned_data_block}
        ]
    }
    service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=update_body).execute()

    # --- 6. Apply Direct Formatting and Borders ---
    print("  -> Applying formatting by comparing to the block on the right...")
    requests = []
    
    # Define the comparison logic
    comparison_logic = {
        "T": lambda n, r: n == r, "P": lambda n, r: n >= r, "S": lambda n, r: n <= r,
        "F": lambda n, r: n <= r, "I": lambda n, r: n <= r, "KI": lambda n, r: n <= r
    }

    ref_col_start = START_COL + BLOCK_WIDTH # Reference data is now to the right

    for r_idx, module_name in enumerate(master_module_list):
        new_row = aligned_data_block[r_idx]
        # Get the reference row from the sheet data we fetched AFTER the insert
        ref_row_data = all_data[START_ROW_INDEX + r_idx] if len(all_data) > START_ROW_INDEX + r_idx else []
        
        for c_idx, header in enumerate(csv_headers):
            color_to_apply = None
            new_val = new_row[c_idx]

            # Check if there is data in the reference column to compare against
            if len(ref_row_data) > ref_col_start + c_idx:
                ref_val = convert_cell(ref_row_data[ref_col_start + c_idx])
                try:
                    # Both must be numbers to be compared
                    if not isinstance(new_val, (int, float)) or not isinstance(ref_val, (int, float)):
                        raise TypeError()
                    
                    if comparison_logic[header](new_val, ref_val):
                        color_to_apply = COLORS["green"]
                    else:
                        color_to_apply = COLORS["red"]
                except (TypeError, KeyError):
                    pass # Not a number or header not in logic map
            elif isinstance(new_val, (int, float)): # No reference data, color all numbers red
                color_to_apply = COLORS["red"]

            if color_to_apply:
                requests.append({
                    "updateCells": {
                        "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color_to_apply}}}]}],
                        "fields": "userEnteredFormat.textFormat.foregroundColor",
                        "range": { "sheetId": sheet.id, "startRowIndex": START_ROW_INDEX + r_idx, "endRowIndex": START_ROW_INDEX + r_idx + 1, "startColumnIndex": START_COL + c_idx, "endColumnIndex": START_COL + c_idx + 1 }
                    }
                })

    # Add cosmetic requests
    max_rows = len(master_module_list)
    border_range = {"sheetId": sheet.id, "startRowIndex": 0, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": START_COL, "endColumnIndex": START_COL + NUM_DATA_COLS}
    requests.extend([
        {"updateDimensionProperties": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": START_COL, "endIndex": START_COL + NUM_DATA_COLS}, "properties": {"pixelSize": 80}, "fields": "pixelSize"}},
        {"mergeCells": {"range": {"sheetId": sheet.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": START_COL, "endColumnIndex": START_COL + NUM_DATA_COLS}, "mergeType": "MERGE_ALL"}},
        {"repeatCell": {"range": {"sheetId": sheet.id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": START_COL, "endColumnIndex": START_COL + NUM_DATA_COLS}, "cell": {"userEnteredFormat": {"backgroundColor": COLORS["light_grey_fill"], "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}},
        {"updateBorders": {"range": border_range, "top": BORDER, "bottom": BORDER, "left": BORDER, "right": BORDER}},
        {"updateBorders": {"range": border_range, "innerHorizontal": BORDER, "innerVertical": BORDER}}
    ])
    
    if requests:
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()

    print(f"✅ Sheet '{sheet_name}' updated successfully.")

def main():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
        creds = GoogleCredentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        service = build("sheets", "v4", credentials=creds)
        
        from oauth2client.service_account import ServiceAccountCredentials as GSpreadCredentials
        gspread_creds = GSpreadCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(gspread_creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        # Process files in a sorted order
        csv_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")])
        for filename in csv_files:
            sheet_name = os.path.splitext(filename)[0]
            csv_path = os.path.join(SOURCE_DIR, filename)
            update_sheet(service, spreadsheet, sheet_name, csv_path)

    except FileNotFoundError:
        print(f"❌ CRITICAL ERROR: Credentials file '{CREDENTIALS_FILE}' not found.")
    except Exception as e:
        print(f"❌ A critical error occurred in main(): {e}")
        raise

if __name__ == "__main__":
    main()
