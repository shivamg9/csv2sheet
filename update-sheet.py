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
START_COL = 9
NUM_DATA_COLS = 6
BLOCK_WIDTH = 9

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
    """Safely converts a string value to a number, returning the original if it fails."""
    if val is None or str(val).strip() == '':
        return ''
    try:
        f = float(val)
        return int(f) if f.is_integer() else f
    except (ValueError, TypeError):
        return str(val).strip()

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    """
    Reads data from a given CSV file, maps it, inserts or updates it in the sheet,
    and applies direct font coloring based on comparison logic.
    """
    try:
        print(f"--- Processing: {sheet_name} from {csv_path} ---")
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it.")
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

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

    # --- 2. Determine Target Column and Get Existing Sheet Data ---
    existing_data = sheet.get_all_values()
    sheet_headers = existing_data[0] if existing_data else []
    
    target_col = -1
    if date_label in sheet_headers:
        target_col = sheet_headers.index(date_label)

    # --- 3. Capture Reference Data BEFORE Making Changes ---
    reference_data_map = {}
    ref_col_start = -1
    if target_col != -1: # Updating in place
        ref_col_start = target_col + BLOCK_WIDTH
    else: # Inserting new
        ref_col_start = START_COL

    if len(existing_data) > START_ROW_INDEX and len(sheet_headers) > ref_col_start:
        print(f"  -> Found reference data starting at column {col_to_a1(ref_col_start)}.")
        for r_idx, row in enumerate(existing_data[START_ROW_INDEX:]):
            module_name = row[0]
            if module_name:
                ref_values = (row[ref_col_start : ref_col_start + NUM_DATA_COLS] + [''] * NUM_DATA_COLS)[:NUM_DATA_COLS]
                reference_data_map[module_name] = [convert_cell(v) for v in ref_values]

    # --- 4. Insert or Identify Columns for the New Data ---
    if target_col == -1:
        print(f"  -> Date '{date_label}' not found. Inserting new columns.")
        target_col = START_COL
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [{"insertDimension": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": START_COL, "endIndex": START_COL + BLOCK_WIDTH}, "inheritFromBefore": False}}]}).execute()
    else:
        print(f"  -> Date '{date_label}' found. Updating columns in place.")

    # --- 5. Align Data and Prepare for Update ---
    master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]] if len(existing_data) > START_ROW_INDEX else []
    new_modules = [m for m in csv_data_map if m not in master_module_list]
    if new_modules:
        print(f"  -> New modules found: {', '.join(new_modules)}")
        new_rows_data = [[m] + [None]*NUM_DATA_COLS for m in new_modules]
        sheet.append_rows(values=new_rows_data, value_input_option='USER_ENTERED', table_range="A1")
        master_module_list.extend(new_modules)
        
    aligned_data_block = [csv_data_map.get(m, [None] * NUM_DATA_COLS) for m in master_module_list]

    # --- 6. Write Data to Sheet ---
    update_body = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": f"{sheet.title}!{col_to_a1(target_col)}1", "values": [[date_label]]},
            {"range": f"{sheet.title}!{col_to_a1(target_col)}2", "values": [csv_headers]},
            {"range": f"{sheet.title}!{col_to_a1(target_col)}{START_ROW_INDEX + 1}", "values": aligned_data_block}
        ]
    }
    service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=update_body).execute()

    # --- 7. Apply Direct Formatting and Borders ---
    print("  -> Applying direct formatting...")
    requests = []
    
    # Define comparison logic
    comparison_logic = {
        "T": lambda n, r: n == r,
        "P": lambda n, r: n >= r,
        "S": lambda n, r: n <= r,
        "F": lambda n, r: n <= r,
        "I": lambda n, r: n <= r,
        "KI": lambda n, r: n <= r
    }

    for r_idx, module_name in enumerate(master_module_list):
        ref_row = reference_data_map.get(module_name)
        new_row = aligned_data_block[r_idx]

        for c_idx, header in enumerate(csv_headers):
            color_to_apply = None
            new_val = new_row[c_idx]

            if ref_row:
                ref_val = ref_row[c_idx]
                try:
                    if not isinstance(new_val, (int, float)) or not isinstance(ref_val, (int, float)):
                        raise TypeError() # Not a number, don't color
                    
                    if comparison_logic[header](new_val, ref_val):
                        color_to_apply = COLORS["green"]
                    else:
                        color_to_apply = COLORS["red"]
                except (TypeError, KeyError):
                    pass # Non-numeric or header not in logic map
            elif isinstance(new_val, (int, float)): # No reference data, color all numbers red
                color_to_apply = COLORS["red"]

            if color_to_apply:
                requests.append({
                    "updateCells": {
                        "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color_to_apply}}}]}],
                        "fields": "userEnteredFormat.textFormat.foregroundColor",
                        "range": { "sheetId": sheet.id, "startRowIndex": START_ROW_INDEX + r_idx, "endRowIndex": START_ROW_INDEX + r_idx + 1, "startColumnIndex": target_col + c_idx, "endColumnIndex": target_col + c_idx + 1 }
                    }
                })

    # Add cosmetic requests
    data_end_col = target_col + NUM_DATA_COLS
    max_rows = len(master_module_list)
    border_range = {"sheetId": sheet.id, "startRowIndex": 0, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": target_col, "endColumnIndex": data_end_col}
    requests.extend([
        {"updateDimensionProperties": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": target_col, "endIndex": data_end_col}, "properties": {"pixelSize": 80}, "fields": "pixelSize"}},
        {"mergeCells": {"range": {"sheetId": sheet.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "mergeType": "MERGE_ALL"}},
        {"repeatCell": {"range": {"sheetId": sheet.id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "cell": {"userEnteredFormat": {"backgroundColor": COLORS["light_grey_fill"], "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}},
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

        if not os.path.isdir(SOURCE_DIR):
            print(f"❌ ERROR: Source directory '{SOURCE_DIR}' not found.")
            return

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
