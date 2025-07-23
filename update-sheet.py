import os
import csv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
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
BLOCK_WIDTH = 9 # Number of columns for a full data block with spacing

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
    """Safely converts a string value to a number or returns it as a string."""
    if val is None or str(val).strip() == '':
        return None
    try:
        f = float(val)
        return int(f) if f.is_integer() else f
    except (ValueError, TypeError):
        return str(val).strip()

def find_reference_block(sheet_headers, main_block_start, block_width):
    """
    Looks for a second block of T,P,S,F,I,KI headers after the main block.
    Returns the starting column index of the reference block, or None if not found.
    """
    expected_headers = ["T", "P", "S", "F", "I", "KI"]
    for i in range(main_block_start + block_width, len(sheet_headers) - len(expected_headers) + 1):
        if sheet_headers[i:i+len(expected_headers)] == expected_headers:
            return i
    return None

def apply_formatting(service, sheet_id, sheet_gid, target_col, has_reference_data, max_rows, reference_block_start=None):
    """
    Builds and executes all formatting requests for a data block.
    Uses the actual reference block start if available.
    """
    print(f"[DEBUG] apply_formatting: target_col={target_col}, has_reference_data={has_reference_data}, max_rows={max_rows}, reference_block_start={reference_block_start}")
    requests = []
    # Use the detected reference block start if available
    ref_col = reference_block_start if reference_block_start is not None else (target_col + BLOCK_WIDTH)
    data_end_col = target_col + NUM_DATA_COLS

    # Set column width and merge header cells
    requests.append({"updateDimensionProperties": {"range": {"sheetId": sheet_gid, "dimension": "COLUMNS", "startIndex": target_col, "endIndex": data_end_col}, "properties": {"pixelSize": 80}, "fields": "pixelSize"}})
    requests.append({"mergeCells": {"range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "mergeType": "MERGE_ALL"}})
    requests.append({"repeatCell": {"range": {"sheetId": sheet_gid, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "cell": {"userEnteredFormat": {"backgroundColor": COLORS["light_grey_fill"], "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}})
    
    border_range = {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": target_col, "endColumnIndex": data_end_col}
    requests.append({"updateBorders": {"range": border_range, "top": BORDER, "bottom": BORDER, "left": BORDER, "right": BORDER}})
    requests.append({"updateBorders": {"range": border_range, "innerHorizontal": BORDER, "innerVertical": BORDER}})

    cond_format_rules = []
    for i, col_header in enumerate(["T", "P", "S", "F", "I", "KI"]):
        current_col_idx = target_col + i
        rule_range = {"sheetId": sheet_gid, "startRowIndex": START_ROW_INDEX, "startColumnIndex": current_col_idx, "endColumnIndex": current_col_idx + 1}
        print(f"[DEBUG] Formatting col '{col_header}' at index {current_col_idx}")
        if not has_reference_data:
            # Only red for not blank if no reference data
            print(f"[DEBUG] No reference data: Only red rule for NOT_BLANK on col '{col_header}'")
            rule = {"ranges": [rule_range], "booleanRule": {"condition": {"type": "NOT_BLANK"}, "format": {"textFormat": {"foregroundColor": COLORS["red"]}}}}
            cond_format_rules.append({"addConditionalFormatRule": {"rule": rule, "index": 0}})
        else:
            # With reference data, set up green/red rules
            ref_col_a1 = col_to_a1(ref_col + i)
            current_cell_a1 = f"{col_to_a1(current_col_idx)}{START_ROW_INDEX + 1}"
            ref_cell_a1 = f"{ref_col_a1}{START_ROW_INDEX + 1}"
            print(f"[DEBUG] Reference cell for '{col_header}': {ref_cell_a1}")
            conditions = {
                "T":  (f"{current_cell_a1}={ref_cell_a1}", f"{current_cell_a1}<>{ref_cell_a1}"),
                "P":  (f"{current_cell_a1}>={ref_cell_a1}", f"{current_cell_a1}<{ref_cell_a1}"),
                "S":  (f"{current_cell_a1}<={ref_cell_a1}", f"{current_cell_a1}>{ref_cell_a1}"),
                "F":  (f"{current_cell_a1}<={ref_cell_a1}", f"{current_cell_a1}>{ref_cell_a1}"),
                "I":  (f"{current_cell_a1}<={ref_cell_a1}", f"{current_cell_a1}>{ref_cell_a1}"),
                "KI": (f"{current_cell_a1}<={ref_cell_a1}", f"{current_cell_a1}>{ref_cell_a1}")
            }
            green_cond, red_cond = conditions[col_header]
            green_formula = f"=AND(NOT(ISBLANK({current_cell_a1})), NOT(ISBLANK({ref_cell_a1})), {green_cond.lstrip('=')})"
            red_formula = f"=AND(NOT(ISBLANK({current_cell_a1})), NOT(ISBLANK({ref_cell_a1})), {red_cond.lstrip('=')})"
            print(f"[DEBUG] Green formula for '{col_header}': {green_formula}")
            print(f"[DEBUG] Red formula for '{col_header}': {red_formula}")
            green_rule = {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": green_formula}]}, "format": {"textFormat": {"foregroundColor": COLORS["green"]}}}}
            red_rule = {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": red_formula}]}, "format": {"textFormat": {"foregroundColor": COLORS["red"]}}}}
            cond_format_rules.append({"addConditionalFormatRule": {"rule": green_rule, "index": 0}})
            cond_format_rules.append({"addConditionalFormatRule": {"rule": red_rule, "index": 0}})

    requests.extend(cond_format_rules)
    if requests:
        print(f"[DEBUG] Sending {len(requests)} formatting requests to Sheets API.")
        service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()
    else:
        print("[DEBUG] No formatting requests to send.")

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    try:
        print(f"--- Processing: {sheet_name} from {csv_path} ---")
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it.")
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

    # *** FIX #1: Only parse the FIRST data block from any CSV ***
    first_block_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            # A block is ~10 columns wide. We take the first part of the row.
            block_segment = row[:10] 
            if any(cell.strip() for cell in block_segment):
                first_block_rows.append(block_segment)

    if len(first_block_rows) < 2 or len(first_block_rows[0]) < NUM_DATA_COLS + 2:
        print(f"⚠️ WARNING: First data block in CSV '{csv_path}' is incomplete. Skipping.")
        return

    date_label = first_block_rows[1][0].strip()
    csv_headers = [h.strip() for h in first_block_rows[0][2:2 + NUM_DATA_COLS]]
    csv_data_map = {row[1].strip(): [convert_cell(c) for c in row[2:2 + NUM_DATA_COLS]] for row in first_block_rows[1:] if len(row) > 1 and row[1].strip()}

    existing_data = sheet.get_all_values()
    master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]] if len(existing_data) > START_ROW_INDEX else []
    
    new_modules = [m for m in csv_data_map if m not in master_module_list]
    if new_modules:
        print(f"  -> New modules found: {', '.join(new_modules)}")
        new_rows = [[m] for m in new_modules]
        sheet.append_rows(values=new_rows, value_input_option='USER_ENTERED', table_range=f"A{len(master_module_list) + START_ROW_INDEX + 1}")
        master_module_list.extend(new_modules)
    
    aligned_data_block = [csv_data_map.get(m, [None] * NUM_DATA_COLS) for m in master_module_list]

    sheet_headers = existing_data[0] if existing_data else []
    target_col = -1
    try:
        target_col = sheet_headers.index(date_label)
    except ValueError:
        pass # Not found, target_col remains -1
    
    print(f"[DEBUG] Sheet headers: {sheet_headers}")
    reference_block_start = None
    if target_col != -1:
        print(f"  -> Date '{date_label}' found. Updating columns in place at col {target_col}.")
        reference_block_start = find_reference_block(sheet_headers, target_col, BLOCK_WIDTH)
        has_reference_data = reference_block_start is not None
        if has_reference_data:
            print(f"[DEBUG] Reference block found at column {reference_block_start}")
        else:
            print(f"[DEBUG] No reference block found after main block.")
    else:
        print(f"  -> Date '{date_label}' not found. Inserting new columns at {START_COL}.")
        target_col = START_COL
        reference_block_start = find_reference_block(sheet_headers, target_col, BLOCK_WIDTH)
        has_reference_data = reference_block_start is not None
        if has_reference_data:
            print(f"[DEBUG] Reference block found at column {reference_block_start}")
        else:
            print(f"[DEBUG] No reference block found after main block.")
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [{"insertDimension": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": START_COL, "endIndex": START_COL + BLOCK_WIDTH}, "inheritFromBefore": False}}]}).execute()

    print(f"[DEBUG] has_reference_data: {has_reference_data}")
    sheet.update(range_name=f"{col_to_a1(target_col)}1", values=[[date_label]])
    sheet.update(range_name=f"{col_to_a1(target_col)}2", values=[csv_headers])
    sheet.update(range_name=f"{col_to_a1(target_col)}{START_ROW_INDEX + 1}", values=aligned_data_block, value_input_option='USER_ENTERED')
    
    print(f"  -> Applying formatting (Reference found: {has_reference_data})...")
    apply_formatting(service, SPREADSHEET_ID, sheet.id, target_col, has_reference_data, len(master_module_list), reference_block_start=reference_block_start)
    
    print(f"✅ Sheet '{sheet_name}' updated successfully.")

def main():
    try:
        # Set up Google Sheets API credentials
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
        creds = GoogleCredentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        service = build("sheets", "v4", credentials=creds)
        
        gspread_creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(gspread_creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        if not os.path.isdir(SOURCE_DIR):
            print(f"❌ ERROR: Source directory '{SOURCE_DIR}' not found.")
            return

        csv_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")])
        if not csv_files:
            print("No CSV files found in 'source' directory. Nothing to do.")
            return

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
