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
START_COL = 9 # This is column J
NUM_DATA_COLS = 6
BLOCK_WIDTH = 9 # The number of columns each block spans (A-I, J-R etc.)

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

def clear_formatting_for_range(service, sheet_id, sheet_gid, start_col, end_col):
    """
    Finds and deletes all conditional formatting rules that apply ONLY to the given column range.
    This is used to prevent duplicate rules when overwriting data for an existing date.
    """
    print(f"  -> LOG: Checking for existing conditional format rules in columns {col_to_a1(start_col)}-{col_to_a1(end_col-1)} to clear them.")
    
    try:
        spreadsheet_data = service.spreadsheets().get(spreadsheetId=sheet_id, fields='sheets(properties,conditionalFormats)').execute()
        target_sheet = next((s for s in spreadsheet_data['sheets'] if s['properties']['sheetId'] == sheet_gid), None)
        if not target_sheet or 'conditionalFormats' not in target_sheet:
            return

        all_rules = target_sheet['conditionalFormats']
        requests_to_delete = []
        rule_ids_to_delete = set()

        for rule in all_rules:
            rule_id = rule.get('ruleId')
            if not rule_id: continue
            
            for rule_range in rule['ranges']:
                if rule_range.get('sheetId', sheet_gid) == sheet_gid:
                    range_start = rule_range.get('startColumnIndex', 0)
                    range_end = rule_range.get('endColumnIndex', 1)
                    if max(start_col, range_start) < min(end_col, range_end):
                        rule_ids_to_delete.add(rule_id)
                        break 
        
        if rule_ids_to_delete:
            requests_to_delete = [{"deleteConditionalFormatRule": {"sheetId": sheet_gid, "ruleId": rid}} for rid in rule_ids_to_delete]
            print(f"  -> LOG: Found and removing {len(requests_to_delete)} old formatting rules from target range.")
            service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": requests_to_delete}).execute()
    except Exception as e:
        print(f"  -> WARNING: Could not clear formatting. Error: {e}")


def apply_formatting(service, sheet_id, sheet_gid, target_col, max_rows):
    """Builds and executes all formatting requests for a data block."""
    print(f"  -> Applying new formatting to block starting at {col_to_a1(target_col)}...")
    requests = []
    data_end_col = target_col + NUM_DATA_COLS

    # Standard Formatting
    requests.append({"updateDimensionProperties": {"range": {"sheetId": sheet_gid, "dimension": "COLUMNS", "startIndex": target_col, "endIndex": data_end_col}, "properties": {"pixelSize": 80}, "fields": "pixelSize"}})
    requests.append({"mergeCells": {"range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "mergeType": "MERGE_ALL"}})
    requests.append({"repeatCell": {"range": {"sheetId": sheet_gid, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": target_col, "endIndex": data_end_col}, "cell": {"userEnteredFormat": {"backgroundColor": COLORS["light_grey_fill"], "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}})
    border_range = {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": target_col, "endIndex": data_end_col}
    requests.append({"updateBorders": {"range": border_range, "top": BORDER, "bottom": BORDER, "left": BORDER, "right": BORDER}})
    requests.append({"updateBorders": {"range": border_range, "innerHorizontal": BORDER, "innerVertical": BORDER}})

    # Conditional Formatting
    ref_block_start_col = target_col - BLOCK_WIDTH
    if ref_block_start_col >= 0:
        for i, col_header in enumerate(["T", "P", "S", "F", "I", "KI"]):
            current_col_idx = target_col + i
            ref_col_idx = ref_block_start_col + 1 + i
            
            rule_range = {"sheetId": sheet_gid, "startRowIndex": START_ROW_INDEX, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": current_col_idx, "endIndex": current_col_idx + 1}
            current_cell_a1 = f"{col_to_a1(current_col_idx)}{START_ROW_INDEX + 1}"
            ref_cell_a1 = f"{col_to_a1(ref_col_idx)}{START_ROW_INDEX + 1}"
            
            conditions = {"T": (f"={current_cell_a1}={ref_cell_a1}", f"={current_cell_a1}<>{ref_cell_a1}"), "P": (f"={current_cell_a1}>={ref_cell_a1}", f"={current_cell_a1}<{ref_cell_a1}"), "S": (f"={current_cell_a1}<={ref_cell_a1}", f"={current_cell_a1}>{ref_cell_a1}"), "F": (f"={current_cell_a1}<={ref_cell_a1}", f"={current_cell_a1}>{ref_cell_a1}"), "I": (f"={current_cell_a1}<={ref_cell_a1}", f"={current_cell_a1}>{ref_cell_a1}"), "KI": (f"={current_cell_a1}<={ref_cell_a1}", f"={current_cell_a1}>{ref_cell_a1}")}
            green_cond, red_cond = conditions[col_header]

            green_formula = f"=AND(NOT(ISBLANK({current_cell_a1})), NOT(ISBLANK({ref_cell_a1})), {green_cond.lstrip('=')})"
            red_formula_comp = f"=AND(NOT(ISBLANK({current_cell_a1})), NOT(ISBLANK({ref_cell_a1})), {red_cond.lstrip('=')})"
            red_formula_no_ref = f"=AND(NOT(ISBLANK({current_cell_a1})), ISBLANK({ref_cell_a1}))"

            requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": green_formula}]}, "format": {"textFormat": {"foregroundColor": COLORS["green"]}}}}, "index": 0}})
            requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": red_formula_comp}]}, "format": {"textFormat": {"foregroundColor": COLORS["red"]}}}}, "index": 1}})
            requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": red_formula_no_ref}]}, "format": {"textFormat": {"foregroundColor": COLORS["red"]}}}}, "index": 2}})

    if requests:
        service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    try:
        print(f"\n--- Processing: {sheet_name} from {csv_path} ---")
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

    # --- CSV Parsing and Module Syncing ---
    first_block_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if any(cell.strip() for cell in row[:10]):
                first_block_rows.append(row[:10])
    if len(first_block_rows) < 2 or len(first_block_rows[0]) < NUM_DATA_COLS + 2: return
    date_label = first_block_rows[1][0].strip()
    csv_headers = [h.strip() for h in first_block_rows[0][2:2 + NUM_DATA_COLS]]
    csv_data_map = {row[1].strip(): [convert_cell(c) for c in row[2:2 + NUM_DATA_COLS]] for row in first_block_rows[1:] if len(row) > 1 and row[1].strip()}
    existing_data = sheet.get_all_values()
    master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]] if len(existing_data) > START_ROW_INDEX else []
    new_modules = sorted([m for m in csv_data_map if m not in master_module_list])
    if new_modules:
        sheet.append_rows([[m] for m in new_modules], value_input_option='USER_ENTERED', table_range=f"A{len(master_module_list) + START_ROW_INDEX + 1}")
        master_module_list.extend(new_modules)
    aligned_data_block = [csv_data_map.get(m, [None] * NUM_DATA_COLS) for m in master_module_list]
    
    # --- REVISED, SIMPLIFIED LOGIC ---
    sheet_headers = existing_data[0] if existing_data else []
    target_col = -1
    
    if date_label in sheet_headers:
        # SCENARIO 1: DATE EXISTS. We will overwrite it.
        target_col = sheet_headers.index(date_label)
        print(f"  -> LOG: Date '{date_label}' found. Will overwrite at column {col_to_a1(target_col)}.")
        # Clean the target area BEFORE writing new data to avoid rule conflicts.
        clear_formatting_for_range(service, SPREADSHEET_ID, sheet.id, target_col, target_col + NUM_DATA_COLS)
    else:
        # SCENARIO 2: NEW DATE. We will insert columns.
        target_col = START_COL
        print(f"  -> LOG: New date '{date_label}'. Inserting columns at {col_to_a1(target_col)}.")
        # This command shifts everything from J onwards to the right, PRESERVING formatting.
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [{"insertDimension": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": START_COL, "endIndex": START_COL + BLOCK_WIDTH}, "inheritFromBefore": False}}]}).execute()
        # We DO NOT clear formatting for the shifted columns.
    
    # This part now runs for BOTH scenarios. It writes data into the determined target_col.
    update_body = { "valueInputoption": "USER_ENTERED", "data": [ {"range": f"{sheet.title}!{col_to_a1(target_col)}1", "values": [[date_label]]}, {"range": f"{sheet.title}!{col_to_a1(target_col)}2", "values": [csv_headers]}, {"range": f"{sheet.title}!{col_to_a1(target_col)}{START_ROW_INDEX + 1}", "values": aligned_data_block}]}
    print(f"  -> LOG: Writing data to column {col_to_a1(target_col)}.")
    service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=update_body).execute()

    # This will apply fresh formatting to the data we just wrote at target_col.
    # It will NOT touch the shifted columns.
    apply_formatting(service, SPREADSHEET_ID, sheet.id, target_col, len(master_module_list))
    
    print(f"✅ Sheet '{sheet_name}' updated successfully.")

def main():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
        creds = GoogleCredentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        service = build("sheets", "v4", credentials=creds)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        csv_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")])
        for filename in csv_files:
            update_sheet(service, spreadsheet, os.path.splitext(filename)[0], os.path.join(SOURCE_DIR, filename))
    except FileNotFoundError: print(f"❌ CRITICAL ERROR: Credentials file '{CREDENTIALS_FILE}' not found.")
    except Exception as e: print(f"❌ A critical error occurred: {e}"); raise

if __name__ == "__main__":
    main()
