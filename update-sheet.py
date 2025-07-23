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
START_ROW_INDEX = 2  # Data starts at row 3 (0-indexed)
START_COL = 9        # Data starts at column J (0-indexed)
NUM_DATA_COLS = 6    # T, P, S, F, I, KI
BLOCK_WIDTH = 9      # Width to insert to maintain spacing

# --- FORMATTING STYLES ---
COLORS = {
    "red": {"red": 0.9, "green": 0.6, "blue": 0.6},
    "green": {"red": 0.7, "green": 0.9, "blue": 0.7},
    "light_grey": {"red": 0.85, "green": 0.85, "blue": 0.85}
}
BOLD_FORMAT = {"textFormat": {"bold": True}}
BORDER = {"style": "SOLID", "width": 1}

def col_to_a1(col_idx):
    """Converts a 0-indexed column number to A1 notation (e.g., 0 -> A, 26 -> AA)."""
    a1 = ""
    col_idx += 1
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx - 1, 26)
        a1 = chr(65 + remainder) + a1
    return a1

def convert_cell(val):
    """Safely converts a string value to an int or float."""
    if val is None or str(val).strip() == '':
        return None
    try:
        f = float(val)
        return int(f) if f.is_integer() else f
    except (ValueError, TypeError):
        return str(val).strip()

def apply_formatting(service, sheet_id, sheet_gid, target_col, has_reference_data, max_rows):
    """Builds and executes all formatting requests for a data block."""
    requests = []
    ref_col = target_col + BLOCK_WIDTH
    data_end_col = target_col + NUM_DATA_COLS
    
    # 1. Column Width
    requests.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_gid, "dimension": "COLUMNS", "startIndex": target_col, "endIndex": data_end_col},
            "properties": {"pixelSize": 80},
            "fields": "pixelSize"
        }
    })
    
    # 2. Merge Date Header
    requests.append({"mergeCells": {"range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "mergeType": "MERGE_ALL"}})
    
    # 3. Style T, P, S... Headers (Bold, Grey Fill)
    requests.append({
        "repeatCell": {
            "range": {"sheetId": sheet_gid, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": target_col, "endColumnIndex": data_end_col},
            "cell": {"userEnteredFormat": {"backgroundColor": COLORS["light_grey"], "textFormat": {"bold": True}}},
            "fields": "userEnteredFormat(backgroundColor,textFormat)"
        }
    })

    # 4. Apply Borders around the entire block
    requests.append({
        "updateBorders": {
            "range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": target_col, "endColumnIndex": data_end_col},
            "top": BORDER, "bottom": BORDER, "left": BORDER, "right": BORDER
        }
    })
    requests.append({
        "updateBorders": {
            "range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": target_col, "endColumnIndex": data_end_col},
            "innerHorizontal": BORDER, "innerVertical": BORDER
        }
    })

    # 5. Conditional Formatting
    rules = []
    for i, col_header in enumerate(["T", "P", "S", "F", "I", "KI"]):
        current_col_idx = target_col + i
        rule_range = {"sheetId": sheet_gid, "startRowIndex": START_ROW_INDEX, "startColumnIndex": current_col_idx, "endIndex": current_col_idx + 1}
        
        if not has_reference_data:
            # If no reference data, all cells are red
            rules.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": "=TRUE"}]}, "format": {"backgroundColor": COLORS["red"]}}}, "index": 0}})
        else:
            # Rules with reference comparison
            ref_col_a1 = col_to_a1(ref_col + i)
            current_cell_a1_relative = f"{col_to_a1(current_col_idx)}{START_ROW_INDEX + 1}"
            ref_cell_a1_relative = f"{ref_col_a1}{START_ROW_INDEX + 1}"
            
            # Define Green and Red conditions based on column
            conditions = {
                "T": (f"={current_cell_a1_relative}={ref_cell_a1_relative}", f"={current_cell_a1_relative}<>{ref_cell_a1_relative}"),
                "P": (f"={current_cell_a1_relative}>={ref_cell_a1_relative}", f"={current_cell_a1_relative}<{ref_cell_a1_relative}"),
                "S": (f"={current_cell_a1_relative}<={ref_cell_a1_relative}", f"={current_cell_a1_relative}>{ref_cell_a1_relative}"),
                "F": (f"={current_cell_a1_relative}<={ref_cell_a1_relative}", f"={current_cell_a1_relative}>{ref_cell_a1_relative}"),
                "I": (f"={current_cell_a1_relative}<={ref_cell_a1_relative}", f"={current_cell_a1_relative}>{ref_cell_a1_relative}"),
                "KI": (f"={current_cell_a1_relative}<={ref_cell_a1_relative}", f"={current_cell_a1_relative}>{ref_cell_a1_relative}")
            }
            green_formula, red_formula = conditions[col_header]

            # Add rule for green and red, ensuring blank reference cells are ignored
            base_check = f"AND(NOT(ISBLANK({ref_cell_a1_relative})), NOT(ISBLANK({current_cell_a1_relative})))"
            rules.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f"={base_check.replace(')', ',' + green_formula.replace('=', '', 1) + ')' )}" ]}, "format": {"backgroundColor": COLORS["green"]}}}, "index": 0}})
            rules.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f"={base_check.replace(')', ',' + red_formula.replace('=', '', 1) + ')' )}" ]}, "format": {"backgroundColor": COLORS["red"]}}}, "index": 0}})

    requests.extend(rules)
    
    if requests:
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    try:
        print(f"--- Processing: {sheet_name} from {csv_path} ---")
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it.")
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = list(csv.reader(f))
        if len(reader) < 2 or len(reader[0]) < NUM_DATA_COLS + 2:
            print(f"⚠️ WARNING: CSV '{csv_path}' is incomplete or empty. Skipping.")
            return

    date_label = reader[1][0].strip()
    csv_headers = [h.strip() for h in reader[0][2:2+NUM_DATA_COLS]]
    csv_data_map = {row[1].strip(): [convert_cell(c) for c in row[2:2+NUM_DATA_COLS]] for row in reader[1:] if row and row[1].strip()}
    
    existing_data = sheet.get_all_values()
    master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]] if len(existing_data) > START_ROW_INDEX else []
    
    new_modules = [m for m in csv_data_map if m not in master_module_list]
    if new_modules:
        print(f"  -> New modules found: {', '.join(new_modules)}")
        new_rows = [[m] for m in new_modules]
        sheet.append_rows(new_rows, value_input_option='USER_ENTERED', table_range="A1")
        master_module_list.extend(new_modules)

    aligned_data_block = [csv_data_map.get(m, [None] * NUM_DATA_COLS) for m in master_module_list]

    sheet_headers = existing_data[0] if existing_data else []
    try:
        target_col = sheet_headers.index(date_label)
        print(f"  -> Date '{date_label}' found. Updating columns in place.")
        has_reference_data = len(sheet_headers) > target_col + BLOCK_WIDTH and sheet_headers[target_col + BLOCK_WIDTH]
    except ValueError:
        print(f"  -> Date '{date_label}' not found. Inserting new columns.")
        target_col = START_COL
        # Insert new columns, which will shift existing data and formatting
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [{"insertDimension": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": START_COL, "endIndex": START_COL + BLOCK_WIDTH}, "inheritFromBefore": False}}]}).execute()
        has_reference_data = len(sheet_headers) > START_COL and sheet_headers[START_COL]

    # Write data to the sheet
    sheet.update_cell(1, target_col + 1, date_label)
    sheet.update(f"{col_to_a1(target_col)}{2}", [csv_headers])
    sheet.update(f"{col_to_a1(target_col)}{START_ROW_INDEX + 1}", aligned_data_block, value_input_option='USER_ENTERED')
    
    # Apply all formatting
    print("  -> Applying formatting...")
    apply_formatting(service, SPREADSHEET_ID, sheet.id, target_col, has_reference_data, len(master_module_list))
    
    print(f"✅ Sheet '{sheet_name}' updated successfully.")

def main():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = GoogleCredentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        service = build("sheets", "v4", credentials=creds)
        
        gspread_creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(gspread_creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        if not os.path.isdir(SOURCE_DIR):
            print(f"❌ ERROR: Source directory '{SOURCE_DIR}' not found.")
            return

        for filename in sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")]):
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
