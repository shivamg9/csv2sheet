import os
import csv
import gspread
from google.oauth2.service_account import Credentials as GoogleCredentials
from googleapiclient.discovery import build
import time

# --- CONFIGURATION ---
SOURCE_DIR = "source"
SPREADSHEET_ID = "1aoT-ponIDn0hLWQ-K8hTYljk9D3P5IgK7ZgvoxE31fU"
CREDENTIALS_FILE = "creds.json"

# --- CONSTANTS ---
START_ROW_INDEX = 2
START_COL = 9 # This is column J, the first column for date-based data blocks.
NUM_DATA_COLS = 6
BLOCK_WIDTH = 9 # The number of columns each block spans (e.g., A-I, J-R, S-AA).

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

def bake_in_and_remove_formatting(service, sheet_id, sheet_gid, target_col, max_rows):
    """
    Reads the effective formatting of a data block, applies it directly,
    and then removes the conditional formatting rules that generated it.
    """
    print(f"  -> Baking in formats for block starting at column {col_to_a1(target_col)}...")
    requests = []
    
    # Define the range of the block to be "baked"
    data_end_col = target_col + NUM_DATA_COLS
    bake_range_a1 = f'{col_to_a1(target_col)}{START_ROW_INDEX + 1}:{col_to_a1(data_end_col - 1)}{START_ROW_INDEX + max_rows}'

    # Get spreadsheet data including effective formats and conditional format rules
    try:
        sheet_data = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            ranges=[bake_range_a1],
            fields='sheets(data(rowData(values(effectiveFormat))),conditionalFormats)'
        ).execute()
    except Exception as e:
        print(f"  -> WARNING: Could not retrieve sheet data for baking. Skipping. Error: {e}")
        return

    sheet_info = sheet_data.get('sheets', [])[0]
    rows_data = sheet_info.get('data', [])[0].get('rowData', [])
    rules_to_delete = set()

    # 1. Build requests to apply direct formatting based on effective format
    for r_idx, row in enumerate(rows_data):
        cells = row.get('values', [])
        for c_idx, cell in enumerate(cells):
            effective_format = cell.get('effectiveFormat', {})
            if 'foregroundColor' in effective_format.get('textFormat', {}):
                color = effective_format['textFormat']['foregroundColor']
                # Check if the color is one of our red/green conditional colors
                if (color == COLORS["red"] or color == COLORS["green"]):
                    requests.append({
                        "updateCell": {
                            "range": {
                                "sheetId": sheet_gid,
                                "startRowIndex": START_ROW_INDEX + r_idx,
                                "endRowIndex": START_ROW_INDEX + r_idx + 1,
                                "startColumnIndex": target_col + c_idx,
                                "endColumnIndex": target_col + c_idx + 1,
                            },
                            "rows": [{"values": [{"userEnteredFormat": {"textFormat": {"foregroundColor": color}}}]}],
                            "fields": "userEnteredFormat.textFormat.foregroundColor"
                        }
                    })

    # 2. Find and build requests to delete the old conditional formatting rules
    all_rules = sheet_info.get('conditionalFormats', [])
    for rule in all_rules:
        for r in rule.get('ranges', []):
            if r['startColumnIndex'] >= target_col and r['endColumnIndex'] <= data_end_col:
                rules_to_delete.add(rule['ruleId'])

    for rule_id in rules_to_delete:
        requests.append({"deleteConditionalFormatRule": {"sheetId": sheet_gid, "ruleId": rule_id}})

    # 3. Execute all requests in a single batch
    if requests:
        print(f"  -> LOG: Found {len(requests) - len(rules_to_delete)} cells to format and {len(rules_to_delete)} rules to delete. Executing batch update.")
        try:
            service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()
        except Exception as e:
            print(f"  -> WARNING: Batch update for baking formats failed. Error: {e}")
    else:
        print("  -> LOG: No conditional formatting found to bake in.")


def apply_new_conditional_formatting(service, sheet_id, sheet_gid, target_col, max_rows):
    """Builds and executes all formatting requests for a NEW data block."""
    print("  -> Building and applying new formatting requests...")
    requests = []
    data_end_col = target_col + NUM_DATA_COLS

    # Standard Formatting
    requests.append({"updateDimensionProperties": {"range": {"sheetId": sheet_gid, "dimension": "COLUMNS", "startIndex": target_col, "endIndex": data_end_col}, "properties": {"pixelSize": 80}, "fields": "pixelSize"}})
    requests.append({"mergeCells": {"range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "mergeType": "MERGE_ALL"}})
    requests.append({"repeatCell": {"range": {"sheetId": sheet_gid, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "cell": {"userEnteredFormat": {"backgroundColor": COLORS["light_grey_fill"], "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}})
    border_range = {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": target_col, "endColumnIndex": data_end_col}
    requests.append({"updateBorders": {"range": border_range, "top": BORDER, "bottom": BORDER, "left": BORDER, "right": BORDER}})
    requests.append({"updateBorders": {"range": border_range, "innerHorizontal": BORDER, "innerVertical": BORDER}})

    # Conditional Formatting
    for i, col_header in enumerate(["T", "P", "S", "F", "I", "KI"]):
        current_col_idx = target_col + i
        ref_col_idx = 1 + i # Reference block's data starts in column B (index 1)
        
        rule_range = {"sheetId": sheet_gid, "startRowIndex": START_ROW_INDEX, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": current_col_idx, "endColumnIndex": current_col_idx + 1}
        current_cell_a1 = f"{col_to_a1(current_col_idx)}{START_ROW_INDEX + 1}"
        ref_cell_a1 = f"{col_to_a1(ref_col_idx)}{START_ROW_INDEX + 1}"
        
        conditions = { "T": (f"={current_cell_a1}={ref_cell_a1}", f"={current_cell_a1}<>{ref_cell_a1}"), "P": (f"={current_cell_a1}>={ref_cell_a1}", f"={current_cell_a1}<{ref_cell_a1}"), "S": (f"={current_cell_a1}<={ref_cell_a1}", f"={current_cell_a1}>{ref_cell_a1}"), "F": (f"={current_cell_a1}<={ref_cell_a1}", f"={current_cell_a1}>{ref_cell_a1}"), "I": (f"={current_cell_a1}<={ref_cell_a1}", f"={current_cell_a1}>{ref_cell_a1}"), "KI": (f"={current_cell_a1}<={ref_cell_a1}", f"={current_cell_a1}>{ref_cell_a1}") }
        green_cond, red_cond = conditions[col_header]

        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f"=AND(NOT(ISBLANK({current_cell_a1})), NOT(ISBLANK({ref_cell_a1})), {green_cond})"}]}, "format": {"textFormat": {"foregroundColor": COLORS["green"]}}}}, "index": 0}})
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f"=AND(NOT(ISBLANK({current_cell_a1})), NOT(ISBLANK({ref_cell_a1})), {red_cond})"}]}, "format": {"textFormat": {"foregroundColor": COLORS["red"]}}}}, "index": 1}})
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f"=AND(NOT(ISBLANK({current_cell_a1})), ISBLANK({ref_cell_a1}))"}]}, "format": {"textFormat": {"foregroundColor": COLORS["red"]}}}}, "index": 2}})

    if requests:
        service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    try:
        print(f"\n--- Processing: {sheet_name} from {csv_path} ---")
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it.")
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

    first_block_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            block_segment = row[:10]
            if any(cell.strip() for cell in block_segment):
                first_block_rows.append(block_segment)

    if len(first_block_rows) < 2 or len(first_block_rows[0]) < NUM_DATA_COLS + 2:
        print(f"⚠️ WARNING: CSV '{csv_path}' seems malformed. Skipping.")
        return

    date_label = first_block_rows[1][0].strip()
    csv_headers = [h.strip() for h in first_block_rows[0][2:2 + NUM_DATA_COLS]]
    csv_data_map = {row[1].strip(): [convert_cell(c) for c in row[2:2 + NUM_DATA_COLS]] for row in first_block_rows[1:] if len(row) > 1 and row[1].strip()}
    print(f"  -> Found date '{date_label}' with {len(csv_data_map)} modules in CSV.")

    existing_data = sheet.get_all_values()
    master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]] if len(existing_data) > START_ROW_INDEX else []
    
    new_modules = sorted([m for m in csv_data_map if m not in master_module_list])
    if new_modules:
        print(f"  -> New modules found: {', '.join(new_modules)}. Appending to sheet.")
        sheet.append_rows(values=[[m] for m in new_modules], value_input_option='USER_ENTERED', table_range=f"A{len(master_module_list) + START_ROW_INDEX + 1}")
        master_module_list.extend(new_modules)
    
    aligned_data_block = [csv_data_map.get(m, [None] * NUM_DATA_COLS) for m in master_module_list]

    sheet_headers = existing_data[0] if existing_data else []
    target_col = -1
    if date_label in sheet_headers:
        target_col = sheet_headers.index(date_label)

    if target_col == -1:
        print(f"  -> Date '{date_label}' not in headers. Inserting new columns.")
        # --- BAKE-IN LOGIC ---
        # Before inserting, check if there's an existing block at the insertion point (START_COL)
        # The header for the first data block is at row index 0.
        if len(sheet_headers) > START_COL and sheet_headers[START_COL]:
            bake_in_and_remove_formatting(service, SPREADSHEET_ID, sheet.id, START_COL, len(master_module_list))
        
        # Now, insert the new columns.
        target_col = START_COL
        insert_req = {"insertDimension": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": START_COL, "endIndex": START_COL + BLOCK_WIDTH}, "inheritFromBefore": False}}
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [insert_req]}).execute()
        time.sleep(1) # Brief pause to allow API changes to settle if needed.

    else:
        print(f"  -> Date '{date_label}' found. Updating columns in place at {col_to_a1(target_col)}.")

    update_body = {"valueInputOption": "USER_ENTERED", "data": [
            {"range": f"{sheet.title}!{col_to_a1(target_col)}1", "values": [[date_label]]},
            {"range": f"{sheet.title}!{col_to_a1(target_col)}2", "values": [csv_headers]},
            {"range": f"{sheet.title}!{col_to_a1(target_col)}{START_ROW_INDEX + 1}", "values": aligned_data_block}
        ]}
    print(f"  -> Writing data to sheet '{sheet.title}' starting at column {col_to_a1(target_col)}.")
    service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=update_body).execute()
    
    # Apply fresh conditional formatting to the new/updated block
    apply_new_conditional_formatting(service, SPREADSHEET_ID, sheet.id, target_col, len(master_module_list))
    
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
