import os
import csv
import gspread
from google.oauth2.service_account import Credentials as GoogleCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- CONFIGURATION ---
SOURCE_DIR = "source"
SPREADSHEET_ID = "1aoT-ponIDn0hLWQ-K8hTYljk9D3P5IgK7ZgvoxE31fU"
CREDENTIALS_FILE = "creds.json"

# --- CONSTANTS ---
START_ROW_INDEX = 2
# The first block of data to be written will always start at column J.
FIRST_DATA_BLOCK_COL = 9 # Column J
# The reference data (the first data set on the sheet) starts at column B.
REFERENCE_DATA_COL = 1 # Column B
NUM_DATA_COLS = 6
BLOCK_WIDTH = 9 # The number of columns each date block spans (A-I, J-R etc.)

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
        f = float(str(val).replace(",", "")) # Handle numbers with commas
        return int(f) if f.is_integer() else f
    except (ValueError, TypeError):
        return str(val).strip()

def _freeze_formatting_for_block(service, sheet, existing_data, master_module_list):
    """
    Evaluates, applies, and then removes conditional formatting for the first data block
    at FIRST_DATA_BLOCK_COL, "freezing" the colors before it's shifted.
    """
    print("  -> LOG: Checking if formatting needs to be frozen for the existing block at column J...")
    # Check if there is actually a data block at the target location to be frozen
    if len(existing_data[0]) <= FIRST_DATA_BLOCK_COL or not existing_data[0][FIRST_DATA_BLOCK_COL]:
        print("  -> LOG: No existing data block found at column J. Nothing to freeze.")
        return

    print("  -> LOG: Freezing formatting for block starting at column J.")
    requests = []
    col_headers = [h.strip() for h in existing_data[1][FIRST_DATA_BLOCK_COL:FIRST_DATA_BLOCK_COL + NUM_DATA_COLS]]

    # 1. Build direct formatting requests by evaluating current data
    for row_idx, module_name in enumerate(master_module_list):
        for col_offset, header in enumerate(col_headers):
            current_col_idx = FIRST_DATA_BLOCK_COL + col_offset
            ref_col_idx = REFERENCE_DATA_COL + col_offset
            sheet_row_idx = START_ROW_INDEX + row_idx

            try:
                current_val = convert_cell(existing_data[sheet_row_idx][current_col_idx])
                ref_val = convert_cell(existing_data[sheet_row_idx][ref_col_idx])
            except IndexError:
                continue # Skip if row/column doesn't exist in the data array

            if current_val is None:
                continue # No data in the cell, no color needed

            color = None
            # Determine color based on the same logic as the conditional formatting
            if ref_val is None:
                color = COLORS["red"] # Data exists with no reference to compare to
            else:
                try:
                    is_pass = False
                    if header == "T": is_pass = (current_val == ref_val)
                    elif header == "P": is_pass = (current_val >= ref_val)
                    elif header in ["S", "F", "I", "KI"]: is_pass = (current_val <= ref_val)
                    
                    color = COLORS["green"] if is_pass else COLORS["red"]
                except TypeError:
                    color = COLORS["red"] # Fails comparison if types are different (e.g., text vs number)

            if color:
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": sheet.id, "startRowIndex": sheet_row_idx, "endRowIndex": sheet_row_idx + 1, "startColumnIndex": current_col_idx, "endColumnIndex": current_col_idx + 1},
                        "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}},
                        "fields": "userEnteredFormat.textFormat.foregroundColor"
                    }
                })

    # 2. Find and delete the old conditional formatting rules for this block
    try:
        sheet_properties = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID, ranges=sheet.title, fields="sheets.conditionalFormats").execute()
        rules = sheet_properties.get('sheets', [{}])[0].get('conditionalFormats', [])
        for i, rule in enumerate(rules):
            for r in rule.get('ranges', []):
                # Check if the rule applies to the column range we are freezing
                if r.get('startColumnIndex') == FIRST_DATA_BLOCK_COL:
                    requests.append({"deleteConditionalFormatRule": {"sheetId": sheet.id, "index": i}})
                    # We add one delete request per rule, not per range in the rule
                    break
    except HttpError as e:
        print(f"  -> WARNING: Could not fetch conditional format rules. May not be able to clear them. Error: {e}")


    # 3. Execute all requests if any were generated
    if requests:
        print(f"  -> LOG: Applying {len(requests)} direct formatting changes and removing old rules.")
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()
    else:
        print("  -> LOG: No formatting changes were needed for the existing block.")


def apply_formatting(service, sheet_id, sheet_gid, target_col, max_rows):
    """Builds and executes all formatting requests for a data block."""
    print("  -> Building formatting requests...")
    requests = []
    data_end_col = target_col + NUM_DATA_COLS

    # --- Standard Formatting (Borders, Headers, etc.) ---
    # (This part is unchanged)
    requests.append({"updateDimensionProperties": {"range": {"sheetId": sheet_gid, "dimension": "COLUMNS", "startIndex": target_col, "endIndex": data_end_col}, "properties": {"pixelSize": 80}, "fields": "pixelSize"}})
    requests.append({"mergeCells": {"range": {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "mergeType": "MERGE_ALL"}})
    requests.append({"repeatCell": {"range": {"sheetId": sheet_gid, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": target_col, "endColumnIndex": data_end_col}, "cell": {"userEnteredFormat": {"backgroundColor": COLORS["light_grey_fill"], "textFormat": {"bold": True}}}, "fields": "userEnteredFormat(backgroundColor,textFormat)"}})
    border_range = {"sheetId": sheet_gid, "startRowIndex": 0, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": target_col, "endColumnIndex": data_end_col}
    requests.append({"updateBorders": {"range": border_range, "top": BORDER, "bottom": BORDER, "left": BORDER, "right": BORDER}})
    requests.append({"updateBorders": {"range": border_range, "innerHorizontal": BORDER, "innerVertical": BORDER}})


    # --- Conditional Formatting ---
    print(f"  -> LOG: Setting up conditional formatting. target_col={target_col}, BLOCK_WIDTH={BLOCK_WIDTH}, max_rows={max_rows}")
    for i, col_header in enumerate(["T", "P", "S", "F", "I", "KI"]):
        current_col_idx = target_col + i
        ref_col_idx = REFERENCE_DATA_COL + i # Always compare against the first block (B, C, D...)

        print(f"\n    -> LOG: Formatting rules for header '{col_header}':")
        print(f"       - Current data is in column: {col_to_a1(current_col_idx)} (index {current_col_idx})")
        print(f"       - Comparing against reference data in column: {col_to_a1(ref_col_idx)} (index {ref_col_idx})")
        
        rule_range = {"sheetId": sheet_gid, "startRowIndex": START_ROW_INDEX, "endRowIndex": START_ROW_INDEX + max_rows, "startColumnIndex": current_col_idx, "endColumnIndex": current_col_idx + 1}
        
        current_cell_a1 = f"{col_to_a1(current_col_idx)}{START_ROW_INDEX + 1}"
        # Use $ to make the reference column absolute
        ref_cell_a1 = f"${col_to_a1(ref_col_idx)}${START_ROW_INDEX + 1}"
        
        conditions = {
            "T":  (f"{current_cell_a1}={ref_cell_a1}", f"{current_cell_a1}<>{ref_cell_a1}"),
            "P":  (f"{current_cell_a1}>={ref_cell_a1}", f"{current_cell_a1}<{ref_cell_a1}"),
            "S":  (f"{current_cell_a1}<={ref_cell_a1}", f"{current_cell_a1}>{ref_cell_a1}"),
            "F":  (f"{current_cell_a1}<={ref_cell_a1}", f"{current_cell_a1}>{ref_cell_a1}"),
            "I":  (f"{current_cell_a1}<={ref_cell_a1}", f"{current_cell_a1}>{ref_cell_a1}"),
            "KI": (f"{current_cell_a1}<={ref_cell_a1}", f"{current_cell_a1}>{ref_cell_a1}")
        }
        green_cond, red_cond = conditions[col_header]

        green_formula = f"=AND(NOT(ISBLANK({current_cell_a1})), NOT(ISBLANK({ref_cell_a1})), {green_cond})"
        red_formula_comp = f"=AND(NOT(ISBLANK({current_cell_a1})), NOT(ISBLANK({ref_cell_a1})), {red_cond})"
        red_formula_no_ref = f"=AND(NOT(ISBLANK({current_cell_a1})), ISBLANK({ref_cell_a1}))"

        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": green_formula}]}, "format": {"textFormat": {"foregroundColor": COLORS["green"]}}}}, "index": 0}})
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": red_formula_comp}]}, "format": {"textFormat": {"foregroundColor": COLORS["red"]}}}}, "index": 1}})
        requests.append({"addConditionalFormatRule": {"rule": {"ranges": [rule_range], "booleanRule": {"condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": red_formula_no_ref}]}, "format": {"textFormat": {"foregroundColor": COLORS["red"]}}}}, "index": 2}})

    if requests:
        print("\n  -> LOG: Executing batch update for formatting.")
        service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={"requests": requests}).execute()

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    try:
        print(f"\n--- Processing: {sheet_name} from {csv_path} ---")
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        all_csv_rows = [row for row in reader]

    if len(all_csv_rows) < 2:
        print(f"⚠️ WARNING: CSV '{csv_path}' has fewer than 2 rows. Skipping.")
        return
        
    date_label = all_csv_rows[1][0].strip()
    csv_headers = [h.strip() for h in all_csv_rows[0][2:2 + NUM_DATA_COLS]]
    csv_data_map = {row[1].strip(): [convert_cell(c) for c in row[2:2 + NUM_DATA_COLS]] for row in all_csv_rows[1:] if len(row) > 1 and row[1].strip()}
    print(f"  -> LOG: Found date '{date_label}' with {len(csv_data_map)} modules in CSV.")

    existing_data = sheet.get_all_values()
    master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]] if len(existing_data) > START_ROW_INDEX else []
    
    new_modules = sorted([m for m in csv_data_map if m not in master_module_list])
    if new_modules:
        print(f"  -> LOG: New modules found: {', '.join(new_modules)}. Appending to sheet.")
        new_rows = [[m] for m in new_modules]
        sheet.append_rows(values=new_rows, value_input_option='USER_ENTERED', table_range=f"A{len(master_module_list) + START_ROW_INDEX + 1}")
        master_module_list.extend(new_modules)
    
    aligned_data_block = [csv_data_map.get(m, [None] * NUM_DATA_COLS) for m in master_module_list]

    sheet_headers = existing_data[0] if existing_data else []
    target_col = -1
    if date_label in sheet_headers:
        target_col = sheet_headers.index(date_label)

    if target_col == -1:
        # *** NEW LOGIC IS CALLED HERE ***
        # Before inserting new columns, freeze the formatting of the block that will be shifted.
        _freeze_formatting_for_block(service, sheet, existing_data, master_module_list)

        print(f"  -> LOG: Date '{date_label}' not in headers. Inserting new columns at index {FIRST_DATA_BLOCK_COL}.")
        target_col = FIRST_DATA_BLOCK_COL
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [{"insertDimension": {"range": {"sheetId": sheet.id, "dimension": "COLUMNS", "startIndex": FIRST_DATA_BLOCK_COL, "endIndex": FIRST_DATA_BLOCK_COL + BLOCK_WIDTH}, "inheritFromBefore": False}}]}).execute()
    else:
        print(f"  -> LOG: Date '{date_label}' found in headers. Updating columns in place at index {target_col}.")
        # If we are just updating in place, we must clear old formatting before applying new rules
        # to avoid conflicts or stale colors.
        requests = [{"updateCells": {"range": {"sheetId": sheet.id, "startRowIndex": START_ROW_INDEX, "startColumnIndex": target_col, "endColumnIndex": target_col + NUM_DATA_COLS}, "fields": "userEnteredFormat.textFormat.foregroundColor"}}]
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()


    update_body = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": f"{sheet.title}!{col_to_a1(target_col)}1", "values": [[date_label]]},
            {"range": f"{sheet.title}!{col_to_a1(target_col + 1)}2", "values": [csv_headers]},
            {"range": f"{sheet.title}!{col_to_a1(target_col)}{START_ROW_INDEX + 1}", "values": aligned_data_block}
        ]
    }
    print(f"  -> LOG: Writing data to sheet '{sheet.title}' starting at column {col_to_a1(target_col)}.")
    service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=update_body).execute()
    
    apply_formatting(service, SPREADSHEET_ID, sheet.id, target_col, len(master_module_list))
    
    print(f"✅ Sheet '{sheet_name}' updated successfully.")

def main():
    # This function is unchanged
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
        creds = GoogleCredentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        service = build("sheets", "v4", credentials=creds)
        
        gspread_creds = gspread.service_account(filename=CREDENTIALS_FILE)
        spreadsheet = gspread_creds.open_by_key(SPREADSHEET_ID)

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
