import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as GoogleCredentials
import os
import csv

# --- CONFIGURATION ---
SOURCE_DIR = "source"
SPREADSHEET_ID = "1aoT-ponIDn0hLWQ-K8hTYljk9D3P5IgK7ZgvoxE31fU"
CREDENTIALS_FILE = "creds.json"

# --- CONSTANTS ---
START_ROW_INDEX = 2
START_COL = 9
BLOCK_WIDTH = 9

def convert_cell(val):
    """
    Safely converts a string value to an int or float.
    If the value is blank or cannot be converted, it returns an empty string.
    """
    if val is None or val.strip() == '':
        return ''
    try:
        f = float(val)
        if f.is_integer():
            return int(f)
        return f
    except (ValueError, TypeError):
        return str(val).strip()

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    """
    Reads data from a given CSV file, maps it to Column A modules.
    If the date exists, it updates the corresponding columns.
    If the date doesn't exist, it inserts a new block for that date.
    """
    try:
        print(f"--- Processing: {sheet_name} from {csv_path} ---")

        # --- 1. Get or Create Worksheet ---
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found. Creating it.")
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

        # --- 2. Read and Parse CSV using the built-in csv module ---
        raw_rows = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            raw_rows = [row for row in reader if any(cell.strip() for cell in row)]

        if len(raw_rows) < 2 or len(raw_rows[0]) < 8:
            print(f"⚠️ WARNING: CSV '{csv_path}' is incomplete or empty. Skipping.")
            return

        header_row = [h.strip() for h in raw_rows[0][2:8]]
        date_label = raw_rows[1][0].strip()

        csv_data_map = {}
        for row in raw_rows[1:]:
            if len(row) < 2 or not row[1].strip():
                continue
            module_name = row[1].strip()
            data_cells = row[2:8]
            row_data = [convert_cell(cell) for cell in data_cells]
            csv_data_map[module_name] = row_data

        # --- 3. Read Existing Sheet and Map Modules ---
        existing_data = sheet.get_all_values()
        if not existing_data:
            existing_data = [[''] for _ in range(START_ROW_INDEX)]

        master_module_list = []
        if len(existing_data) > START_ROW_INDEX:
            master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]]

        # --- 4. Identify and Add New Modules ---
        new_modules_in_this_run = [m for m in csv_data_map if m not in master_module_list]
        if new_modules_in_this_run:
            print(f"  -> New modules found and added: {', '.join(new_modules_in_this_run)}")
            for module_name in new_modules_in_this_run:
                master_module_list.append(module_name)
                # Ensure the existing_data list is long enough before appending
                while len(existing_data) < START_ROW_INDEX + len(master_module_list):
                    existing_data.append([])
                existing_data[START_ROW_INDEX + master_module_list.index(module_name)].insert(0, module_name)


        # --- 5. Align CSV Data to the Master Module Order ---
        aligned_data_block = [csv_data_map.get(m, [''] * 6) for m in master_module_list]

        # --- 6. Prepare Sheet for New Data Block ---
        sheet_headers = existing_data[0] if len(existing_data) > 0 else []
        target_col = -1

        # Search for the date in the headers to determine if we are updating or inserting
        for i, header_val in enumerate(sheet_headers):
            if header_val.strip() == date_label:
                target_col = i
                break

        if target_col != -1:
            print(f"  -> Date '{date_label}' found. Updating data in place.")
        else:
            print(f"  -> Date '{date_label}' not found. Inserting new columns.")
            target_col = START_COL
            # Shift existing data to make space for the new block
            for i in range(len(existing_data)):
                while len(existing_data[i]) < START_COL:
                    existing_data[i].append("")
                row = existing_data[i]
                old_tail = row[START_COL:]
                gap = [""] * BLOCK_WIDTH
                row[START_COL:] = gap + old_tail

        # --- 7. Insert or Update the Aligned Data into the Sheet Structure ---
        max_height = len(aligned_data_block)

        # Ensure header rows are long enough
        while len(existing_data) < START_ROW_INDEX:
            existing_data.append([])
        while len(existing_data[0]) < target_col + BLOCK_WIDTH:
            existing_data[0].append("")
        while len(existing_data[1]) < target_col + len(header_row):
            existing_data[1].append("")

        existing_data[0][target_col] = date_label
        for j in range(len(header_row)):
            existing_data[1][target_col + j] = header_row[j]

        for r in range(max_height):
            sheet_row_index = START_ROW_INDEX + r
            for c in range(len(header_row)):
                while len(existing_data[sheet_row_index]) < target_col + c + 1:
                    existing_data[sheet_row_index].append("")
                existing_data[sheet_row_index][target_col + c] = aligned_data_block[r][c]

        # --- 8. Write All Changes to the Google Sheet ---
        sheet.update(range_name="A1", values=existing_data, value_input_option='USER_ENTERED')

        requests = [
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet._properties["sheetId"],
                        "startRowIndex": 0, "endRowIndex": 1,
                        "startColumnIndex": target_col, "endColumnIndex": target_col + BLOCK_WIDTH
                    }
                }
            },
            {
                "mergeCells": {
                    "range": {
                        "sheetId": sheet._properties["sheetId"],
                        "startRowIndex": 0, "endRowIndex": 1,
                        "startColumnIndex": target_col, "endColumnIndex": target_col + 6
                    },
                    "mergeType": "MERGE_ALL"
                }
            }
        ]
        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()

        print(f"✅ Sheet '{sheet_name}' updated successfully. Block processed at column {chr(target_col + 65)}.")

    except Exception as e:
        print(f"❌ ERROR processing sheet '{sheet_name}': {e}")
        raise

def main():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        
        scoped_creds = GoogleCredentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        service = build("sheets", "v4", credentials=scoped_creds)

        if not os.path.isdir(SOURCE_DIR):
            print(f"❌ ERROR: Source directory '{SOURCE_DIR}' not found.")
            return

        csv_files = [f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")]
        if not csv_files:
            print("No CSV files found in the 'source' directory. Nothing to do.")
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
