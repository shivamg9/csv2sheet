import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as GoogleCredentials
import os
import csv # Using the built-in CSV module

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
    Reads data from a given CSV file, maps it to Column A modules, and inserts it
    as a new block into the specified worksheet.
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
            # Filter out any completely empty rows
            raw_rows = [row for row in reader if any(cell.strip() for cell in row if cell)]

        if len(raw_rows) < 2:
            print(f"⚠️ WARNING: CSV '{csv_path}' has insufficient data. Skipping.")
            return

        # Find the first row with valid data and extract date from it
        date_label = None
        first_data_row_idx = None
        
        for i in range(1, len(raw_rows)):
            row = raw_rows[i]
            if len(row) > 0 and row[0].strip():
                date_label = row[0].strip()
                first_data_row_idx = i
                break
        
        if not date_label or first_data_row_idx is None:
            print(f"⚠️ WARNING: No valid date found in CSV '{csv_path}'. Skipping.")
            return

        # Extract header from first row, ensuring we have at least 6 columns for T,P,S,F,I,KI
        header_row = []
        if len(raw_rows[0]) >= 8:
            header_row = [h.strip() for h in raw_rows[0][2:8]]
        else:
            # If header is incomplete, use default labels
            header_row = ['T', 'P', 'S', 'F', 'I', 'KI']
        
        # Ensure we have exactly 6 header labels
        while len(header_row) < 6:
            header_row.append('')

        csv_data_map = {}
        
        # Process all data rows, handling multi-column format
        for row in raw_rows[1:]:
            if not row:
                continue
                
            # Handle multi-column format - look for data in different positions
            # First column set (columns 0-7)
            if len(row) > 1 and row[1].strip():
                module_name = row[1].strip()
                data_cells = row[2:8] if len(row) >= 8 else row[2:] + [''] * (8 - len(row))
                row_data = [convert_cell(cell) for cell in data_cells[:6]]
                csv_data_map[module_name] = row_data
            
            # Second column set (columns 10-17) - if exists
            if len(row) > 11 and row[11].strip():
                module_name = row[11].strip()
                data_cells = row[12:18] if len(row) >= 18 else row[12:] + [''] * (18 - len(row))
                row_data = [convert_cell(cell) for cell in data_cells[:6]]
                csv_data_map[module_name] = row_data
            
            # Third column set (columns 20-27) - if exists
            if len(row) > 21 and row[21].strip():
                module_name = row[21].strip()
                data_cells = row[22:28] if len(row) >= 28 else row[22:] + [''] * (28 - len(row))
                row_data = [convert_cell(cell) for cell in data_cells[:6]]
                csv_data_map[module_name] = row_data
            
            # Fourth column set (columns 30-37) - if exists
            if len(row) > 31 and row[31].strip():
                module_name = row[31].strip()
                data_cells = row[32:38] if len(row) >= 38 else row[32:] + [''] * (38 - len(row))
                row_data = [convert_cell(cell) for cell in data_cells[:6]]
                csv_data_map[module_name] = row_data

        if not csv_data_map:
            print(f"⚠️ WARNING: No valid module data found in CSV '{csv_path}'. Skipping.")
            return

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
                existing_data.append([module_name])

        # --- 5. Align CSV Data to the Master Module Order ---
        aligned_data_block = [csv_data_map.get(m, [''] * 6) for m in master_module_list]

        # --- 6. Prepare Sheet for New Data Block ---
        max_height = len(aligned_data_block)
        
        # Ensure existing_data has enough rows
        while len(existing_data) < START_ROW_INDEX + max_height:
            existing_data.append([''])
        
        for i in range(len(existing_data)):
            while len(existing_data[i]) < START_COL:
                existing_data[i].append("")

        for i in range(len(existing_data)):
            row = existing_data[i]
            old_tail = row[START_COL:] if len(row) > START_COL else []
            gap = [""] * BLOCK_WIDTH
            row[START_COL:] = gap + old_tail
            
        # --- 7. Insert the Aligned Data into the Sheet Structure ---
        if len(existing_data[0]) < START_COL + 6:
            existing_data[0].extend([""] * (START_COL + 6 - len(existing_data[0]) + 2))
        existing_data[0][START_COL] = date_label

        for j in range(6):
            if len(existing_data[1]) <= START_COL + j:
                existing_data[1].extend([""] * (START_COL + j - len(existing_data[1]) + 1))
            existing_data[1][START_COL + j] = header_row[j]

        for r in range(max_height):
            for c in range(6):
                row_idx = r + START_ROW_INDEX
                col_idx = START_COL + c
                while len(existing_data[row_idx]) <= col_idx:
                    existing_data[row_idx].append("")
                existing_data[row_idx][col_idx] = aligned_data_block[r][c]

        # --- 8. Write All Changes to the Google Sheet ---
        sheet.update(range_name="A1", values=existing_data, value_input_option='USER_ENTERED')

        requests = [
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet._properties["sheetId"],
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": START_COL,
                        "endColumnIndex": START_COL + BLOCK_WIDTH
                    }
                }
            },
            {
                "mergeCells": {
                    "range": {
                        "sheetId": sheet._properties["sheetId"],
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": START_COL,
                        "endColumnIndex": START_COL + 6 # 6 columns for the new data block
                    },
                    "mergeType": "MERGE_ALL"
                }
            }
        ]

        service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()

        print(f"✅ Sheet '{sheet_name}' updated successfully. New block mapped and inserted at column {chr(START_COL + 65)}.")

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
