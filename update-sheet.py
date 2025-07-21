import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as GoogleCredentials
import os

# --- CONFIGURATION ---
SOURCE_DIR = "source"
SPREADSHEET_ID = "1aoT-ponIDn0hLWQ-K8hTYljk9D3P5IgK7ZgvoxE31fU"
CREDENTIALS_FILE = "creds.json"

# --- CONSTANTS ---
START_ROW_INDEX = 2
START_COL = 9
BLOCK_WIDTH = 9

def convert_cell(val):
    """Safely converts a value to an int or float if possible, otherwise returns a stripped string."""
    # This function now expects a string, not a NaN float
    if val == '':
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

        # --- 2. Read and Parse CSV Data into a Map ---
        # **FIX 1: Fill all empty cells with '' immediately to prevent NaN issues.**
        raw = pd.read_csv(csv_path, header=None).fillna('')
        if raw.shape[1] < 8 or raw.shape[0] < 2:
            print(f"⚠️ WARNING: CSV '{csv_path}' is incomplete. Skipping.")
            return

        date_label = str(raw.iloc[1, 0]).strip()
        header_row = [str(raw.iloc[0, i]).strip() for i in range(1, 8)]
        
        csv_data_map = {}
        for i in range(1, raw.shape[0]):
            module_name = str(raw.iloc[i, 1]).strip()
            # **FIX 2: Skip any rows where the module name is blank.**
            if not module_name:
                continue
            row_data = [convert_cell(raw.iloc[i, j]) for j in range(1, 8)]
            csv_data_map[module_name] = row_data

        # --- 3. Read Existing Sheet and Map Modules ---
        existing_data = sheet.get_all_values()
        if not existing_data:
            existing_data = [[''] for _ in range(START_ROW_INDEX)]

        # **FIX 3: Robustly get module list, ignoring empty rows/cells.**
        master_module_list = []
        if len(existing_data) > START_ROW_INDEX:
            master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]]

        # --- 4. Identify and Add New Modules ---
        new_modules_in_this_run = []
        for module_name in csv_data_map.keys():
            if module_name not in master_module_list:
                new_modules_in_this_run.append(module_name)
                master_module_list.append(module_name)
                existing_data.append([module_name])

        if new_modules_in_this_run:
            print(f"  -> New modules found and added: {', '.join(new_modules_in_this_run)}")
            
        # --- 5. Align CSV Data to the Master Module Order ---
        aligned_data_block = []
        for module_name in master_module_list:
            row_to_add = csv_data_map.get(module_name, [''] * 7)
            aligned_data_block.append(row_to_add)

        # --- 6. Prepare Sheet for New Data Block ---
        max_height = len(aligned_data_block)
        
        for i in range(len(existing_data)):
            while len(existing_data[i]) < START_COL:
                existing_data[i].append("")

        for i in range(len(existing_data)):
            row = existing_data[i]
            old_tail = row[START_COL:]
            gap = [""] * BLOCK_WIDTH
            row[START_COL:] = gap + old_tail
            
        # --- 7. Insert the Aligned Data into the Sheet Structure ---
        if len(existing_data[0]) < START_COL + 7:
            existing_data[0].extend([""] * (START_COL + 7 - len(existing_data[0]) + 2))
        existing_data[0][START_COL] = date_label

        for j in range(7):
            existing_data[1][START_COL + j] = header_row[j]

        for r in range(max_height):
            for c in range(7):
                while len(existing_data[r + START_ROW_INDEX]) < START_COL + c + 1:
                    existing_data[r + START_ROW_INDEX].append("")
                existing_data[r + START_ROW_INDEX][START_COL + c] = aligned_data_block[r][c]

        # --- 8. Write All Changes to the Google Sheet ---
        sheet.update(range_name="A1", values=existing_data, value_input_option='USER_ENTERED')

        requests = [{
            "mergeCells": {
                "range": {
                    "sheetId": sheet._properties["sheetId"],
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": START_COL,
                    "endColumnIndex": START_COL + 7
                },
                "mergeType": "MERGE_ALL"
            }
        }]

        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests}
        ).execute()

        print(f"✅ Sheet '{sheet_name}' updated successfully. New block mapped and inserted at column {chr(START_COL + 65)}.")

    except Exception as e:
        print(f"❌ ERROR processing sheet '{sheet_name}': {e}")


def main():
    """
    Main function to authenticate and process all CSV files in the source directory.
    """
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets"
        ]
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
        print(f"❌ CRITICAL ERROR: Credentials file '{CREDENTIALS_FILE}' not found. Make sure it's in the root directory.")
    except Exception as e:
        print(f"❌ A critical error occurred in main(): {e}")
        raise

if __name__ == "__main__":
    main()
