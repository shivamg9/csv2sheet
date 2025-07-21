import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as GoogleCredentials
import copy

# CONFIG
CSV_URL = "https://raw.githubusercontent.com/shivamg9/csv2sheet/refs/heads/main/source.csv"
SPREADSHEET_ID = "1aoT-ponIDn0hLWQ-K8hTYljk9D3P5IgK7ZgvoxE31fU"
CREDENTIALS_FILE = "creds.json"

# Constants
START_ROW_INDEX = 2  # Data starts from the 3rd row (0-indexed)
START_COL = 9        # Column J (0-indexed)
BLOCK_WIDTH = 9      # 7 data + 2 gap

def convert_cell(val):
    """Converts a value to int if possible, otherwise float, otherwise string."""
    try:
        f = float(val)
        if f.is_integer():
            return int(f)
        return f
    except (ValueError, TypeError):
        return str(val).strip()

try:
    # --- 1. Setup Google Sheets APIs ---
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1
    scoped_creds = GoogleCredentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
    service = build("sheets", "v4", credentials=scoped_creds)

    # --- 2. Read and Parse CSV Data ---
    raw = pd.read_csv(CSV_URL, header=None)
    if raw.shape[1] < 8 or raw.shape[0] < 2:
        raise ValueError("CSV block is incomplete or malformed.")

    date_label = str(raw.iloc[1, 0]).strip()
    header_row = [str(raw.iloc[0, i]).strip() for i in range(1, 8)]

    # Create a dictionary mapping module names to their data rows from the CSV
    csv_data_map = {}
    for i in range(1, raw.shape[0]):
        module_name = str(raw.iloc[i, 1]).strip()
        if pd.isna(module_name) or not module_name:
            continue
        row_data = [convert_cell(raw.iloc[i, j]) for j in range(1, 8)]
        csv_data_map[module_name] = row_data

    # --- 3. Read Existing Sheet and Map Modules ---
    existing_data = sheet.get_all_values()
    if not existing_data: # Handle case where sheet is completely empty
        existing_data = [[''] for _ in range(START_ROW_INDEX)]

    # Get the master list of modules from Column A (starting after the header rows)
    master_module_list = []
    if len(existing_data) > START_ROW_INDEX:
        master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:]]

    # --- 4. Identify and Add New Modules to the Sheet Structure ---
    new_modules_found = False
    for module_name in csv_data_map.keys():
        if module_name not in master_module_list:
            new_modules_found = True
            master_module_list.append(module_name)
            # Add a new row to our in-memory sheet data with just the module name
            existing_data.append([module_name])
            
    # --- 5. Align CSV Data to the Master Module Order ---
    aligned_data_block = []
    for module_name in master_module_list:
        # Get the module's data from the CSV map, or a list of empty strings if not found
        row_to_add = csv_data_map.get(module_name, [''] * 7)
        aligned_data_block.append(row_to_add)

    # --- 6. Prepare Sheet for New Data Block Insertion ---
    max_height = len(aligned_data_block)
    
    # Ensure all rows in memory have enough columns to avoid index errors
    for i in range(len(existing_data)):
        while len(existing_data[i]) < START_COL:
            existing_data[i].append("")

    # Shift existing content to the right to make space for the new block
    for i in range(len(existing_data)):
        row = existing_data[i]
        old_tail = row[START_COL:]
        gap = [""] * BLOCK_WIDTH
        row[START_COL:] = gap + old_tail
        
    # --- 7. Insert the Aligned Data into the Sheet Structure ---
    
    # Insert top label row (Date)
    if len(existing_data[0]) < START_COL + 7:
        existing_data[0].extend([""] * (START_COL + 7 - len(existing_data[0]) + 2))
    existing_data[0][START_COL] = date_label

    # Insert header row (T, P, S, etc.)
    for j in range(7):
        existing_data[1][START_COL + j] = header_row[j]

    # Insert the aligned data block
    for r in range(max_height):
        for c in range(7):
            # Ensure the target row has enough columns
            while len(existing_data[r + START_ROW_INDEX]) < START_COL + c + 1:
                existing_data[r + START_ROW_INDEX].append("")
            existing_data[r + START_ROW_INDEX][START_COL + c] = aligned_data_block[r][c]

    # --- 8. Write All Changes to the Google Sheet ---
    sheet.update("A1", existing_data, value_input_option='USER_ENTERED')

    # Merge the date header cell
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

    print(f"✅ New block inserted at column {chr(START_COL + 65)} and mapped to Column A successfully.")

except Exception as e:
    print(f"❌ ERROR: {e}")
    raise
