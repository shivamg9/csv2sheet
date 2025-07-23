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

def parse_multi_date_csv(csv_path):
    """
    Parse CSV files that may have multiple date blocks in a single row structure.
    Returns a dictionary with dates as keys and module data as values.
    """
    date_blocks = {}
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        raw_rows = [row for row in reader if any(cell.strip() for cell in row if cell)]
    
    if not raw_rows:
        return date_blocks
    
    header_row = raw_rows[0]
    
    # Find all date column positions
    date_positions = []
    for i, cell in enumerate(header_row):
        if cell.strip().lower() == 'date':
            date_positions.append(i)
    
    # If no date columns found, treat as single date format
    if not date_positions:
        return parse_single_date_csv(csv_path)
    
    # Process each data row
    for row_idx in range(1, len(raw_rows)):
        row = raw_rows[row_idx]
        
        # For each date position, extract the data block
        for date_pos in date_positions:
            if date_pos >= len(row):
                continue
                
            date_val = row[date_pos].strip() if date_pos < len(row) else ''
            if not date_val:
                continue
                
            module_pos = date_pos + 1
            data_start_pos = date_pos + 2
            data_end_pos = data_start_pos + 6  # T,P,S,F,I,KI
            
            if module_pos >= len(row):
                continue
                
            module_name = row[module_pos].strip() if module_pos < len(row) else ''
            if not module_name:
                continue
            
            # Extract the 6 data columns (T,P,S,F,I,KI)
            data_cells = []
            for pos in range(data_start_pos, min(data_end_pos, len(row))):
                data_cells.append(row[pos] if pos < len(row) else '')
            
            # Pad with empty strings if needed
            while len(data_cells) < 6:
                data_cells.append('')
            
            row_data = [convert_cell(cell) for cell in data_cells]
            
            if date_val not in date_blocks:
                date_blocks[date_val] = {}
            
            date_blocks[date_val][module_name] = row_data
    
    return date_blocks

def parse_single_date_csv(csv_path):
    """
    Parse CSV files with single date format.
    Returns a dictionary with a single date key.
    """
    raw_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        raw_rows = [row for row in reader if any(cell.strip() for cell in row if cell)]

    if len(raw_rows) < 2 or len(raw_rows[0]) < 8:
        return {}

    # Get date from first data row
    date_label = raw_rows[1][0].strip() if len(raw_rows[1]) > 0 else ''
    if not date_label:
        return {}

    csv_data_map = {}
    for row in raw_rows[1:]:
        if len(row) < 2 or not row[1].strip():
            continue

        module_name = row[1].strip()
        data_cells = row[2:8] if len(row) >= 8 else row[2:] + [''] * (8 - len(row))
        row_data = [convert_cell(cell) for cell in data_cells[:6]]
        csv_data_map[module_name] = row_data

    return {date_label: csv_data_map}

def update_sheet_with_multiple_dates(service, spreadsheet, sheet_name, date_blocks):
    """
    Update sheet with multiple date blocks.
    """
    try:
        print(f"--- Processing: {sheet_name} with {len(date_blocks)} date blocks ---")

        # --- 1. Get or Create Worksheet ---
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found. Creating it.")
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

        # --- 2. Read Existing Sheet and Map Modules ---
        existing_data = sheet.get_all_values()
        if not existing_data:
            existing_data = [[''] for _ in range(START_ROW_INDEX)]

        master_module_list = []
        if len(existing_data) > START_ROW_INDEX:
            master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and len(row) > 0 and row[0]]

        # --- 3. Process each date block (most recent first) ---
        sorted_dates = sorted(date_blocks.keys(), reverse=True)
        
        for date_label in sorted_dates:
            csv_data_map = date_blocks[date_label]
            
            print(f"  -> Processing date block: {date_label}")
            
            # --- 4. Identify and Add New Modules ---
            new_modules_in_this_run = [m for m in csv_data_map if m not in master_module_list]
            if new_modules_in_this_run:
                print(f"    -> New modules found and added: {', '.join(new_modules_in_this_run)}")
                for module_name in new_modules_in_this_run:
                    master_module_list.append(module_name)
                    # Ensure existing_data has enough rows
                    while len(existing_data) <= START_ROW_INDEX + len(master_module_list) - 1:
                        existing_data.append([''])
                    existing_data[START_ROW_INDEX + len(master_module_list) - 1] = [module_name]

            # --- 5. Align CSV Data to the Master Module Order ---
            aligned_data_block = [csv_data_map.get(m, [''] * 6) for m in master_module_list]

            # --- 6. Prepare Sheet for New Data Block ---
            max_height = len(aligned_data_block)
            
            # Ensure all rows have enough columns
            min_cols_needed = START_COL + BLOCK_WIDTH
            for i in range(len(existing_data)):
                while len(existing_data[i]) < min_cols_needed:
                    existing_data[i].append("")
            
            # Ensure we have enough rows for all modules
            while len(existing_data) < START_ROW_INDEX + max_height:
                existing_data.append([''] * min_cols_needed)

            # Shift existing data to make room for new block
            for i in range(len(existing_data)):
                if len(existing_data[i]) > START_COL:
                    row = existing_data[i]
                    old_tail = row[START_COL:]
                    gap = [""] * BLOCK_WIDTH
                    row[START_COL:] = gap + old_tail
                    
            # --- 7. Insert the Aligned Data into the Sheet Structure ---
            # Ensure header rows exist and have enough columns
            while len(existing_data) < 2:
                existing_data.append([''] * min_cols_needed)
            
            for i in range(2):
                while len(existing_data[i]) < START_COL + 6:
                    existing_data[i].append("")

            existing_data[0][START_COL] = date_label
            
            # Add column headers
            header_labels = ['T', 'P', 'S', 'F', 'I', 'KI']
            for j in range(6):
                existing_data[1][START_COL + j] = header_labels[j]

            # Insert data
            for r in range(max_height):
                row_index = r + START_ROW_INDEX
                if row_index >= len(existing_data):
                    existing_data.append([''] * min_cols_needed)
                
                for c in range(6):
                    col_index = START_COL + c
                    while len(existing_data[row_index]) <= col_index:
                        existing_data[row_index].append("")
                    existing_data[row_index][col_index] = aligned_data_block[r][c]

        # --- 8. Write All Changes to the Google Sheet ---
        sheet.update(range_name="A1", values=existing_data, value_input_option='USER_ENTERED')

        # --- 9. Merge cells for the most recent date header ---
        if sorted_dates:
            latest_date = sorted_dates[0]
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
                            "endColumnIndex": START_COL + 6
                        },
                        "mergeType": "MERGE_ALL"
                    }
                }
            ]

            service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()

        print(f"✅ Sheet '{sheet_name}' updated successfully with {len(date_blocks)} date blocks.")

    except Exception as e:
        print(f"❌ ERROR processing sheet '{sheet_name}': {e}")
        import traceback
        traceback.print_exc()
        raise

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    """
    Main function to update a sheet from a CSV file.
    Handles both single-date and multi-date formats.
    """
    try:
        # Parse the CSV file
        date_blocks = parse_multi_date_csv(csv_path)
        
        if not date_blocks:
            print(f"⚠️ WARNING: No valid data found in '{csv_path}'. Skipping.")
            return
        
        # Update the sheet with the parsed data
        update_sheet_with_multiple_dates(service, spreadsheet, sheet_name, date_blocks)
        
    except Exception as e:
        print(f"❌ ERROR processing sheet '{sheet_name}': {e}")
        import traceback
        traceback.print_exc()
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
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
