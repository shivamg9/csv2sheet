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
START_COL = 9    # Column J (API is 0-indexed, gspread is 1-indexed)
BLOCK_WIDTH = 9  # 7 data columns + 2 gap columns

def convert_cell(val):
    """Safely converts a value to an int or float if possible, otherwise returns a stripped string."""
    try:
        f = float(val)
        if f.is_integer():
            return int(f)
        return f
    except (ValueError, TypeError):
        return str(val).strip()

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    """
    Inserts columns and writes data from a CSV to the specified worksheet,
    preserving existing merged cells and formatting.
    """
    try:
        print(f"--- Processing: {sheet_name} from {csv_path} ---")

        # Get the worksheet by name. If it doesn't exist, create it.
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found. Creating it.")
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="80")

        # Read the CSV data.
        raw = pd.read_csv(csv_path, header=None)
        if raw.shape[1] < 8 or raw.shape[0] < 2:
            print(f"⚠️ WARNING: CSV '{csv_path}' is incomplete or malformed. Skipping.")
            return

        # Parse the date label, headers, and data rows.
        date_label = str(raw.iloc[1, 0]).strip()
        header_row = [str(raw.iloc[0, i]).strip() for i in range(1, 8)]
        
        data_rows = []
        for i in range(1, raw.shape[0]):
            if pd.isna(raw.iloc[i, 0]):
                break
            row = [convert_cell(raw.iloc[i, j]) for j in range(1, 8)]
            data_rows.append(row)

        # STEP 1: Insert new columns using the API.
        # This safely shifts all existing content (including merged cells) to the right.
        insert_cols_request = {
            "insertDimension": {
                "range": {
                    "sheetId": sheet._properties["sheetId"],
                    "dimension": "COLUMNS",
                    "startIndex": START_COL,
                    "endIndex": START_COL + BLOCK_WIDTH
                },
                "inheritFromBefore": False
            }
        }
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [insert_cols_request]}
        ).execute()
        print(f"Inserted {BLOCK_WIDTH} new columns at column {chr(START_COL + 65)}.")

        # STEP 2: Prepare data for a batch update into the new empty space.
        cells_to_update = []
        
        # Add date label (gspread uses 1-based indexing for rows/cols)
        cells_to_update.append(gspread.Cell(row=1, col=START_COL + 1, value=date_label))
        
        # Add header row
        for i, header in enumerate(header_row):
            cells_to_update.append(gspread.Cell(row=2, col=START_COL + 1 + i, value=header))
            
        # Add data rows
        for r, data_row in enumerate(data_rows):
            for c, cell_value in enumerate(data_row):
                cells_to_update.append(gspread.Cell(row=3 + r, col=START_COL + 1 + c, value=cell_value))

        # Perform the batch update to write the new data.
        sheet.update_cells(cells_to_update, value_input_option='USER_ENTERED')
        
        # STEP 3: Merge the header cell now that the data is written.
        # This is safe because we're merging blank, unformatted cells.
        merge_header_request = {
            "mergeCells": {
                "range": {
                    "sheetId": sheet._properties["sheetId"],
                    "startRowIndex": 0,          # API is 0-indexed: Row 1
                    "endRowIndex": 1,            # Merges a single row
                    "startColumnIndex": START_COL, # Column J
                    "endColumnIndex": START_COL + 7  # Up to Column P (7 columns wide)
                },
                "mergeType": "MERGE_ALL"
            }
        }
        
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [merge_header_request]}
        ).execute()

        print(f"✅ Sheet '{sheet_name}' updated successfully.")

    except Exception as e:
        print(f"❌ ERROR processing sheet '{sheet_name}': {e}")

def main():
    """Authenticates and processes all CSV files in the source directory."""
    try:
        # Setup Google Sheets APIs
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

        # Loop through all files in the source directory.
        for filename in sorted(os.listdir(SOURCE_DIR)): # sorted() for predictable order
            if filename.endswith(".csv"):
                sheet_name = os.path.splitext(filename)[0]
                csv_path = os.path.join(SOURCE_DIR, filename)
                update_sheet(service, spreadsheet, sheet_name, csv_path)

    except Exception as e:
        print(f"❌ A critical error occurred in main(): {e}")
        raise

if __name__ == "__main__":
    main()
