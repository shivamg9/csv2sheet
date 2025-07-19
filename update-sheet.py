import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials as GoogleCredentials
import os

# --- CONFIGURATION ---
# Directory containing the source CSV files.
SOURCE_DIR = "source"
# Your Google Spreadsheet ID.
SPREADSHEET_ID = "1aoT-ponIDn0hLWQ-K8hTYljk9D3P5IgK7ZgvoxE31fU"
# Credentials file name.
CREDENTIALS_FILE = "creds.json"

# --- CONSTANTS ---
START_COL = 9  # Column J (0-indexed) where the new block will be inserted.
BLOCK_WIDTH = 9  # Width of the data block (7 columns) plus a 2-column gap.

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
    Reads data from a given CSV file and inserts it as a new block on the left side
    of the specified worksheet.
    """
    try:
        print(f"--- Processing: {sheet_name} from {csv_path} ---")

        # Get the specific worksheet by its name. If it doesn't exist, create it.
        try:
            sheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Worksheet '{sheet_name}' not found. Creating it.")
            sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

        # Read the CSV data.
        raw = pd.read_csv(csv_path, header=None)
        if raw.shape[1] < 8 or raw.shape[0] < 2:
            print(f"⚠️ WARNING: CSV '{csv_path}' block is incomplete or malformed. Skipping.")
            return

        # Parse the date label, headers, and data rows from the CSV.
        date_label = str(raw.iloc[1, 0]).strip()
        header_row = [str(raw.iloc[0, i]).strip() for i in range(1, 8)]
        
        data_rows = []
        for i in range(1, raw.shape[0]):
            if pd.isna(raw.iloc[i, 0]):
                break
            row = [convert_cell(raw.iloc[i, j]) for j in range(1, 8)]
            data_rows.append(row)

        max_height = len(data_rows)
        
        # Read the entire existing content from the sheet.
        existing_data = sheet.get_all_values()

        # Ensure the sheet has enough rows and columns to work with.
        while len(existing_data) < max_height + 2:
            existing_data.append([])
        for i in range(len(existing_data)):
            while len(existing_data[i]) < START_COL:
                existing_data[i].append("")

        # Shift existing content to the right to make space for the new block.
        for i in range(len(existing_data)):
            row = existing_data[i]
            old_tail = row[START_COL:]
            gap = [""] * BLOCK_WIDTH
            row[START_COL:] = gap + old_tail

        # Insert the new content: top label, header, and data.
        if len(existing_data[0]) < START_COL + 7:
            existing_data[0] += [""] * (START_COL + 7 - len(existing_data[0]) + 2)
        existing_data[0][START_COL] = date_label

        for j in range(7):
            existing_data[1][START_COL + j] = header_row[j]

        for r in range(max_height):
            for c in range(7):
                existing_data[r + 2][START_COL + c] = data_rows[r][c]

        # Perform a single batch update to write all data back to the sheet.
        sheet.update("A1", existing_data)

        # Create a request to merge the cells for the date header.
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

        # Execute the merge request.
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests}
        ).execute()

        print(f"✅ Sheet '{sheet_name}' updated successfully. New block inserted at column {chr(START_COL + 65)}.")

    except Exception as e:
        print(f"❌ ERROR processing sheet '{sheet_name}': {e}")
        # Optionally re-raise if you want the whole script to fail on a single error
        # raise

def main():
    """
    Main function to authenticate and process all CSV files in the source directory.
    """
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

        # Check if the source directory exists.
        if not os.path.isdir(SOURCE_DIR):
            print(f"❌ ERROR: Source directory '{SOURCE_DIR}' not found.")
            return

        # Loop through all files in the source directory.
        for filename in os.listdir(SOURCE_DIR):
            if filename.endswith(".csv"):
                # Derive the sheet name from the CSV filename (e.g., "dev-int.csv" -> "dev-int").
                sheet_name = os.path.splitext(filename)[0]
                csv_path = os.path.join(SOURCE_DIR, filename)
                # Call the update function for each CSV.
                update_sheet(service, spreadsheet, sheet_name, csv_path)

    except Exception as e:
        print(f"❌ A critical error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
