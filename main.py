import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv
from prefect import task,flow
import traceback
import sys, os
from datetime import datetime,timedelta
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
import gspread
import logging

# Self Defined scripts
from clickhouse_ops import get_data
# from slack_notification import slack_notification

env_path = '.env'
load_dotenv(dotenv_path=env_path)

# Define the name of your Google Sheet and worksheet
sheet_name = "October GM Analysis"
worksheet_name = "Base Data"

# Define the path to your service account credentials JSON file
json_path = 'core-outrider-377712-b19bf5f164b1.json'

# Set up credentials for Google Sheets API
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
credentials = ServiceAccountCredentials.from_json_keyfile_name(json_path, scopes)
@flow(name='insight_sales', retries = 3, retry_delay_seconds = 10)
def insight_sales() :
    gc = gspread.service_account(filename=json_path)

    sh = gc.open(sheet_name)

    # Get the specified worksheet
    worksheet = sh.worksheet(worksheet_name)

    # Get all values from the worksheet
    data = worksheet.get_all_values()

    # Create a Pandas DataFrame from the data
    df = pd.DataFrame(data[1:], columns=data[0])


    # Print the first few rows of the DataFrame
    selected_columns = ['report_fk', 'study_name','Grouped Study name', 'client_name', 'cost','Date_', 'rad_fk', 'rad_name', 'With Pre-Read Cost']

    # Create a new DataFrame with the selected columns
    gm_df = df[selected_columns]



    logging.basicConfig(filename="hll.log",
                            format='%(asctime)s %(message)s',
                            filemode='a+', force=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
        
    base_df = get_data('sql_query\clients.sql')
    base_df.fillna(0, inplace=True)
    # merge_columns = ['client_name']

    # Merge the DataFrames based on the 'client_name' column
    merged_df = gm_df.merge(base_df[['client_fk', 'client_name', 'unique_id', 'final onboarded date', 'type of client']], on='client_name', how='inner')
    merged_df['final onboarded date']=merged_df['final onboarded date'].astype(str)
    merged_df['Date_']=merged_df['Date_'].astype(str)
    merged_df['cost']=merged_df['cost'].astype(int)
    merged_df['With Pre-Read Cost'] = merged_df['With Pre-Read Cost'].str.replace('₹', '')
    merged_df['With Pre-Read Cost'] = merged_df['With Pre-Read Cost'].str.replace(',', '')
    merged_df['With Pre-Read Cost'] = pd.to_numeric(merged_df['With Pre-Read Cost'])

    print(merged_df.columns)


    logger.info("Fetched dataframe")
    file = gspread.authorize(credentials)

    sheet = file.open("inside sales ond growth")
    worksheet = sheet.worksheet('raw')
    try:
        worksheet.clear()
        header_row = merged_df.columns.tolist()
            # Convert DataFrame to a list of lists
        data_to_append = merged_df.values.tolist()

        worksheet.append_rows([header_row] + data_to_append)

        logger.info("Updated Google Sheet successfully")
    except Exception as e:
        logger.error("Error updating Google Sheet: %s", e)
        
        print("Sheet Updated")

if __name__ == "__main__":
    insight_sales()
