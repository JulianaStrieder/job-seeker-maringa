import configparser
import gspread
import pandas as pd
import re
import warnings
from datetime import datetime
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials



config = configparser.ConfigParser()
config.read('js_config.cfg')


MY_JSON_KEYFILE_NAME=config['GSHEETS']['MY_JSON_KEYFILE_NAME']
MY_SPREADSHEET_KEY=config['GSHEETS']['MY_SPREADSHEET_KEY']
CLEAN_DATA_SPREADSHEET_KEY=config['GSHEETS']['CLEAN_DATA_SPREADSHEET_KEY']
MY_EMAIL_ADDRESS=config['GSHEETS']['MY_EMAIL_ADDRESS']

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(MY_JSON_KEYFILE_NAME, scope)
gc = gspread.authorize(credentials)


def open_gsheets_and_get_data():
    '''
    Get raw data from Google Sheets and return a table.
    '''
    
    spreadsheet_key = MY_SPREADSHEET_KEY

    book = gc.open_by_key(spreadsheet_key)
    worksheet_name = str(datetime.today().strftime('%Y-%m-%d'))
    worksheet = book.worksheet(worksheet_name)

    table_from_worksheet = worksheet.get_all_values()

    return table_from_worksheet


def my_data_wrangling(table_from_worksheet):
    '''
    Transform a table with data from Google Sheets to a Pandas dataframe.
    Extract text data and save it as df columns.
    Select only important columns to be saved as cleaned data.
    Add a column with the date when was scraped.
    '''
    warnings.filterwarnings("ignore")

    df = pd.DataFrame(table_from_worksheet[1:], columns=table_from_worksheet[0])
    df['Job_Description_Splitted'] = df['Job_Description'].apply(lambda x: x.split('Descrição:')[1])
    df['Job_Description_Only'] = df['Job_Description_Splitted'].apply(lambda x: x.split('Requisitos:')[0])
    df['Job_Description_Splitted_1'] = df['Job_Description_Splitted'].apply(lambda x: x.split('Requisitos:')[1])
    df['Job_Requirements_Only'] = df['Job_Description_Splitted_1'].apply(lambda x: x.split('Benefícios')[0])
    df['Job_Title_Only'] = df['Job_Title'].apply(lambda x: x.split('Informática, TI, Internet e Telecomunicação')[0])
    df['Work_Experience'] = df['Job_Description'].str.contains('Experiência:Sim', flags=re.IGNORECASE)
    df['Internship'] = df['Job_Description'].str.contains('Estágio', flags=re.IGNORECASE)
    df['Date_Publication_Cleaned'] = df['Date_Publication'].str.extract('(../../....)')
    df['Date_Publication_Cleaned'] =  pd.to_datetime(df['Date_Publication_Cleaned'], format='%d/%m/%Y')
    df_cleaned = df[['Job_Description_Only', 'Job_Requirements_Only', 'Job_Title_Only', 'Date_Publication_Cleaned', 'Work_Experience', 'Internship']]
    df_cleaned['Scrap_Date'] = datetime.today().strftime('%Y-%m-%d')

    return df_cleaned



def write_cleaned_data_to_worksheet(df):
    '''
    Write clean data to a new Google Sheets (cleansed data layer)
    '''
    sh = gc.open('job-seeker-maringa-cleaned-data')
    
    clean_data_worksheet_name = str(datetime.today().strftime('%Y-%m-%d') + '_cleaned')

    sh.share(MY_EMAIL_ADDRESS, perm_type='user', role='writer')

    d2g.upload(df, CLEAN_DATA_SPREADSHEET_KEY, clean_data_worksheet_name, credentials=credentials, row_names=False)


def write_new_data_to_historical_gs(df):
    '''
    Add new data to a Google Sheets with historical data.
    '''
    book = gc.open_by_key(CLEAN_DATA_SPREADSHEET_KEY)

    worksheet = book.worksheet('All_Together')

    table = worksheet.get_all_values()

    df_historical = pd.DataFrame(table[1:], columns=table[0])

    begin_upload_in_row = 'A' + str(df_historical.shape[0] + 2)

    d2g.upload(df, CLEAN_DATA_SPREADSHEET_KEY, 'All_Together', credentials=credentials, clean=False, row_names=False, col_names=False, start_cell=begin_upload_in_row)


def main():
    
    table_from_worksheet = open_gsheets_and_get_data()
    
    df = my_data_wrangling(table_from_worksheet)
    
    write_cleaned_data_to_worksheet(df)

    write_new_data_to_historical_gs(df)

    return print('Data Wrangling 1 completed!')




if __name__ == "__main__":
    main()
