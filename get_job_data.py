from bs4 import BeautifulSoup
import configparser
import csv
import gspread
import pandas as pd
import requests
from datetime import datetime
from df2gspread import df2gspread as d2g
from time import sleep
from oauth2client.service_account import ServiceAccountCredentials



config = configparser.ConfigParser()
config.read('js_config.cfg')

MY_JSON_KEYFILE_NAME=config['GSHEETS']['MY_JSON_KEYFILE_NAME']
MY_SPREADSHEET_KEY=config['GSHEETS']['MY_SPREADSHEET_KEY']
MY_SPREADSHEET_NAME=config['GSHEETS']['MY_SPREADSHEET_NAME']
MY_EMAIL=config['GSHEETS']['MY_EMAIL']



def get_job_data():
    npo_jobs = {}
    job_no = 0


    for i in range(1, 2): 
        print('Page: ', i)
        i = str(i)
        url = "http://empregos.maringa.com/?page=" + i

        response = requests.get(url)
        data = response.text
        soup = BeautifulSoup(data, 'html.parser')
        jobs = soup.find_all('div', {'class':'card-anuncio clickable-row'})

    
        for job in jobs:
            title = job.find('div',{'class':'col-12 col-sm-6 col-md-8'}).get_text(strip=True)
            date = job.find('div', {'class': 'col-12 col-sm-6 col-md-4 text-muted text-mobile'}).text
            link = job['data-href']
            job_response = requests.get(link)
            job_data = job_response.text
            job_soup = BeautifulSoup(job_data, 'html.parser')
            job_description = job_soup.find('div', {'class':'row'}).get_text(strip=True)
            
            npo_jobs[job_no] = [title, date, link, job_description]
            
            job_no+=1

        sleep(60)

    print('Total Jobs: ', job_no)

    return npo_jobs


def filter_job_data(npo_jobs):
    '''
    Transform dictionary to dataframe and filter data by city and job classification.
    Argument:
        npo_jobs: dictionary with scraped data.
    '''
    npo_jobs_df = pd.DataFrame.from_dict(npo_jobs, orient = 'index', columns = ['Job_Title', 'Date_Publication', 'Link', 'Job_Description'])
    
    df = npo_jobs_df[(npo_jobs_df['Job_Title'].str.contains('Informática, TI, Internet e Telecomunicação')) & (npo_jobs_df['Date_Publication'].str.contains('Maringá'))]

    return df 


def write_data_to_gsheets(df_to_write):
    '''
    Write dataframe to Google Sheets.
    '''
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(MY_JSON_KEYFILE_NAME, scope)
    
    gc = gspread.authorize(credentials)
    spreadsheet_key = MY_SPREADSHEET_KEY
    
    sh = gc.open(MY_SPREADSHEET_NAME)
    sh.share(MY_EMAIL, perm_type='user', role='writer')
    
    worksheet_name = str(datetime.today().strftime('%Y-%m-%d'))
    
    d2g.upload(df_to_write, spreadsheet_key, worksheet_name, credentials=credentials, row_names=True)
    
    return print('Data successfully loaded!')
    


def main():
    data_from_scrap = get_job_data()
    print('Web scraping accomplished!')
    
    df = filter_job_data(data_from_scrap)
    print('Data filtered')
    
    write_data_to_gsheets(df)


if __name__ == "__main__":
    main()