# Job Seeker Web Scraping

## Introduction


## Motivation

When I decided to make a career change in 2018, I was affraid to invest my time pursuing a new career and not get a good job as soon as possible. I was just getting to understand the role of data analysis for companies, and just started to hear about data science. 

I already knew that the city where I was living in that moment had a lot of opportunities for developers, but I couldn't have a clear picture about roles and programming languanges more necessary in the market. So, I decided to build this project to get data from job oportunities and let the data help me to make my choices.

## Prerequisities

- Python 3.7
- BeautifulSoup from bs4
- csv
- pandas
- requests
- time

## How to use


## Data Pipeline

The data pipeline starts with gathering data, using `get_job_data.py` script via a web scraping. After that, the data is filtered by city and job classification to get only job positions for Maringá city and classified as "Informática, TI, Internet e Telecomunicação" (portuguese for Informatic, TI, Internet and Telecommunication). The filtered data is saved in a spreadsheet called `job-seeker-maringa-raw-data` in Google Sheets with the worksheet name receiving the date when the scrap was done in the following format `%Y-%m-%d`.




## To Do

- Are there duplicated posts?? Some posts have the folowing:
Publicado em:
10/03/2020

Meanwhile, other posts have:

Publicado em:
03/03/2020
Última edição:
10/03/2020

I still need to figure out how to deal with the above data duplication.


## References

[Beautiful Soup: Build a Web Scraper with Python from Real Python](https://realpython.com/beautiful-soup-web-scraper-python/)

[gspread documentation](https://gspread.readthedocs.io/en/latest/index.html)

[Nice tutorial about Jupyter Notebook and Google Drive Integration](https://socraticowl.com/post/integrate-google-sheets-and-jupyter-notebooks/)