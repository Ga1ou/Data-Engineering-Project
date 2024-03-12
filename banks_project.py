# Code for ETL operations on Country-GDP data

# Importing the required libraries
import sqlite3
import datetime
import requests
from bs4 import BeautifulSoup as BeautifulSoup
import pandas as pd
import numpy as np
#from io import StringIO

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open('code_log.txt', 'a') as file:
        file.write(f"{time_stamp} : {message}\n")

# To demonstrate the logging, let's call log_progress with a sample message
log_progress("Sample log message.")
# We'll check the content of the file to confirm that the message is logged correctly.
with open('code_log.txt', 'r') as file:
    log_contents = file.read()
    
#print(log_contents)

#2
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', table_attribs)
    
    html_content = str(table)
    df = pd.read_html(StringIO(html_content))[0]
    return df

    
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = {"class": "wikitable"}  # Example attribute, this may not be the correct one.

# Function call for demonstration purposes
try:
    df = extract(url, table_attribs)
    print(df)
    log_progress("Data extraction completed successfully.")
except Exception as e:
    log_progress(f"Data extraction failed with error: {e}")


url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

df_1 = extract(url, table_attribs)

#3
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    exchange_rate_df = pd.read_csv(csv_path)
    # Convert the DataFrame into a dictionary
    df = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    # Calculate and add the new columns for market capitalization in different currencies
    df['MC_USD_Billion'] = np.ones(len(df))
    df['MC_GBP_Billion'] = [np.round(x* df['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * df['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * df['INR'], 2) for x in df['MC_USD_Billion']]

    df = pd.DataFrame(df)
    return df  



    

csv_path = '/home/project/exchange_rate.csv'
df_transformed = transform(df_1, csv_path)
print("Transformation data: \n", df_transformed)






def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    try:
        df.to_csv(output_path, index=False)
        log_progress(f"Data successfully saved to {output_path}")
    except Exception as e:
        log_progress(f"Failed to save data to {output_path}: {e}")

output_path = '/home/project/output.csv'
#load_to_csv(df_transformed, output_path)
print('Finished output')





#5
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    try:
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
        sql_connection.commit()
        log_progress(f"Data successfully loaded into database table {table_name}")
    except Exception as e:
        log_progress(f"Failed to load data into database table {table_name}: {e}")
        raise

conn = sqlite3.connect('Banks.db')


table_name = 'Largest_banks'
load_to_db(df_transformed, conn, table_name)






def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''



    
    
    query_output = pd.read_sql(query_statement, conn)
    print(query_statement)
    print(query_output)


query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, conn)