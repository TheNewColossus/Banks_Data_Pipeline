import pandas as pd
import sqlite3 as sq
import urllib.request
from datetime import datetime as dt

#Downloading the exchange rate file
urllib.request.urlretrieve("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv", "exchange_rate.csv")

#Log function for journaling the progress
def log_progress(msg):
    """
    msg: content to be logged in the code_log text file
    """
    now = dt.now().strftime("%d-%m-%Y %H:%M:%S")
    with open("code_log.txt",'a') as file:
        file.write(f"{now} :: {msg}\n")

#Extracting the market-capitalization table from the url
def extract():
    log_progress("Starting the extraction phase of the pipeline")
    #The URL from which data-table will be scraped
    url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"

    df = pd.read_html(url)
    log_progress("Extraction phase completed successfully")
    return df[1]

#Wrangling the data in the dataframe
def transform(data_frame):
    log_progress("Starting the transformation phase of the pipeline")
    #Exchange rate table
    ex_df = pd.read_csv("exchange_rate.csv")

    #Adding columns to the dataframe
    data_frame["Market cap (GBP$ billion)"] = round(data_frame["Market cap (US$ billion)"] * ex_df[ex_df.Currency == "GBP"]["Rate"].item(),2)
    data_frame["Market cap (EUR$ billion)"] = round(data_frame["Market cap (US$ billion)"] * ex_df[ex_df.Currency == "EUR"]["Rate"].item(),2)
    data_frame["Market cap (INR$ billion)"] = round(data_frame["Market cap (US$ billion)"] * ex_df[ex_df.Currency == "INR"]["Rate"].item(),2)

    #Renaming column names
    data_frame.rename(columns = {"Bank name":"Name",
        "Market cap (GBP$ billion)":"MC_GBP_Billion",
        "Market cap (US$ billion)":"MC_US_Billion",
        "Market cap (EUR$ billion)":"MC_EUR_Billion",
        "Market cap (INR$ billion)":"MC_INR_Billion"},inplace = True)
    data_frame.drop(columns = ["Rank"],inplace = True)

    log_progress("Transformation phase completed successfully")
    return data_frame

#Saving to a csv file
def load_to_csv(data_frame):
    log_progress("Saving to CSV file")
    data_frame.to_csv("Largest_banks_data.csv")
    log_progress("Saving to CSV done!")
    return

#Saving to a db
def load_to_db(data_frame):
    log_progress("Saving to a database")
    conn = sq.connect("Banks.db")
    data_frame.to_sql("Largest_banks",conn,if_exists = "replace")
    conn.commit()
    conn.close()
    log_progress("Saving to database done!")
    return

#Using a seperate function for running queries
def running_queries(database_name):
    #Selecting all records from the database
    conn = sq.connect(database_name)
    cur = conn.cursor()
    log_progress("Running Select Query")
    cur.execute("SELECT * FROM Largest_banks")
    for record in cur.fetchall():
        print(record)
    log_progress("Select Query ran successfully")
    print("\n" * 2) # For seperating the query results from the subsequent queries


    #Selecting the bank with the largest market cap in EUR from the table
    log_progress("Running Max query")
    cur = conn.cursor()
    cur.execute("SELECT Name FROM Largest_banks WHERE MC_EUR_Billion = (SELECT MAX(MC_EUR_Billion) FROM Largest_banks)")
    for record in cur.fetchall():
        print(record)
    log_progress("Max query ran successfully")
    print("\n" * 2) # For seperating the query results from the subsequent queries

    #Selecting the bank with the smallest market cap in INR from the table
    log_progress("Running Min query")
    cur = conn.cursor()
    cur.execute("SELECT Name FROM Largest_banks WHERE MC_INR_Billion = (SELECT MIN(MC_INR_Billion) FROM Largest_banks)")
    for record in cur.fetchall():
        print(record)
    log_progress("Min query ran successfully")

    conn.close()

if __name__ == "__main__":
    df = extract()
    df = transform(df)
    load_to_csv(df)
    load_to_db(df)
    running_queries("Banks.db")
