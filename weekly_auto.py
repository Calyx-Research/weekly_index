import sys
import time
import re
import pdfplumber
import pandas as pd
import requests
import urllib.parse
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import os

# =========================
# 0️⃣ Database Configuration
# =========================
load_dotenv() # This loads the variables from .env

USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DB_NAME = os.getenv("DB_NAME")

safe_password = urllib.parse.quote_plus(PASSWORD)
connection_uri = f"mysql+pymysql://{USER}:{safe_password}@{HOST}:{PORT}/{DB_NAME}"
engine = create_engine(connection_uri)

# =========================
# 1️⃣ Scraper & Parser
# =========================
def download_and_extract_report(target_date=None):
    if target_date is None:
        # Defaults to today's date in DD-MM-YYYY format
        target_date = datetime.now().strftime("%d-%m-%Y")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    url = "https://ngxgroup.com/exchange/data/market-report/"
    
    try:
        print(f"[*] Opening browser to find report for {target_date}...")
        driver.get(url)
        time.sleep(5)

        # Find link containing the date string
        xpath = f"//a[contains(., '{target_date}')]"
        download_element = driver.find_element(By.XPATH, xpath)
        pdf_url = download_element.get_attribute("href")

        print(f"[*] Found PDF Link: {pdf_url}")

        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(pdf_url, headers=headers)

        filename = f"report_{target_date}.pdf"
        with open(filename, "wb") as f:
            f.write(response.content)

        return parse_pdf(filename, target_date)

    except Exception as e:
        print(f"[!] Error during scraping: {e}")
        return None
    finally:
        driver.quit()

def parse_pdf(pdf_path, report_date):
    structured_list = []
    
    with pdfplumber.open(pdf_path) as pdf:
        # Index table is consistently on Page 3 (index 2)
        page = pdf.pages[2]
        text = page.extract_text()
        
        # Regex captures: Name, then 7 numeric values (handles commas/negatives)
        pattern = r"(.+?)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.-]+)\s+([\d,.-]+)\s+([\d,.-]+)\s+([\d,.-]+)"

        for line in text.split('\n'):
            if "INDEX" in line or "Date" in line: continue
                
            match = re.search(pattern, line)
            if match:
                groups = match.groups()
                entry = {
                    "REPORT_DATE": report_date,
                    "INDEX": groups[0].strip(),
                    "COL_1": groups[1], # Previous Week Close
                    "COL_2": groups[2], # Current Week Close
                    "WEEKLY_CHANGE": groups[3],
                    "WtD": groups[4],
                    "MtD": groups[5],
                    "QtD": groups[6],
                    "YtD": groups[7]
                }
                structured_list.append(entry)

    return pd.DataFrame(structured_list)

# =========================
# 2️⃣ Upload function
# =========================
def upload_market_index(df: pd.DataFrame):
    if df is None or df.empty:
        print("[!] No data to upload.")
        return

    # Clean numeric columns (remove commas)
    numeric_cols = ["COL_1", "COL_2", "WEEKLY_CHANGE", "WtD", "MtD", "QtD", "YtD"]
    for col in numeric_cols:
        df[col] = df[col].str.replace(',', '', regex=False)
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Final Rename to match DB schema
    df_upload = df.rename(columns={
        "COL_1": "WEEK_CLOSE_PREV",
        "COL_2": "WEEK_CLOSE_CURR"
    })

    # Ensure DATE is in SQL format (YYYY-MM-DD)
    df_upload['REPORT_DATE'] = pd.to_datetime(df_upload['REPORT_DATE'], dayfirst=True).dt.date

    try:
        with engine.begin() as conn:
            df_upload.to_sql(
                "market_index",
                con=conn,
                if_exists="append",
                index=False,
                method="multi"
            )
        print(f"Successfully uploaded {len(df_upload)} rows to 'market_index'.")
    except Exception as e:
        print(f"[!] Database Error: {e}")

# =========================
# 3️⃣ Execution
# =========================
if __name__ == "__main__":
    # Check if a date was passed as an argument: python scraper.py DD-MM-YYYY
    if len(sys.argv) > 1:
        manual_date = sys.argv[1]
        print(f"[*] Manual override: Running for date {manual_date}")
        raw_df = download_and_extract_report(manual_date)
    else:
        # Default behavior: Run for today
        raw_df = download_and_extract_report() 
    
    if raw_df is not None and not raw_df.empty:
        upload_market_index(raw_df)
    else:
        print("[!] Execution finished with no data to upload.")