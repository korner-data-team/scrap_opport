import json
import os
import random
import re
import time
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from google.cloud import bigquery


# Constants
DIV_CLASS_NAME = "d4a98186ec"
URL = "https://www.booking.com/searchresults.fr.html?label=metagha-link-LUFR-hotel-470750_dev-desktop_los-1_bw-2_dow-Friday_defdate-1_room-0_gstadt-2_rateid-public_aud-0_gacid-17490061106_mcid-10_ppa-1_clrid-0_ad-1_gstkid-0_checkin-20240628_ppt-&sid=0eb90c0c3d736f438381a1729cf346ef&aid=356931&ss=Paris&ssne=Paris&ssne_untouched=Paris&efdco=1&lang=en-gb&src=searchresults&dest_id=-1456928&dest_type=city&checkin=2024-11-06&checkout=2024-11-07&group_adults=2&no_rooms=1&group_children=0&nflt=ht_id%3D204"
PROJECT_ID = "korner-datalake"
TABLE_ID_1 = "KornerScraper_HotelsAvailability.Arrondissement"
TABLE_ID_2 = "KornerScraper_HotelsAvailability.ArrondissementSummary"

# Initialize BigQuery Client

def init_driver():
    """Create and return a headless Selenium WebDriver instance with proper options."""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")  # Bypass OS security
    chrome_options.add_argument("--disable-dev-shm-usage")  # Prevent limited resource issues
    chrome_options.add_argument("--disable-gpu")  # Fix headless GPU rendering issues
    chrome_options.add_argument("--window-size=1920,1080")  # Ensure full page rendering

    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Prevent bot detection
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])  # Hide automation flag
    chrome_options.add_experimental_option("useAutomationExtension", False)

    chrome_options.add_argument("--log-level=3")

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    return driver

def get_table_1(driver) -> pd.DataFrame:
    """Fetch hotel count for dynamically generated check-in/check-out dates."""
    current_date = datetime.now()
    results = []
    MAX_RETRIES = 3  # Maximum number of retries if hotel count is zero

    for i in range(180):  # Generate date ranges
        check_in = (current_date + timedelta(days=i)).strftime("%Y-%m-%d")
        check_out = (current_date + timedelta(days=i + 1)).strftime("%Y-%m-%d")
        hotels_count = 0

        for attempt in range(1, MAX_RETRIES + 1):
            url = f"https://www.booking.com/searchresults.fr.html?ss=Paris&checkin={check_in}&checkout={check_out}&group_adults=2&no_rooms=1&group_children=0&nflt=ht_id%3D204"
            driver.get(url)
            time.sleep(random.uniform(3, 8))  # Random delay

            try:
                search_hotels = driver.find_element(By.CLASS_NAME, DIV_CLASS_NAME).text
                hotels_count = int(re.sub(r"[^0-9]", "", search_hotels))
            except NoSuchElementException:
                print(f"[ERROR] Element not found for {check_in} - {check_out}")

            if hotels_count > 0:
                break  # Successfully retrieved a non-zero count, break retry loop
            else:
                print(f"[RETRY] Attempt {attempt} failed: 0 hotels found for {check_in} - {check_out}")
                time.sleep(2)  # Optional: extra wait before retrying

        results.append(
            {
                "ObservationDate": current_date.strftime("%Y-%m-%d"),
                "CheckInDate": check_in,
                "PropertiesCount": hotels_count,
            }
        )

    return pd.DataFrame(results)


def get_table_2(driver) -> pd.DataFrame:
    """Fetch the number of hotels listed on a given Booking.com search results page."""
    results = []
    driver.get(URL)
    current_date = datetime.now().strftime("%Y-%m-%d")
    time.sleep(random.uniform(5, 10))  # Delay to allow full page load

    try:
        search_hotels = driver.find_element(By.CLASS_NAME, DIV_CLASS_NAME).text
        hotels_count = int(re.sub(r"[^0-9]", "", search_hotels)) if search_hotels else 0
        results.append(
            {"ObservationDate": current_date, "PropertiesCount": hotels_count}
        )
    except NoSuchElementException:
        print(f"[ERROR] Element not found on {URL}")

    return pd.DataFrame(results)


def booking_properties_to_bq():
    """Fetch properties data and load it into BigQuery."""
    client = bigquery.Client()  # Google Cloud BigQuery client
    driver = init_driver()

    # Get data tables
    df_table_1 = get_table_1(driver)
    df_table_2 = get_table_2(driver)

    # # Insert data into BigQuery
    try:
        pandas_gbq.to_gbq(df_table_1, TABLE_ID_1, project_id=PROJECT_ID, if_exists="append")
        pandas_gbq.to_gbq(df_table_2, TABLE_ID_2, project_id=PROJECT_ID, if_exists="append")
        print("Data successfully uploaded to BigQuery.")
    except Exception as e:
        print(f"An error occurred while uploading to BigQuery: {e}")

    #Close the WebDriver
    driver.quit()

booking_properties_to_bq()
