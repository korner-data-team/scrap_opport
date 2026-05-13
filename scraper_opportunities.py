"""
Booking.com Paris hotel availability scraper.

Uses undetected-chromedriver to bypass AWS WAF, and a robust multi-strategy
parser that survives Booking.com changing their wording (which they do every
few months).

The parser tries 4 strategies in order:
  1. <h1> aria-label
  2. <h1> visible text
  3. <title> tag
  4. Scan of headings/divs/spans

If Booking changes ONE thing, the others still work.
"""

import random
import re
import time
import subprocess
import pandas as pd
import pandas_gbq
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import bigquery
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


# ---- BigQuery config ----
PROJECT_ID = "korner-datalake"
TABLE_ID_1 = "KornerScraper_HotelsAvailability.Arrondissement"
TABLE_ID_2 = "KornerScraper_HotelsAvailability.ArrondissementSummary"

# ---- Scraper config ----
BASE_URL = "https://www.booking.com/searchresults.fr.html"
DAYS_TO_SCRAPE = 180
MAX_RETRIES = 3


# ---- Robust parser ----
# Words signalling "this is a hotel count" — add to this list if you see new ones.
COUNT_NOUNS = [
    "établissements?",   # "1 530 établissements trouvés"
    "hébergements?",     # "Nous avons trouvé 1 566 hébergements"
    "hôtels?",
    "logements?",
    "propriétés?",
    "places?",
    "properties",
    "hotels",
    "stays",
    "accommodations?",
]
COUNT_NOUNS_RE = "|".join(COUNT_NOUNS)

# Verbs signalling "X was found"
COUNT_VERBS = ["trouvé", "trouve", "trouvés", "trouvées", "found"]
COUNT_VERBS_RE = "|".join(COUNT_VERBS)

# A number with optional French/English thousands separators (space, nbsp, comma, dot).
NUMBER_RE = r"(\d[\d\s\xa0,\.]{0,8}\d|\d)"


def _parse_int(raw: str):
    """Convert '1 566' / '1,566' / '1.566' into 1566. Return None if invalid."""
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return None
    n = int(digits)
    # Sanity bounds for Paris hotel counts (with ht_id=204 filter).
    if 10 <= n <= 100000:
        return n
    return None


def _search_patterns(text: str):
    """Try several regex patterns; return the first valid count, else None."""
    if not text:
        return None

    patterns = [
        # "trouvé 1 566 hébergements" (verb + number + noun) — most specific
        rf"(?:{COUNT_VERBS_RE})\s+{NUMBER_RE}\s+(?:{COUNT_NOUNS_RE})",
        # "1 566 hébergements trouvés" (number + noun + verb)
        rf"{NUMBER_RE}\s+(?:{COUNT_NOUNS_RE})\s+(?:{COUNT_VERBS_RE})",
        # "1 530 établissements" (number + noun, no verb)
        rf"{NUMBER_RE}\s+(?:{COUNT_NOUNS_RE})",
        # "trouvé 1 566" (verb + number, no noun)
        rf"(?:{COUNT_VERBS_RE})\s+{NUMBER_RE}",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            count = _parse_int(match.group(1))
            if count is not None:
                return count
    return None


def extract_hotel_count(html: str) -> int:
    """Extract hotel count from a Booking.com search results page."""
    soup = BeautifulSoup(html, "html.parser")

    # Strategy 1: <h1> aria-label (most stable)
    for h1 in soup.find_all("h1", attrs={"aria-label": True}):
        count = _search_patterns(h1["aria-label"])
        if count is not None:
            return count

    # Strategy 2: <h1> visible text
    for h1 in soup.find_all("h1"):
        count = _search_patterns(h1.get_text())
        if count is not None:
            return count

    # Strategy 3: <title>
    title = soup.find("title")
    if title:
        count = _search_patterns(title.get_text())
        if count is not None:
            return count

    # Strategy 4: scan headings, divs, and spans for matching text
    for tag in soup.find_all(["h2", "h3", "div", "span"], limit=200):
        text = tag.get_text(strip=True)
        if 5 < len(text) < 200:
            count = _search_patterns(text)
            if count is not None:
                return count

    return 0


# ---- Browser & scraping ----
def init_driver():
    """Create and return a stealth Chrome WebDriver instance."""
    options = uc.ChromeOptions()
    # options.add_argument("--headless=new")  # disabled — Booking detects this
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    try:
        chrome_version = subprocess.check_output(
            ["google-chrome", "--version"], text=True
        )
        major_version = int(chrome_version.split()[-1].split(".")[0])
        return uc.Chrome(options=options, version_main=major_version)
    except Exception:
        return uc.Chrome(options=options)


def build_url(check_in: str, check_out: str) -> str:
    return (
        f"{BASE_URL}?ss=Paris&checkin={check_in}&checkout={check_out}"
        f"&group_adults=2&no_rooms=1&group_children=0&nflt=ht_id%3D204"
    )


def fetch_hotel_count(driver, url: str) -> int:
    """Load a search URL with retries and parse the hotel count."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            # Wait until the h1 with the count appears (max 10s).
            # On a normal load this returns in ~1-2s; only WAF challenges take longer.
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )
                # Tiny extra wait for aria-label to populate
                time.sleep(0.5)
            except Exception:
                pass  # let extract_hotel_count handle it
            
            count = extract_hotel_count(driver.page_source)
            if count > 0:
                return count
            print(f"  [RETRY {attempt}] parsed 0 hotels")
        except Exception as e:
            print(f"  [RETRY {attempt}] error: {e}")
        time.sleep(2 + attempt)
    return 0


def get_table_1(driver) -> pd.DataFrame:
    """Fetch hotel count for the next 180 days of one-night stays in Paris."""
    current_date = datetime.now()
    results = []

    for i in range(DAYS_TO_SCRAPE):
        check_in = (current_date + timedelta(days=i)).strftime("%Y-%m-%d")
        check_out = (current_date + timedelta(days=i + 1)).strftime("%Y-%m-%d")
        url = build_url(check_in, check_out)

        hotels_count = fetch_hotel_count(driver, url)
        print(f"[OK] {check_in} → {hotels_count} hotels")

        results.append({
            "ObservationDate": current_date.strftime("%Y-%m-%d"),
            "CheckInDate": check_in,
            "PropertiesCount": hotels_count,
        })

        time.sleep(random.uniform(0.5, 1.5))

    return pd.DataFrame(results)


def get_table_2(driver) -> pd.DataFrame:
    """Fetch the summary hotel count for the original fixed URL."""
    fixed_url = (
        "https://www.booking.com/searchresults.fr.html"
        "?ss=Paris&dest_id=-1456928&dest_type=city"
        "&group_adults=2&no_rooms=1&group_children=0"
        "&nflt=ht_id%3D204"
    )

    hotels_count = fetch_hotel_count(driver, fixed_url)
    current_date = datetime.now().strftime("%Y-%m-%d")

    return pd.DataFrame(
        [{"ObservationDate": current_date, "PropertiesCount": hotels_count}]
    )

def booking_properties_to_bq():
    """Fetch properties data and load it into BigQuery."""
    driver = init_driver()
 
    try:
        df_table_1 = get_table_1(driver)
        df_table_2 = get_table_2(driver)
 
        try:
            pandas_gbq.to_gbq(
                df_table_1, TABLE_ID_1, project_id=PROJECT_ID, if_exists="append"
            )
            pandas_gbq.to_gbq(
                df_table_2, TABLE_ID_2, project_id=PROJECT_ID, if_exists="append"
            )
            print("Data successfully uploaded to BigQuery.")
        except Exception as e:
            print(f"An error occurred while uploading to BigQuery: {e}")
    finally:
        driver.quit()
 
 
booking_properties_to_bq()
