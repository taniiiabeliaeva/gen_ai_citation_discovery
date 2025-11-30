# tools/data_utils.py
import pandas as pd
import requests
import os
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()
OPENALEX_BASE_URL = 'https://api.openalex.org/works'
POLITE_EMAIL = os.getenv("POLITE_EMAIL")

# Global variables
PAPER_DF: pd.DataFrame = pd.DataFrame()
CSV_FILE_PATH = 'data/works_final.csv'

def load_paper_data():
    """Loads the CSV file into a global pandas DataFrame."""
    global PAPER_DF
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(f"CSV file not found at {CSV_FILE_PATH}. Please run setup scripts first.")
        
    df = pd.read_csv(CSV_FILE_PATH)
    df['openalex_id_short'] = df['openalex_id'].str.replace('https://openalex.org/', '', regex=False)
    PAPER_DF = df.fillna('')
    print(f"Data Loaded: {len(PAPER_DF)} papers.")

def _get_openalex_id_from_title(search_term: str) -> Optional[str]:
    """Helper to find the short OpenAlex ID."""
    match = PAPER_DF[
        PAPER_DF['title'].str.contains(search_term, case=False, na=False)
    ]
    return match.iloc[0]['openalex_id_short'] if not match.empty else None

def _openalex_api_call(endpoint_suffix: str, filters: Dict[str, str] = {}) -> Optional[Dict[str, Any]]:
    """Generic function to call the OpenAlex API."""
    url = f"{OPENALEX_BASE_URL}{endpoint_suffix}"
    params = {'mailto': POLITE_EMAIL, **filters}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error calling OpenAlex API at {url}: {e}")
        return None