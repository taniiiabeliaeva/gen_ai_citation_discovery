import pandas as pd
import re
import requests
import time
from difflib import SequenceMatcher

INPUT_CSV  = "/backend/data/works.csv"
OUTPUT_CSV = "/backend/data/works_openalex.csv"

SLEEP_SEC = 0.35          
TITLE_SIM_THRESHOLD = 0.85  

# ----------------------------
# Helpers
# ----------------------------
def clean_doi(doi):
    if not isinstance(doi, str):
        return None
    doi = doi.strip().lower()
    doi = re.sub(r"https?://(dx\.)?doi\.org/", "", doi)
    doi = doi.split()[0]
    return doi or None

def normalize_title(s):
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def title_similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

def fetch_openalex_by_doi(doi):
    if not doi:
        return {}
    url = f"https://api.openalex.org/works/https://doi.org/{doi}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return {}
        data = r.json()
        return {
            "match_method": "doi",
            "openalex_id": data.get("id"),
            "cited_by_count": data.get("cited_by_count"),
            "referenced_works_count": len(data.get("referenced_works", []) or []),
            "concepts": "; ".join(
                [c.get("display_name") for c in (data.get("concepts") or []) if c.get("display_name")]
            ),
        }
    except requests.RequestException:
        return {}

def fetch_openalex_by_title(title):
    if not title:
        return {}
    url = "https://api.openalex.org/works"
    params = {"search": title, "per-page": 1}
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            return {}
        results = r.json().get("results", [])
        if not results:
            return {}
        best = results[0]
        return {
            "match_method": "title",
            "openalex_id": best.get("id"),
            "oa_title": best.get("title"),
            "cited_by_count": best.get("cited_by_count"),
            "referenced_works_count": len(best.get("referenced_works", []) or []),
            "concepts": "; ".join(
                [c.get("display_name") for c in (best.get("concepts") or []) if c.get("display_name")]
            ),
        }
    except requests.RequestException:
        return {}

# ----------------------------
# Load
# ----------------------------
df = pd.read_csv(INPUT_CSV)

if "title" not in df.columns:
    raise ValueError("Missing required column: 'title'")

if "doi" not in df.columns:
    df["doi"] = None

# Clean DOI (so "https://doi.org/..." works)
df["doi"] = df["doi"].apply(clean_doi)


# Precompute normalized titles
df["_title_norm"] = df["title"].apply(normalize_title)

# ----------------------------
# Fetch OpenAlex per row (DOI else title)
# ----------------------------
records = []
for idx, row in df.iterrows():
    doi = row.get("doi")
    title = row.get("title") or ""
    title_norm = row.get("_title_norm") or ""

    meta = {}
    # 1) DOI path if present
    if isinstance(doi, str) and doi:
        meta = fetch_openalex_by_doi(doi)
        time.sleep(SLEEP_SEC)
    # 2) fallback: title search
    if (not meta) and title_norm:
        meta = fetch_openalex_by_title(title)
        time.sleep(SLEEP_SEC)

        # Accept only if the returned title is close enough
        if meta and meta.get("oa_title"):
            sim = title_similarity(title_norm, normalize_title(meta["oa_title"]))
            if sim >= TITLE_SIM_THRESHOLD:
                meta["title_similarity"] = round(sim, 3)
            else:
                meta = {}  # reject low-confidence match

    records.append(meta)

    if (len(records) % 50) == 0:
        print(f"Processed {len(records)} / {len(df)} rows")

# keep alignment: use df.index
meta_df = pd.DataFrame(records, index=df.index)

df_out = pd.concat([df.drop(columns=["_title_norm"]), meta_df.drop(columns=["oa_title"], errors="ignore")], axis=1)

df_out.to_csv(OUTPUT_CSV, index=False)
print("Matched OpenAlex IDs:", df_out["openalex_id"].notna().sum(), "/", len(df_out))
