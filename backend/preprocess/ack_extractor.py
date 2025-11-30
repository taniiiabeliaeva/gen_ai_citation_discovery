import requests
import pandas as pd
import time
import json
import os
import re
from urllib.parse import urlparse, urlsplit, urlunsplit, parse_qsl, urlencode, urljoin
from bs4 import BeautifulSoup
######################################################################################################
# Paths
######################################################################################################
data=""
######################################################################################################
# Functions
######################################################################################################
def fetch_abstract_from_doi(doi):
    url = f"https://api.crossref.org/works/{doi}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        # Extract abstract (if available)
        abstract = data["message"].get("abstract", None)
        # Return the abstract if it's found
        return abstract
    except requests.exceptions.RequestException as e:
        print(f"Error fetching DOI {doi}: {e}")
        return None
def fetch_abstract_from_doi_negotiation(doi):
    url = f"https://doi.org/{doi}"
    headers = {
        "Accept": "application/json",  # Request metadata in JSON format
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # The response will contain JSON metadata
        data = response.json()
        # Check if the abstract is present in the metadata (depends on the DOI source)
        abstract = data.get("abstract", None)
        return abstract
    except requests.exceptions.RequestException as e:
        print(f"Error with DOI content negotiation for {doi}: {e}")
        return None
def fetch_abstract_from_springer(doi):
    url = f"https://link.springer.com/{doi}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        # Search for the abstract on the Springer page
        abstract_section = soup.find('div', {'id': 'Abs1-content'})
        # Extract the text from the paragraph inside that section
        if abstract_section:
            abstract_text = abstract_section.find('p').get_text()
            return abstract_text
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error scraping DOI {doi}: {e}")
        return None
def fetch_abstract(row):
    doi = row["doi"]
    print(f"Fetching abstract for doi: {doi}")
    if not doi:
        return None
    abstract = fetch_abstract_from_doi(doi)
    if abstract:
        return abstract
    abstract = fetch_abstract_from_doi_negotiation(doi)
    if abstract:
        return abstract
    abstract = fetch_abstract_from_springer(doi)
    if abstract:
        return abstract
    return None

# ---------- NEW HELPERS (only for Repositum-hosted PDFs) ----------
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "repositum-pdf-fetcher/1.1"})
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
SESSION.mount("https://", HTTPAdapter(max_retries=retry))
SESSION.mount("http://", HTTPAdapter(max_retries=retry))

BASE_REPOSITUM = "https://repositum.tuwien.at"

# Separate session for HTML/PDF (no JSON Accept)
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "repositum-pdf-scraper/1.1"})

def _safe_filename(s, maxlen=150, default="file"):
    s = (s or default).strip()
    s = re.sub(r"[^\w\-.]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("._")
    return (s or default)[:maxlen]

def _extract_handle_from_metadata(metadata):
    """
    Robustly extract a handle like '20.500.12708/209747' from metadata values.
    Works for:
      - https://repositum.tuwien.at/handle/20.500.12708/209747
      - http://hdl.handle.net/20.500.12708/209747
      - any string that contains '20.500.12708/<suffix>'
    """
    candidates = []
    for m in metadata:
        if m.get("key") in ("dc.identifier.uri", "dc.identifier"):
            v = str(m.get("value", "")).strip()
            if not v:
                continue
            # 1) direct /handle/ form
            if "repositum.tuwien.at/handle/" in v:
                candidates.append(v.split("/handle/", 1)[-1].strip().strip("/"))
            # 2) hdl.handle.net form
            elif "hdl.handle.net" in v:
                # path is '/20.500.12708/209747'
                path = urlparse(v).path.strip("/")
                if path:
                    candidates.append(path)
            # 3) raw '20.500.12708/...' anywhere in the string
            else:
                mobj = re.search(r"(20\.500\.12708/\S+)", v)
                if mobj:
                    # strip trailing punctuation or HTML entities
                    cand = mobj.group(1).rstrip(").,;\"'")
                    candidates.append(cand)

    # Return the first good-looking candidate
    for c in candidates:
        if c.startswith("20.500.12708/"):
            return c
    return None

def _scrape_pdf_links_from_handle(handle):
    """Return absolute PDF URLs from the handle HTML page."""
    if not handle:
        return []
    url = f"{BASE_REPOSITUM}/handle/{handle}"
    try:
        r = HTTP.get(url, timeout=(8, 25), headers={"Accept": "text/html"})
        if r.status_code != 200:
            print(f"    > Handle GET {r.status_code}: {url}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        pdfs = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Only bitstreams; prefer obvious PDFs
            if "/bitstream/" in href:
                is_pdf = href.lower().endswith(".pdf") \
                         or "pdf" in (a.get("type", "") or "").lower() \
                         or "pdf" in (a.get_text("") or "").lower()
                if is_pdf:
                    pdfs.add(urljoin(BASE_REPOSITUM, href))
        return sorted(pdfs)
    except requests.RequestException as e:
        print(f"    > Handle request error: {e}")
        return []

def _download_pdf(url, title_hint, dst_dir="pdfs"):
    """Stream a PDF to disk; returns local path or None."""
    os.makedirs(dst_dir, exist_ok=True)
    name = _safe_filename(title_hint or os.path.basename(urlparse(url).path) or "document")
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    path = os.path.join(dst_dir, name)
    if os.path.exists(path):
        stem, ext = os.path.splitext(name)
        i = 2
        while os.path.exists(os.path.join(dst_dir, f"{stem}_{i}{ext}")):
            i += 1
        path = os.path.join(dst_dir, f"{stem}_{i}{ext}")
    try:
        with HTTP.get(url, stream=True, timeout=(8, 60), headers={"Accept": "*/*"}) as r:
            if r.status_code != 200:
                print(f"    > PDF GET {r.status_code}: {url}")
                return None
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=262144):
                    if chunk:
                        f.write(chunk)
        return path
    except requests.RequestException as e:
        print(f"    > PDF download error: {e}")
        return None

def fetch_repositum_pdfs_by_handle(metadata, title_hint=""):
    """End-to-end: metadata -> handle -> scrape -> download -> [paths]."""
    handle = _extract_handle_from_metadata(metadata)
    if not handle:
        return []
    urls = _scrape_pdf_links_from_handle(handle)
    if not urls:
        return []
    paths = []
    for u in urls:
        p = _download_pdf(u, title_hint or os.path.basename(urlparse(u).path))
        if p:
            paths.append(p)
    return paths


def query_repositum(url, pub_type, offset, rows_out, max_items=None, sleep_between=0.03):
    processed, total = 0, None
    while True:
        response = requests.get(
            f"{url}&offset={offset}",
            headers={"Accept": "application/json"},
            timeout=30
        )
        if response.status_code != 200:
            print(f"[WARN] Offset {offset}: HTTP {response.status_code}. Retrying after 2s…")
            time.sleep(2)
            response = requests.get(f"{url}&offset={offset}", headers={"Accept": "application/json"}, timeout=30)
            if response.status_code != 200:
                print(f"[ERROR] Offset {offset}: still HTTP {response.status_code}. Stopping.")
                break

        repositum_pubs = response.json()
        total = repositum_pubs.get("numberOfItems", 0) if total is None else total
        items = repositum_pubs.get("items", [])
        if not items:
            print("[INFO] No more items; stopping.")
            break

        print(f"[PAGE] Items {offset+1}–{min(offset+len(items), total)} of ~{total} (type={pub_type})")
        for item in items:
            try:
                metadata = item["metadata"]
                orgunit = ";".join({m["value"][0:4] for m in metadata if m["key"] == "tuw.publication.orgunit"})
                title = "".join([m["value"] for m in metadata if m["key"] == "dc.title"])
                if pub_type == "thesis":
                    pub_sub_type = "".join([m["value"] for m in metadata if m["key"] == "dc.type.qualificationlevel"])
                elif pub_type == "publication":
                    pub_sub_type = "".join({m["value"] for m in metadata if m["key"] == "dc.type" and m.get("language") == "en"})
                else:
                    pub_sub_type = ""
                date_issued = "".join([m["value"] for m in metadata if m["key"] == "dc.date.issued"])
                authors = ",".join([m["value"] for m in metadata if m["key"] == "dc.contributor.author"])
                abstract_en = "".join([str(m["value"]) for m in metadata if m["key"] == "dc.description.abstract" and m.get("language") == "en"])
                abstract_de = "".join([str(m["value"]) for m in metadata if m["key"] == "dc.description.abstract" and m.get("language") == "de"])
                doi = "".join([str(m["value"]) for m in metadata if m["key"] == "tuw.publisher.doi"])
                publisher = "".join([str(m["value"]) for m in metadata if m["key"] == "tuw.relation.publisher"])

                # NEW: scrape handle page for PDFs and download
                pdf_local_paths = fetch_repositum_pdfs_by_handle(metadata, title_hint=title)
                if pdf_local_paths:
                    print(f"  [+] {title[:70]}… -> {len(pdf_local_paths)} PDF(s)")
                else:
                    print(f"  [0]  {title[:70]}… -> no Repositum PDF")

                rows_out.append({
                    "orgunit": orgunit,
                    "title": title,
                    "pub_type": pub_sub_type,
                    "date_issued": date_issued,
                    "authors": authors,
                    "doi": doi,
                    "publisher": publisher,
                    "abstract_en": abstract_en,
                    "abstract_de": abstract_de,
                    "pdf_paths": ";".join(pdf_local_paths),
                    "pdf_count": len(pdf_local_paths),
                })

                processed += 1
                if max_items and processed >= max_items:
                    print(f"[INFO] Reached max_items={max_items}; stopping.")
                    return
            except Exception as e:
                print(f"[WARN] Skipping one item due to error: {e}")
            time.sleep(sleep_between)

        offset += len(items)
        if total and offset >= total:
            break

######################################################################################################
# Pipeline
######################################################################################################
dataset_columns = ["orgunit", "title", "pub_type", "date_issued", "authors", "doi", "publisher", "abstract_en", "abstract_de", "pdf_paths", "pdf_count"]

rows = []
query_repositum(
    "https://repositum.tuwien.at/rest/orgunit/publications_full/tiss_id/1601?count=true&recursive=true&from=2018",
    "publication",
    0,
    rows,
    # max_items=50,  # optional quick test
)
publications_df = pd.DataFrame(rows, columns=dataset_columns)
print("Saving to works.csv (PDFs in ./pdfs/ when present on Repositum)")
publications_df.to_csv("works.csv", escapechar="\\", index=False)
#theses_list = []
#query_repositum("https://repositum.tuwien.at/rest/orgunit/supervised_full/tiss_id/1601?count=true&recursive=true&from=2016",
#                "thesis", 0, theses_list)
#theses_df = pd.DataFrame(theses_list, columns=dataset_columns)
# Combine publications and theses
#print("Stacking theses and publications dataframe.")
#results_df = pd.concat([publications_df, theses_df], axis=0, ignore_index=True)
# Create a column "abstract" which is either the english abstract, the german abstract in case the english does not exist, nor null
#print("Creating abstracts column with either english or -- in case the english is None -- with german abstracts")
#results_df["abstract"] = results_df["abstract_en"].replace("", None).fillna(results_df["abstract_de"])
#results_df["abstract"] = results_df["abstract"].replace("", None)
## Extract the year from the date_issued
#print("Extraing year from date_issued")
#results_df["year"] = results_df["date_issued"].str.extract(r"(\d{4})").astype(float).astype("Int64")
## Fetch abstracts from doi metadata or publisher
#print("Fetching abstracts from doi metadata or publisher.")
#results_df["doi"] = results_df["doi"].replace("", None)
#results_df_copy = results_df.copy()
#results_df_without_abstracts = results_df_copy[results_df_copy["abstract"].isnull()]
#print(f"There are {results_df_without_abstracts.shape[0]} works without an abstract.")
#abstracts_fetched = results_df_without_abstracts.apply(fetch_abstract, axis=1)
#results_df.loc[results_df_without_abstracts.index, "abstract"] = abstracts_fetched
# Save works
#print(f"Saving to {data}/works.csv")
#results_df.to_csv(f"{data}/works.csv", escapechar="\\", index=False)





