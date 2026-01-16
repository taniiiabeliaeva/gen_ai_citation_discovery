# tools/openalex_tools.py
from langchain_core.tools import tool
from typing import List, Optional
from tools.data_utils import _get_openalex_id_from_title, _openalex_api_call, PAPER_DF
from core.graph_state import CitedWorksList, Work

# --- OpenAlex Tool List (Exported for agent.py) ---
ALL_OPENALEX_TOOLS = []


@tool
def get_citation_details(search_term: str) -> str:
    """Fetches the official BibTeX citation for a paper from OpenAlex."""
    openalex_id = _get_openalex_id_from_title(search_term)
    if not openalex_id:
        return f"Error: Paper matching '{search_term}' not found or has no OpenAlex ID."
    work_data = _openalex_api_call(f"/{openalex_id}")

    authors = ", ".join(
        [a["author"]["display_name"] for a in work_data.get("authorships", [])]
    )

    return Work(
        title=work_data.get("title", "N/A"),
        authors=authors,
        publication_year=work_data.get("publication_year", 0),
        openalex_id=openalex_id,
        doi=work_data.get("doi"),
    )


ALL_OPENALEX_TOOLS.append(get_citation_details)


@tool
def get_cited_works(search_term: str, limit: int = 5) -> CitedWorksList:
    """
    Finds papers that have cited the specified work and returns them as a structured list.
    """
    openalex_id = _get_openalex_id_from_title(search_term)
    if not openalex_id:
        return CitedWorksList(citing_works=[])

    filters = {
        "filter": f"cites:{openalex_id}",
        "sort": "cited_by_count:desc",
        "per_page": str(limit),
    }
    results = _openalex_api_call("", filters)

    if not results or not results.get("results"):
        return CitedWorksList(citing_works=[])

    works_list = []
    for work_data in results["results"]:
        authors = ", ".join(
            [a["author"]["display_name"] for a in work_data.get("authorships", [])]
        )
        oa_id = work_data.get("id", "N/A").replace("https://openalex.org/", "")

        works_list.append(
            Work(
                title=work_data.get("title", "N/A"),
                authors=authors,
                publication_year=work_data.get("publication_year", 0),
                openalex_id=oa_id,
                doi=work_data.get("doi"),
            )
        )

    return CitedWorksList(citing_works=works_list)


ALL_OPENALEX_TOOLS.append(get_cited_works)


@tool
def find_related_works(search_term: str, limit: int = 5) -> CitedWorksList:
    """
    Finds papers that are related to the specified work and returns them as a structured list.
    """
    openalex_id = _get_openalex_id_from_title(search_term)
    if not openalex_id:
        return CitedWorksList(citing_works=[])

    filters = {
        "filter": f"related_to:{openalex_id}",
        "sort": "cited_by_count:desc",
        "per_page": str(limit),
    }
    results = _openalex_api_call("", filters)

    if not results or not results.get("results"):
        return CitedWorksList(citing_works=[])

    works_list = []
    for work_data in results["results"]:
        authors = ", ".join(
            [a["author"]["display_name"] for a in work_data.get("authorships", [])]
        )
        oa_id = work_data.get("id", "N/A").replace("https://openalex.org/", "")

        works_list.append(
            Work(
                title=work_data.get("title", "N/A"),
                authors=authors,
                publication_year=work_data.get("publication_year", 0),
                openalex_id=oa_id,
                doi=work_data.get("doi"),
            )
        )

    return CitedWorksList(citing_works=works_list)


ALL_OPENALEX_TOOLS.append(find_related_works)


@tool
def get_referenced_works(search_term: str, limit: int = 5) -> CitedWorksList:
    """
    Finds papers that are REFERENCED BY the specified work (its bibliography) and returns them as a structured list.
    """
    openalex_id = _get_openalex_id_from_title(search_term)
    if not openalex_id:
        return CitedWorksList(citing_works=[])

    filters = {
        "filter": f"cited_by:{openalex_id}",
        "sort": "cited_by_count:desc",
        "per_page": str(limit),
    }
    results = _openalex_api_call("", filters)

    if not results or not results.get("results"):
        return CitedWorksList(citing_works=[])

    works_list = []
    for work_data in results["results"]:
        authors = ", ".join(
            [a["author"]["display_name"] for a in work_data.get("authorships", [])]
        )
        oa_id = work_data.get("id", "N/A").replace("https://openalex.org/", "")

        works_list.append(
            Work(
                title=work_data.get("title", "N/A"),
                authors=authors,
                publication_year=work_data.get("publication_year", 0),
                openalex_id=oa_id,
                doi=work_data.get("doi"),
            )
        )

    return CitedWorksList(citing_works=works_list)


ALL_OPENALEX_TOOLS.append(get_referenced_works)
