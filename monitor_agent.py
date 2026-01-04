"""World Bank GIS Opportunities Monitor Agent for Nigeria.

This script is designed to run once per day (e.g., as a PythonAnywhere
scheduled task). It monitors three data streams:

1. Projects (search.worldbank.org Projects API)
   - Detects new or updated World Bank projects in Nigeria that have a
     GIS / spatial / environmental analytics angle.

2. Procurement Notices / Tenders (Finances One dataset DS00979)
   - Detects new procurement opportunities (EOI, RFP, etc.) related to GIS.

3. Contract Awards (Finances One dataset DS01666)
   - Detects newly awarded contracts relevant to GIS to provide market
     intelligence about potential competitors or partners.

It sends four kinds of Discord embeds via a webhook:

- Project Plan Alert (new / updated projects)
- Tender / EOI Alert (job opportunities)
- Contract Award Alert (competitor intelligence)
- Weekly Heartbeat (dead-man's switch)

The script is built to be safe for PythonAnywhere free tier:
- Uses only the standard library plus `requests`.
- Uses lightweight JSON files for state.
- Has robust logging and network retry logic.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from time import sleep
from typing import Any, Dict, Iterable, List

import requests
from urllib.parse import quote_plus


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOGGER = logging.getLogger("wb_gis_monitor_agent")

# Discord webhook URL: use environment variable for deployment.
ENV_WEBHOOK_URL = os.getenv("WB_DISCORD_WEBHOOK_URL")
DISCORD_WEBHOOK_URL = (
    ENV_WEBHOOK_URL
    if ENV_WEBHOOK_URL
    else "https://discordapp.com/api/webhooks/REPLACE_ME"
)

# State files (stored in the same directory as the script).
PROJECTS_STATE_FILE = "processed_projects.json"
DOCS_STATE_FILE = "processed_docs.json"
TENDERS_STATE_FILE = "processed_tenders.json"
AWARDS_STATE_FILE = "processed_awards.json"
MONITOR_STATE_FILE = "monitor_state.json"

# World Bank endpoints.
WB_PROJECTS_API_URL = "https://search.worldbank.org/api/v2/projects"
WB_WDS_API_URL = "https://search.worldbank.org/api/v3/wds"
WB_FINANCES_DATA_URL = "https://datacatalogapi.worldbank.org/dexapps/fone/api/data"

# Finances One asset IDs (currently not publicly queryable via the simple
# REST style used here; the corresponding streams are left in the codebase
# but can be disabled via flags below until a working endpoint is found).
TENDER_ASSET_ID = "DS00979"  # Procurement notices (tenders / EOI).
AWARD_ASSET_ID = "DS01666"  # Contract awards.

# Feature flags for optional streams.
ENABLE_TENDERS_STREAM = False
ENABLE_AWARDS_STREAM = False

# HTTP timeout and retry behaviour.
HTTP_TIMEOUT = 15
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3

# Country code for Nigeria.
NIGERIA_COUNTRY_CODE = "NG"

# Single, central GIS/spatial/environment/health keyword list reused
# across all streams.
KEYWORDS: List[str] = [
    # Core GIS / spatial
    "GIS",
    "Geographic Information System",
    "Geospatial",
    "Geo-spatial",
    "Spatial",
    "Spatial Data",
    "Spatial Analysis",
    "Spatial Analytics",
    "Spatial Planning",
    "Location Intelligence",
    "Geo-data",
    "Location Data",
    "Location-based",

    # Mapping / cartography
    "Mapping",
    "Map",
    "Cartography",
    "Topographic",
    "Topography",
    "Base Map",
    "Basemap",

    # Remote sensing / imagery
    "Remote Sensing",
    "Earth Observation",
    "EO Data",
    "Satellite",
    "Satellite Imagery",
    "Imagery",
    "Image Analysis",
    "LiDAR",
    "Raster Data",

    # Drones / aerial surveys
    "UAV",
    "Drone",
    "Aerial Survey",
    "Aerial Photography",

    # Surveying / land administration
    "GNSS",
    "GPS",
    "Survey",
    "Surveying",
    "Land Survey",
    "Cadastral",
    "Cadastre",
    "Land Administration",
    "Land Registration",
    "Land Use",
    "Parcel",
    "Parcel Mapping",
    "Boundary",
    "Boundary Mapping",
    "Geodetic",

    # Environment / climate / DRM
    "Environmental Monitoring",
    "Environmental Information System",
    "Natural Resource Mapping",
    "Natural Resource Management",
    "Watershed Management",
    "Hydrological Modeling",
    "Hydrology",
    "Flood Risk Mapping",
    "Flood Hazard Mapping",
    "Disaster Risk Mapping",
    "Disaster Risk Management",
    "DRM Platform",
    "Climate Risk Mapping",
    "Climate Risk Assessment",
    "Climate Vulnerability",
    "Vulnerability Mapping",
    "Coastal Erosion Mapping",
    "Ecosystem Mapping",
    "Habitat Mapping",
    "Biodiversity Mapping",
    "Climate Resilience",

    # Urban / transport planning
    "Urban Spatial Planning",
    "Urban Planning",
    "Transport Modeling",
    "Accessibility Analysis",
    "Network Analysis",

    # Health / epidemiology
    "Health Mapping",
    "Disease Mapping",
    "Epidemiological Mapping",
    "Spatial Epidemiology",
    "Disease Hotspot",
    "Hotspot Mapping",
    "Disease Surveillance",
    "Outbreak Mapping",
    "Health Facility Mapping",
    "Service Coverage Mapping",
]


# Extra terms to enrich contractor-oriented Google search queries.
CONTRACTOR_SEARCH_TERMS: List[str] = [
    # Core GIS / spatial
    "GIS",
    "geospatial",
    "spatial analysis",
    "spatial analytics",
    "mapping",
    "remote sensing",
    "satellite imagery",
    "drone survey",
    "land administration",
    "cadastral mapping",
    "surveying",
    "spatial data",

    # Environment / climate / DRM
    "environmental monitoring",
    "natural resource management",
    "watershed management",
    "hydrology",
    "flood risk",
    "disaster risk management",
    "climate risk",
    "climate resilience",

    # Health / epidemiology
    "health mapping",
    "disease surveillance",
    "epidemiology",

    # Implementation / business terms
    "consultant",
    "consulting",
    "technical assistance",
    "implementation",
    "implementation partner",
    "engineering",
    "engineering firm",
]


# ---------------------------------------------------------------------------
# Generic utilities
# ---------------------------------------------------------------------------


def get_with_retries(
    url: str,
    *,
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    timeout: int = HTTP_TIMEOUT,
    max_retries: int = MAX_RETRIES,
) -> requests.Response | None:
    """Perform a GET request with basic retry logic.

    Returns the last successful response or None if all attempts fail.
    """

    attempt = 1
    while attempt <= max_retries:
        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            )
            return response
        except requests.RequestException as exc:  # pragma: no cover - network
            if attempt == max_retries:
                LOGGER.error(
                    "GET %s failed after %d attempts: %s",
                    url,
                    attempt,
                    exc,
                )
                return None

            LOGGER.warning(
                "GET %s failed on attempt %d/%d: %s; retrying...",
                url,
                attempt,
                max_retries,
                exc,
            )
            sleep(RETRY_BACKOFF_SECONDS * attempt)
            attempt += 1

    return None


def post_with_retries(
    url: str,
    *,
    payload: Dict[str, Any],
    headers: Dict[str, str] | None = None,
    timeout: int = HTTP_TIMEOUT,
    max_retries: int = MAX_RETRIES,
) -> requests.Response | None:
    """Perform a POST request with basic retry logic."""

    attempt = 1
    while attempt <= max_retries:
        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=timeout,
            )
            return response
        except requests.RequestException as exc:  # pragma: no cover - network
            if attempt == max_retries:
                LOGGER.error(
                    "POST %s failed after %d attempts: %s",
                    url,
                    attempt,
                    exc,
                )
                return None

            LOGGER.warning(
                "POST %s failed on attempt %d/%d: %s; retrying...",
                url,
                attempt,
                max_retries,
                exc,
            )
            sleep(RETRY_BACKOFF_SECONDS * attempt)
            attempt += 1

    return None


def text_matches_keywords(text: str, keywords: Iterable[str]) -> bool:
    """Return True if any keyword is found in the given text (case-insensitive)."""

    if not text:
        return False
    lower_text = text.lower()
    for kw in keywords:
        if kw.lower() in lower_text:
            return True
    return False


def build_contractor_search_url(project_name: str) -> str:
    """Build a Google search URL focused on contractor discovery.

    Logic (extended):
    https://www.google.com/search?q="World+Bank"+"[Project Name]"+<GIS terms>+"contractor"
    """

    terms: List[str] = [f'"World Bank"', f'"{project_name}"']
    for term in CONTRACTOR_SEARCH_TERMS:
        terms.append(f'"{term}"')
    terms.append('"contractor"')

    query = "+".join(terms)
    encoded_query = quote_plus(query)
    return f"https://www.google.com/search?q={encoded_query}"


# ---------------------------------------------------------------------------
# JSON state helpers
# ---------------------------------------------------------------------------


def _load_state_map(path: str) -> Dict[str, str]:
    """Load a mapping of ID -> last_update string from disk.

    Returns an empty dict on any error or if the file does not exist.
    """

    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}

    if isinstance(data, dict):
        return {str(k): str(v) for k, v in data.items()}

    if isinstance(data, list):
        # Legacy format: list of IDs only.
        return {str(item): "" for item in data}

    return {}


def _save_state_map(path: str, mapping: Dict[str, str]) -> None:
    """Persist mapping of ID -> last_update string to disk."""

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2)
    except OSError as exc:
        LOGGER.error("Failed to save state to %s: %s", path, exc)


def load_monitor_state(path: str = MONITOR_STATE_FILE) -> Dict[str, str]:
    """Load the monitor's own state (e.g., last heartbeat date)."""

    return _load_state_map(path)


def save_monitor_state(state: Dict[str, str], path: str = MONITOR_STATE_FILE) -> None:
    """Save the monitor's own state to disk."""

    _save_state_map(path, state)


# ---------------------------------------------------------------------------
# Stream 1: Projects
# ---------------------------------------------------------------------------


def fetch_projects_for_nigeria(rows_per_page: int = 50) -> List[Dict[str, Any]]:
    """Fetch active World Bank projects for Nigeria.

    Uses the search.worldbank.org Projects API.
    """

    all_projects: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = {
            "format": "json",
            "status": "Active",
            # countrycode_exact hint; we'll still filter client-side as backup.
            "countrycode_exact": NIGERIA_COUNTRY_CODE,
            "rows": rows_per_page,
            "page": page,
        }

        response = get_with_retries(
            WB_PROJECTS_API_URL,
            params=params,
            headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
        )
        if response is None:
            LOGGER.error("Failed to fetch projects page %d.", page)
            break

        try:
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover - network
            LOGGER.error("Projects API error on page %d: %s", page, exc)
            break

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            LOGGER.error("Failed to parse JSON from Projects API: %s", exc)
            break

        total = int(payload.get("total", 0))
        rows = int(payload.get("rows", rows_per_page))

        projects = payload.get("projects") or {}
        if not isinstance(projects, dict):
            LOGGER.warning("Unexpected 'projects' structure from API.")
            break

        # Filter to projects where Nigeria (NG) is in the country code list.
        for proj in projects.values():
            country_codes = proj.get("countrycode") or []
            if isinstance(country_codes, list) and NIGERIA_COUNTRY_CODE in country_codes:
                all_projects.append(proj)

        LOGGER.info(
            "Projects: page %d -> %d rows (total=%d).",
            page,
            len(projects),
            total,
        )

        if page * rows >= total or not projects:
            break

        page += 1

    return all_projects


def extract_project_text(project: Dict[str, Any]) -> str:
    """Concatenate title and abstract-like fields into a single text blob."""

    parts: List[str] = []
    title = project.get("project_name") or ""
    if isinstance(title, str):
        parts.append(title)

    abstract = project.get("project_abstract")
    if isinstance(abstract, dict):
        for value in abstract.values():
            if isinstance(value, str):
                parts.append(value)

    return " \n".join(parts)


def project_matches_keywords(project: Dict[str, Any]) -> bool:
    """Return True if any GIS keyword is found in the project text."""

    return text_matches_keywords(extract_project_text(project), KEYWORDS)


def get_project_last_update(project: Dict[str, Any]) -> str:
    """Return the last-update marker for a project.

    Uses `p2a_updated_date` if present, otherwise an empty string.
    """

    updated = project.get("p2a_updated_date") or ""
    if isinstance(updated, str):
        return updated
    return ""


def format_project_approval_date(project: Dict[str, Any]) -> str:
    """Return a human-friendly approval date for the project."""

    raw_date = project.get("boardapprovaldate")
    if isinstance(raw_date, str) and raw_date:
        return raw_date.split("T", 1)[0]
    return "N/A"


def format_project_total_amount(project: Dict[str, Any]) -> str:
    """Return the total funding amount for the project."""

    amount = project.get("totalamt") or project.get("totalcommamt")
    if isinstance(amount, str) and amount:
        return f"${amount}"
    return "N/A"


def get_project_url(project: Dict[str, Any]) -> str:
    """Return the World Bank project detail URL."""

    url = project.get("url")
    if isinstance(url, str) and url:
        return url

    proj_id = str(project.get("id") or "").strip()
    if proj_id:
        return (
            "https://projects.worldbank.org/en/projects-operations/"
            f"project-detail/{proj_id}"
        )

    return "https://projects.worldbank.org/en/projects-operations/projects-overview"


def send_project_alert_embed(
    project: Dict[str, Any],
    is_update: bool,
) -> bool:
    """Send a Discord embed for a new or updated project plan."""

    if not DISCORD_WEBHOOK_URL or "REPLACE_ME" in DISCORD_WEBHOOK_URL:
        LOGGER.warning("Webhook URL not configured; skipping project alert.")
        return False

    project_name = project.get("project_name") or "(No title)"
    project_id = str(project.get("id") or "").strip() or "(unknown)"

    approval_date = format_project_approval_date(project)
    total_amount = format_project_total_amount(project)
    project_url = get_project_url(project)
    contractor_url = build_contractor_search_url(str(project_name))

    alert_type = "Project Update" if is_update else "New Project Plan"

    embed = {
        "title": f"{alert_type}: {project_name}",
        "url": project_url,
        "color": 0x1ABC9C,  # teal
        "fields": [
            {"name": "Project ID", "value": project_id, "inline": True},
            {"name": "Approval Date", "value": approval_date, "inline": True},
            {"name": "Total Funding Amount", "value": total_amount, "inline": True},
            {
                "name": "World Bank Project Page",
                "value": f"[Open Project]({project_url})",
                "inline": False,
            },
            {
                "name": "Find Contractor",
                "value": f"[Search on Google]({contractor_url})",
                "inline": False,
            },
        ],
        "footer": {
            "text": "World Bank GIS Monitor (Projects, Nigeria)",
        },
    }

    payload = {
        "username": "World Bank GIS Monitor",
        "embeds": [embed],
    }

    response = post_with_retries(
        DISCORD_WEBHOOK_URL,
        payload=payload,
        headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
    )
    if response is None:
        LOGGER.error("Project alert webhook failed after retries.")
        return False

    if 200 <= response.status_code < 300:
        LOGGER.info("Sent project %s alert (update=%s).", project_id, is_update)
        return True

    LOGGER.error(
        "Project alert webhook returned %s: %s",
        response.status_code,
        response.text,
    )
    return False


# ---------------------------------------------------------------------------
# Stream 2: Procurement Plan Documents (WDS /v3/wds)
# ---------------------------------------------------------------------------


def fetch_procurement_plans_for_nigeria(rows: int = 200) -> List[Dict[str, Any]]:
    """Fetch recent procurement plan documents for Nigeria from WDS.

    Uses the /api/v3/wds endpoint, filtered by country name "Nigeria" and
    document type "Procurement Plan". The result format is a dictionary of
    document IDs keyed by an internal ID (e.g. "D40079170").
    """

    params = {
        "format": "json",
        "rows": rows,
        "country": "Nigeria",
        "docty_exact": "Procurement Plan",
    }

    response = get_with_retries(
        WB_WDS_API_URL,
        params=params,
        headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
    )
    if response is None:
        LOGGER.error("Failed to fetch procurement plan documents.")
        return []

    try:
        response.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network
        LOGGER.error("WDS documents API error: %s", exc)
        return []

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        LOGGER.error("Failed to parse JSON from WDS documents API: %s", exc)
        return []

    documents = payload.get("documents") or {}
    if not isinstance(documents, dict):
        LOGGER.warning("Unexpected WDS 'documents' structure; skipping.")
        return []

    rows_list: List[Dict[str, Any]] = []
    for key, value in documents.items():
        if key == "facets":
            continue
        if isinstance(value, dict):
            rows_list.append(value)

    LOGGER.info("WDS: fetched %d procurement plan documents for Nigeria.", len(rows_list))
    return rows_list


def extract_document_text(doc: Dict[str, Any]) -> str:
    """Extract a combined text field from a WDS document record."""

    parts: List[str] = []

    display_title = doc.get("display_title")
    if isinstance(display_title, str):
        parts.append(display_title)

    docna = doc.get("docna")
    if isinstance(docna, dict):
        for entry in docna.values():
            if isinstance(entry, dict):
                title = entry.get("docna")
                if isinstance(title, str):
                    parts.append(title)

    for key in ("theme", "subsc"):
        value = doc.get(key)
        if isinstance(value, str):
            parts.append(value)

    sectr = doc.get("sectr")
    if isinstance(sectr, dict):
        for entry in sectr.values():
            if isinstance(entry, dict):
                sector = entry.get("sector")
                if isinstance(sector, str):
                    parts.append(sector)

    return " \n".join(parts)


def document_matches_keywords(doc: Dict[str, Any]) -> bool:
    """Return True if any GIS keyword is found in the document metadata text."""

    return text_matches_keywords(extract_document_text(doc), KEYWORDS)


def get_document_id(doc: Dict[str, Any]) -> str:
    """Return the WDS document ID as a string."""

    value = doc.get("id")
    return str(value) if value is not None else ""


def get_document_last_update(doc: Dict[str, Any]) -> str:
    """Return last-modified marker for a WDS document."""

    updated = doc.get("last_modified_date") or ""
    if isinstance(updated, str):
        return updated
    return ""


def get_document_urls(doc: Dict[str, Any]) -> tuple[str, str]:
    """Return (page_url, pdf_url) for the document, with safe fallbacks."""

    page_url = doc.get("url")
    if not isinstance(page_url, str) or not page_url:
        page_url = "https://documents.worldbank.org/en/publication/documents-reports"

    pdf_url = doc.get("pdfurl")
    if not isinstance(pdf_url, str) or not pdf_url:
        pdf_url = page_url

    return page_url, pdf_url


def send_procurement_plan_alert_embed(doc: Dict[str, Any], is_update: bool) -> bool:
    """Send a Discord embed for a new or updated procurement plan document."""

    if not DISCORD_WEBHOOK_URL or "REPLACE_ME" in DISCORD_WEBHOOK_URL:
        LOGGER.warning("Webhook URL not configured; skipping document alert.")
        return False

    doc_id = get_document_id(doc) or "(unknown)"

    project_id = doc.get("projectid") or "N/A"
    project_name = doc.get("projn") or "(Unknown project)"
    docty = doc.get("docty") or "Procurement Plan"

    doc_date = (
        doc.get("docdt")
        or doc.get("disclosure_date")
        or doc.get("datestored")
        or "N/A"
    )

    display_title = doc.get("display_title") or project_name

    page_url, pdf_url = get_document_urls(doc)

    alert_type = "Updated Procurement Plan" if is_update else "New Procurement Plan"

    embed = {
        "title": f"{alert_type}: {display_title}",
        "url": pdf_url,
        "color": 0x2ECC71,  # green
        "fields": [
            {"name": "Document ID", "value": doc_id, "inline": True},
            {"name": "Project ID", "value": str(project_id), "inline": True},
            {"name": "Document Type", "value": str(docty), "inline": True},
            {"name": "Document Date", "value": str(doc_date), "inline": True},
            {
                "name": "Project / Document Page",
                "value": f"[Open in Browser]({page_url})",
                "inline": False,
            },
            {
                "name": "Download Procurement Plan (PDF)",
                "value": f"[Open PDF]({pdf_url})",
                "inline": False,
            },
        ],
        "footer": {
            "text": "World Bank GIS Monitor (Procurement Plans, Nigeria)",
        },
    }

    payload = {
        "username": "World Bank GIS Monitor",
        "embeds": [embed],
    }

    response = post_with_retries(
        DISCORD_WEBHOOK_URL,
        payload=payload,
        headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
    )
    if response is None:
        LOGGER.error("Procurement plan alert webhook failed after retries.")
        return False

    if 200 <= response.status_code < 300:
        LOGGER.info("Sent procurement plan alert for document %s.", doc_id)
        return True

    LOGGER.error(
        "Procurement plan alert webhook returned %s: %s",
        response.status_code,
        response.text,
    )
    return False


# ---------------------------------------------------------------------------
# Stream 3: Procurement Notices / Tenders (Finances One DS00979)
# ---------------------------------------------------------------------------


def fetch_tenders_for_nigeria(limit: int = 500) -> List[Dict[str, Any]]:
    """Fetch recent procurement notices / tenders for Nigeria.

    This uses the Finances One data API with assetId=DS00979.

    NOTE: Filter parameter names (e.g., `country_code`, `q`) may need to be
    adjusted based on live testing. The current implementation is a best
    guess and is designed to fail gracefully if the API rejects the
    parameters.
    """

    params = {
        "assetId": TENDER_ASSET_ID,
        # Most likely country filter for procurement notices.
        # You may need to try variations like `borrower_country_code`.
        "country_code": NIGERIA_COUNTRY_CODE,
        # Keyword search term (may be `search_term` or similar).
        # Start with a compact subset of GIS keywords to keep the query short.
        "q": "GIS geospatial mapping remote sensing land administration",
        # Pagination / limit placeholders (to be adjusted after testing).
        "page": 1,
        "page_size": limit,
    }

    response = get_with_retries(
        WB_FINANCES_DATA_URL,
        params=params,
        headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
    )
    if response is None:
        LOGGER.error("Failed to fetch procurement notices.")
        return []

    try:
        response.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network
        LOGGER.error("Tenders API error: %s", exc)
        return []

    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        LOGGER.error("Failed to parse JSON from Tenders API: %s", exc)
        return []

    # The Finances One API can return data under different keys; we try a
    # couple of common patterns and fall back gracefully.
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("data") or data.get("rows") or []
    else:
        rows = []

    if not isinstance(rows, list):
        LOGGER.warning("Unexpected tenders data structure; skipping.")
        return []

    LOGGER.info("Fetched %d procurement notices (raw).", len(rows))
    return [r for r in rows if isinstance(r, dict)]


def extract_tender_text(record: Dict[str, Any]) -> str:
    """Extract a combined text field from a tender record."""

    parts: List[str] = []

    for key in (
        "notice_title",
        "tender_title",
        "contract_title",
        "description",
        "summary",
    ):
        value = record.get(key)
        if isinstance(value, str):
            parts.append(value)

    return " \n".join(parts)


def tender_matches_keywords(record: Dict[str, Any]) -> bool:
    """Return True if any GIS keyword is found in the tender text."""

    return text_matches_keywords(extract_tender_text(record), KEYWORDS)


def get_tender_id(record: Dict[str, Any]) -> str:
    """Best-effort extraction of a stable tender / notice ID."""

    for key in ("notice_id", "tender_id", "contract_id", "id"):
        value = record.get(key)
        if value is not None:
            return str(value)
    return ""


def get_tender_last_update(record: Dict[str, Any]) -> str:
    """Return a last-update marker for a tender record.

    This may need adjustment once the exact schema is known. Common
    candidates include `notice_publish_date`, `updated_date`, etc.
    """

    for key in (
        "updated_date",
        "last_update_date",
        "notice_publish_date",
        "publish_date",
    ):
        value = record.get(key)
        if isinstance(value, str):
            return value
    return ""


def get_tender_url(record: Dict[str, Any]) -> str:
    """Return a URL for the tender if present, or a generic fallback."""

    for key in ("url", "notice_url", "tender_url", "link"):
        value = record.get(key)
        if isinstance(value, str) and value:
            return value

    # Generic procurement search page as fallback.
    return (
        "https://www.worldbank.org/en/projects-operations/products-and-"
        "services/procurement-projects-programs"
    )


def send_tender_alert_embed(record: Dict[str, Any]) -> bool:
    """Send a Discord embed for a new GIS-related tender / EOI."""

    if not DISCORD_WEBHOOK_URL or "REPLACE_ME" in DISCORD_WEBHOOK_URL:
        LOGGER.warning("Webhook URL not configured; skipping tender alert.")
        return False

    tender_id = get_tender_id(record) or "(unknown)"

    title = (
        record.get("notice_title")
        or record.get("tender_title")
        or record.get("contract_title")
        or "(Untitled procurement notice)"
    )

    method = (
        record.get("procurement_method")
        or record.get("procurement_method_name")
        or record.get("method")
        or "N/A"
    )

    publish_date = (
        record.get("notice_publish_date")
        or record.get("publish_date")
        or record.get("date")
        or "N/A"
    )

    url = get_tender_url(record)

    embed = {
        "title": f"New GIS Tender / EOI: {title}",
        "url": url,
        "color": 0xE67E22,  # orange
        "fields": [
            {"name": "Notice / Tender ID", "value": tender_id, "inline": True},
            {"name": "Procurement Method", "value": method, "inline": True},
            {"name": "Publish Date", "value": publish_date, "inline": True},
            {
                "name": "Tender / EOI Details",
                "value": f"[Open Notice]({url})",
                "inline": False,
            },
        ],
        "footer": {
            "text": "World Bank GIS Monitor (Tenders, Nigeria)",
        },
    }

    payload = {
        "username": "World Bank GIS Monitor",
        "embeds": [embed],
    }

    response = post_with_retries(
        DISCORD_WEBHOOK_URL,
        payload=payload,
        headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
    )
    if response is None:
        LOGGER.error("Tender alert webhook failed after retries.")
        return False

    if 200 <= response.status_code < 300:
        LOGGER.info("Sent tender alert for %s.", tender_id)
        return True

    LOGGER.error(
        "Tender alert webhook returned %s: %s",
        response.status_code,
        response.text,
    )
    return False


# ---------------------------------------------------------------------------
# Stream 3: Contract Awards (Finances One DS01666)
# ---------------------------------------------------------------------------


def fetch_awards_for_nigeria(limit: int = 500) -> List[Dict[str, Any]]:
    """Fetch recent contract awards for Nigeria.

    This uses the Finances One data API with assetId=DS01666.

    NOTE: Filter parameter names (e.g., `borrower_country_code`, `q`) may
    need to be adjusted based on live testing. The implementation is
    conservative and fails gracefully.
    """

    params = {
        "assetId": AWARD_ASSET_ID,
        # Most likely country filter for awards.
        "borrower_country_code": NIGERIA_COUNTRY_CODE,
        # Keyword search term placeholder.
        "q": "GIS geospatial mapping remote sensing land administration",
        # Pagination / limit placeholders.
        "page": 1,
        "page_size": limit,
    }

    response = get_with_retries(
        WB_FINANCES_DATA_URL,
        params=params,
        headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
    )
    if response is None:
        LOGGER.error("Failed to fetch contract awards.")
        return []

    try:
        response.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network
        LOGGER.error("Awards API error: %s", exc)
        return []

    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        LOGGER.error("Failed to parse JSON from Awards API: %s", exc)
        return []

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("data") or data.get("rows") or []
    else:
        rows = []

    if not isinstance(rows, list):
        LOGGER.warning("Unexpected awards data structure; skipping.")
        return []

    LOGGER.info("Fetched %d contract awards (raw).", len(rows))
    return [r for r in rows if isinstance(r, dict)]


def extract_award_text(record: Dict[str, Any]) -> str:
    """Extract a combined text field from an award record."""

    parts: List[str] = []

    for key in (
        "contract_title",
        "description",
        "procurement_description",
        "project_name",
    ):
        value = record.get(key)
        if isinstance(value, str):
            parts.append(value)

    return " \n".join(parts)


def award_matches_keywords(record: Dict[str, Any]) -> bool:
    """Return True if any GIS keyword is found in the award text."""

    return text_matches_keywords(extract_award_text(record), KEYWORDS)


def get_award_id(record: Dict[str, Any]) -> str:
    """Best-effort extraction of a stable award ID."""

    for key in ("contract_id", "award_id", "id"):
        value = record.get(key)
        if value is not None:
            return str(value)
    return ""


def get_award_last_update(record: Dict[str, Any]) -> str:
    """Return a last-update marker for an award record."""

    for key in (
        "updated_date",
        "last_update_date",
        "award_date",
        "contract_sign_date",
    ):
        value = record.get(key)
        if isinstance(value, str):
            return value
    return ""


def get_award_url(record: Dict[str, Any]) -> str:
    """Return a URL for the award if present, or a generic fallback."""

    for key in ("url", "contract_url", "link"):
        value = record.get(key)
        if isinstance(value, str) and value:
            return value

    # Generic contract awards info page as fallback.
    return (
        "https://financesone.worldbank.org/contract-awards-in-"
        "investment-project-financing-(since-fy-2020)/DS01666"
    )


def format_award_amount(record: Dict[str, Any]) -> str:
    """Return a readable award amount if present."""

    for key in (
        "contract_amount_usd",
        "contract_value_usd",
        "contract_value",
        "amount",
    ):
        value = record.get(key)
        if isinstance(value, (int, float)):
            return f"${value:,.2f}"
        if isinstance(value, str) and value:
            return f"${value}"
    return "N/A"


def get_award_supplier_name(record: Dict[str, Any]) -> str:
    """Return the supplier / contractor name if present."""

    for key in ("supplier_name", "supplier", "contractor_name", "vendor_name"):
        value = record.get(key)
        if isinstance(value, str) and value:
            return value
    return "(Unknown supplier)"


def send_award_alert_embed(record: Dict[str, Any]) -> bool:
    """Send a Discord embed for a new GIS-related contract award."""

    if not DISCORD_WEBHOOK_URL or "REPLACE_ME" in DISCORD_WEBHOOK_URL:
        LOGGER.warning("Webhook URL not configured; skipping award alert.")
        return False

    award_id = get_award_id(record) or "(unknown)"

    title = (
        record.get("contract_title")
        or record.get("description")
        or "(Untitled contract award)"
    )

    supplier = get_award_supplier_name(record)
    amount = format_award_amount(record)

    award_date = (
        record.get("award_date")
        or record.get("contract_sign_date")
        or record.get("date")
        or "N/A"
    )

    url = get_award_url(record)

    embed = {
        "title": f"Competitor Alert: {supplier}",
        "url": url,
        "color": 0xC0392B,  # red
        "fields": [
            {"name": "Contract ID", "value": award_id, "inline": True},
            {"name": "Supplier", "value": supplier, "inline": True},
            {"name": "Award Amount", "value": amount, "inline": True},
            {"name": "Award Date", "value": award_date, "inline": True},
            {
                "name": "Contract / Award Details",
                "value": f"[Open Award]({url})",
                "inline": False,
            },
            {
                "name": "Keywords",
                "value": "GIS / spatial-related contract award in Nigeria",
                "inline": False,
            },
        ],
        "footer": {
            "text": "World Bank GIS Monitor (Awards, Nigeria)",
        },
    }

    payload = {
        "username": "World Bank GIS Monitor",
        "embeds": [embed],
    }

    response = post_with_retries(
        DISCORD_WEBHOOK_URL,
        payload=payload,
        headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
    )
    if response is None:
        LOGGER.error("Award alert webhook failed after retries.")
        return False

    if 200 <= response.status_code < 300:
        LOGGER.info("Sent award alert for %s.", award_id)
        return True

    LOGGER.error(
        "Award alert webhook returned %s: %s",
        response.status_code,
        response.text,
    )
    return False


# ---------------------------------------------------------------------------
# Heartbeat / health check
# ---------------------------------------------------------------------------


def send_discord_heartbeat(
    total_projects: int,
    total_docs: int,
    total_tenders: int,
    total_awards: int,
    project_alerts: int,
    document_alerts: int,
    tender_alerts: int,
    award_alerts: int,
) -> bool:
    """Send a periodic heartbeat message so we know the monitor is alive."""

    if not DISCORD_WEBHOOK_URL or "REPLACE_ME" in DISCORD_WEBHOOK_URL:
        LOGGER.warning("Webhook URL not configured; skipping heartbeat.")
        return False

    description = "System healthy: daily scan for GIS opportunities in Nigeria."

    embed = {
        "title": "World Bank GIS Monitor Heartbeat",
        "description": description,
        "color": 0x3498DB,  # blue
        "fields": [
            {
                "name": "Projects scanned",
                "value": str(total_projects),
                "inline": True,
            },
            {
                "name": "Procurement plans scanned",
                "value": str(total_docs),
                "inline": True,
            },
            {
                "name": "Tenders scanned",
                "value": str(total_tenders),
                "inline": True,
            },
            {
                "name": "Awards scanned",
                "value": str(total_awards),
                "inline": True,
            },
            {
                "name": "Project alerts this run",
                "value": str(project_alerts),
                "inline": True,
            },
            {
                "name": "Procurement plan alerts this run",
                "value": str(document_alerts),
                "inline": True,
            },
            {
                "name": "Tender alerts this run",
                "value": str(tender_alerts),
                "inline": True,
            },
            {
                "name": "Award alerts this run",
                "value": str(award_alerts),
                "inline": True,
            },
        ],
        "footer": {
            "text": "Heartbeat status from World Bank GIS Monitor (Nigeria)",
        },
    }

    payload = {
        "username": "World Bank GIS Monitor",
        "embeds": [embed],
    }

    response = post_with_retries(
        DISCORD_WEBHOOK_URL,
        payload=payload,
        headers={"User-Agent": "WB-GIS-Monitor-Agent/1.0"},
    )
    if response is None:
        LOGGER.error("Heartbeat webhook failed after retries.")
        return False

    if 200 <= response.status_code < 300:
        LOGGER.info("Sent heartbeat message.")
        return True

    LOGGER.error(
        "Heartbeat webhook returned %s: %s",
        response.status_code,
        response.text,
    )
    return False


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_monitor() -> None:
    """Main orchestration: run all three data streams and send alerts."""

    LOGGER.info("Loading state files...")
    projects_state = _load_state_map(PROJECTS_STATE_FILE)
    tenders_state = _load_state_map(TENDERS_STATE_FILE)
    awards_state = _load_state_map(AWARDS_STATE_FILE)
    monitor_state = load_monitor_state()

    LOGGER.info(
        "Loaded state: %d projects, %d tenders, %d awards.",
        len(projects_state),
        len(tenders_state),
        len(awards_state),
    )

    total_projects = 0
    total_docs = 0
    total_tenders = 0
    total_awards = 0
    project_alerts = 0
    document_alerts = 0
    tender_alerts = 0
    award_alerts = 0

    # -------------------------
    # Stream 1: Projects
    # -------------------------

    LOGGER.info("Fetching active projects for Nigeria...")
    projects = fetch_projects_for_nigeria()
    total_projects = len(projects)
    LOGGER.info("Projects: %d records.", total_projects)

    matching_projects: List[Dict[str, Any]] = [
        p for p in projects if project_matches_keywords(p)
    ]
    LOGGER.info("Projects: %d GIS-related projects.", len(matching_projects))

    for project in matching_projects:
        project_id = str(project.get("id") or "").strip()
        if not project_id:
            continue

        current_update = get_project_last_update(project)
        previous_update = projects_state.get(project_id)

        if previous_update is not None and previous_update == current_update:
            continue

        is_update = previous_update is not None and previous_update != current_update
        if is_update:
            LOGGER.info(
                "Project %s updated (was %s, now %s).",
                project_id,
                previous_update,
                current_update,
            )
        else:
            LOGGER.info("New project %s detected.", project_id)

        sent = send_project_alert_embed(project, is_update=is_update)
        if sent:
            projects_state[project_id] = current_update
            project_alerts += 1

    _save_state_map(PROJECTS_STATE_FILE, projects_state)
    LOGGER.info(
        "Saved %d project entries to %s.",
        len(projects_state),
        PROJECTS_STATE_FILE,
    )

    # -------------------------
    # Stream 2: Procurement Plan Documents (WDS)
    # -------------------------
    LOGGER.info("Fetching procurement plan documents for Nigeria (WDS)...")
    docs = fetch_procurement_plans_for_nigeria()
    total_docs = len(docs)
    LOGGER.info("Procurement plans: %d records.", total_docs)

    matching_docs: List[Dict[str, Any]] = [
        d for d in docs if document_matches_keywords(d)
    ]
    LOGGER.info(
        "Procurement plans: %d GIS-related documents.",
        len(matching_docs),
    )

    docs_state = _load_state_map(DOCS_STATE_FILE)

    for doc in matching_docs:
        doc_id = get_document_id(doc)
        if not doc_id:
            continue

        current_update = get_document_last_update(doc)
        previous_update = docs_state.get(doc_id)

        if previous_update is not None and previous_update == current_update:
            continue

        is_update = previous_update is not None and previous_update != current_update
        LOGGER.info(
            "New/updated procurement plan %s detected (was %s, now %s).",
            doc_id,
            previous_update,
            current_update,
        )

        sent = send_procurement_plan_alert_embed(doc, is_update=is_update)
        if sent:
            docs_state[doc_id] = current_update
            document_alerts += 1

    _save_state_map(DOCS_STATE_FILE, docs_state)
    LOGGER.info(
        "Saved %d procurement plan entries to %s.",
        len(docs_state),
        DOCS_STATE_FILE,
    )

    # -------------------------
    # Stream 3: Tenders / Notices (Finances One)
    # -------------------------

    if ENABLE_TENDERS_STREAM:
        LOGGER.info("Fetching procurement notices / tenders for Nigeria...")
        tenders = fetch_tenders_for_nigeria()
        total_tenders = len(tenders)
        LOGGER.info("Tenders: %d records.", total_tenders)

        matching_tenders: List[Dict[str, Any]] = [
            t for t in tenders if tender_matches_keywords(t)
        ]
        LOGGER.info("Tenders: %d GIS-related notices.", len(matching_tenders))

        for tender in matching_tenders:
            tender_id = get_tender_id(tender)
            if not tender_id:
                continue

            current_update = get_tender_last_update(tender)
            previous_update = tenders_state.get(tender_id)

            if previous_update is not None and previous_update == current_update:
                continue

            LOGGER.info(
                "New/updated tender %s detected (was %s, now %s).",
                tender_id,
                previous_update,
                current_update,
            )

            sent = send_tender_alert_embed(tender)
            if sent:
                tenders_state[tender_id] = current_update
                tender_alerts += 1

        _save_state_map(TENDERS_STATE_FILE, tenders_state)
        LOGGER.info(
            "Saved %d tender entries to %s.",
            len(tenders_state),
            TENDERS_STATE_FILE,
        )
    else:
        LOGGER.info("Tenders stream is disabled; skipping Finances One tenders.")

    # -------------------------
    # Stream 4: Contract Awards (Finances One)
    # -------------------------

    if ENABLE_AWARDS_STREAM:
        LOGGER.info("Fetching contract awards for Nigeria...")
        awards = fetch_awards_for_nigeria()
        total_awards = len(awards)
        LOGGER.info("Awards: %d records.", total_awards)

        matching_awards: List[Dict[str, Any]] = [
            a for a in awards if award_matches_keywords(a)
        ]
        LOGGER.info("Awards: %d GIS-related contract awards.", len(matching_awards))

        for award in matching_awards:
            award_id = get_award_id(award)
            if not award_id:
                continue

            current_update = get_award_last_update(award)
            previous_update = awards_state.get(award_id)

            if previous_update is not None and previous_update == current_update:
                continue

            LOGGER.info(
                "New/updated award %s detected (was %s, now %s).",
                award_id,
                previous_update,
                current_update,
            )

            sent = send_award_alert_embed(award)
            if sent:
                awards_state[award_id] = current_update
                award_alerts += 1

        _save_state_map(AWARDS_STATE_FILE, awards_state)
        LOGGER.info(
            "Saved %d award entries to %s.",
            len(awards_state),
            AWARDS_STATE_FILE,
        )
    else:
        LOGGER.info("Awards stream is disabled; skipping Finances One awards.")

    # -------------------------
    # Weekly heartbeat (dead-man's switch)
    # -------------------------

    today = date.today().isoformat()
    last_heartbeat = monitor_state.get("last_heartbeat_date", "")

    # Monday (weekday=0) is the default heartbeat day; adjust if you prefer.
    if date.today().weekday() == 0 and last_heartbeat != today:
        sent = send_discord_heartbeat(
            total_projects=total_projects,
            total_docs=total_docs,
            total_tenders=total_tenders,
            total_awards=total_awards,
            project_alerts=project_alerts,
            document_alerts=document_alerts,
            tender_alerts=tender_alerts,
            award_alerts=award_alerts,
        )
        if sent:
            monitor_state["last_heartbeat_date"] = today
            save_monitor_state(monitor_state)

    LOGGER.info(
        "Run complete. Alerts this run -> projects: %d, tenders: %d, awards: %d.",
        project_alerts,
        tender_alerts,
        award_alerts,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    try:
        run_monitor()
    except Exception as exc:  # pragma: no cover - safety net
        # Catch-all safeguard so a single unexpected error does not kill
        # the scheduled task permanently.
        LOGGER.exception("Unhandled exception in monitor_agent: %s", exc)
