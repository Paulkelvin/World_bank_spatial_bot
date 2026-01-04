"""World Bank GIS Opportunities Monitor for Nigeria.

This script fetches active World Bank projects, filters those related to
GIS / geospatial topics for Nigeria (country code NG), and sends a summary
of new matches to a Discord channel via a webhook.

Designed to be run daily, e.g., from PythonAnywhere scheduled tasks.
"""

import json
import logging
import os
from datetime import date
from time import sleep
from typing import Dict, Iterable, List

import requests
from urllib.parse import quote_plus


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Logger for this module.
LOGGER = logging.getLogger("wb_gis_monitor")

# Replace this with your real Discord webhook URL before running. You can
# also override it at runtime using the WB_DISCORD_WEBHOOK_URL environment
# variable, which is convenient for PythonAnywhere.
ENV_WEBHOOK_URL = os.getenv("WB_DISCORD_WEBHOOK_URL")
DISCORD_WEBHOOK_URL = (
    ENV_WEBHOOK_URL
    if ENV_WEBHOOK_URL
    else "https://discordapp.com/api/webhooks/REPLACE_ME"
)

# Local JSON file to persist which project IDs have already been processed.
PROCESSED_IDS_FILE = "processed_ids.json"

# Local JSON file to track monitor state (e.g. last heartbeat date).
STATE_FILE = "monitor_state.json"

# World Bank Projects API endpoint.
WB_PROJECTS_API_URL = "https://search.worldbank.org/api/v2/projects"

# Keywords to search for in project titles and descriptions.
# These are intentionally broad to catch most GIS / spatial / environmental
# analytics work across sectors (land, environment, climate, health, etc.).
KEYWORDS = [
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

    # Urban / transport planning
    "Urban Spatial Planning",
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

# Extra terms to enrich the contractor Google search so that the
# results are more likely to be GIS / geospatial / environmental / health
# analytics implementers and consulting firms.
CONTRACTOR_SEARCH_TERMS = [
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


def load_monitor_state(path: str = STATE_FILE) -> Dict[str, str]:
    """Load monitor state (e.g. last heartbeat date) from disk.

    Returns an empty dict on first run or on any read/parse error.
    """

    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            # Ensure keys/values are strings for simplicity.
            return {str(k): str(v) for k, v in data.items()}
    except (OSError, json.JSONDecodeError):
        return {}

    return {}


def save_monitor_state(state: Dict[str, str], path: str = STATE_FILE) -> None:
    """Persist monitor state to disk."""

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except OSError as exc:
        LOGGER.error("Failed to save monitor state: %s", exc)

# HTTP timeout (seconds) for all outbound requests.
HTTP_TIMEOUT = 15

# Simple retry configuration for HTTP calls.
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3


def get_with_retries(
    url: str,
    *,
    params: Dict | None = None,
    headers: Dict | None = None,
    timeout: int = HTTP_TIMEOUT,
    max_retries: int = MAX_RETRIES,
) -> requests.Response | None:
    """Perform a GET request with basic retry logic.

    Returns the last successful ``requests.Response`` or ``None`` if all
    attempts fail.
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
        except requests.RequestException as exc:
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
    payload: Dict,
    headers: Dict | None = None,
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
        except requests.RequestException as exc:
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


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def load_processed_projects(path: str = PROCESSED_IDS_FILE) -> Dict[str, str]:
    """Load processed projects from disk.

    The modern format is a mapping of ``project_id -> last_update_string``.
    For backward compatibility, if the file contains a simple list of
    project IDs, they are loaded with an empty last-update value.
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


def save_processed_projects(
    projects: Dict[str, str], path: str = PROCESSED_IDS_FILE
) -> None:
    """Persist processed project IDs and last-update markers to disk."""

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(projects, f, indent=2)
    except OSError as exc:
        LOGGER.error("Failed to save processed projects: %s", exc)


# ---------------------------------------------------------------------------
# World Bank API integration
# ---------------------------------------------------------------------------


def fetch_active_projects_for_nigeria(rows_per_page: int = 50) -> List[Dict]:
    """Fetch all active World Bank projects for Nigeria (NG).

    The World Bank search API does not always filter perfectly by country
    via query parameters, so this function additionally filters by
    `countrycode` on the client side.
    """

    all_projects: List[Dict] = []
    page = 1

    while True:
        params = {
            "format": "json",
            "status": "Active",
            "rows": rows_per_page,
            "page": page,
        }

        response = get_with_retries(
            WB_PROJECTS_API_URL,
            params=params,
            headers={"User-Agent": "WB-GIS-Monitor/1.0"},
        )
        if response is None:
            LOGGER.error("Failed to fetch page %d from World Bank API.", page)
            break

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            LOGGER.error(
                "World Bank API responded with error on page %d: %s",
                page,
                exc,
            )
            break

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            LOGGER.error("Failed to parse JSON from World Bank API: %s", exc)
            break

        # Basic pagination info.
        total = int(payload.get("total", 0))
        rows = int(payload.get("rows", rows_per_page))

        projects_dict = payload.get("projects") or {}
        if not isinstance(projects_dict, dict):
            LOGGER.warning("Unexpected 'projects' format from API.")
            break

        # Filter to projects where Nigeria (NG) is in the country code list.
        for project in projects_dict.values():
            country_codes = project.get("countrycode") or []
            if not isinstance(country_codes, list):
                continue
            if "NG" in country_codes:
                all_projects.append(project)

        LOGGER.info(
            "Retrieved page %d: %d projects (total=%d).",
            page,
            len(projects_dict),
            total,
        )

        # Stop when we've fetched all known results.
        if page * rows >= total or not projects_dict:
            break

        page += 1

    return all_projects


# ---------------------------------------------------------------------------
# Keyword filtering and formatting helpers
# ---------------------------------------------------------------------------


def extract_project_text(project: Dict) -> str:
    """Concatenate title and description-ish fields into a single text blob."""

    parts: List[str] = []

    title = project.get("project_name") or ""
    if isinstance(title, str):
        parts.append(title)

    # The API often has a nested `project_abstract` with a `cdata` field.
    abstract = project.get("project_abstract")
    if isinstance(abstract, dict):
        for value in abstract.values():
            if isinstance(value, str):
                parts.append(value)

    return " \n".join(parts)


def project_matches_keywords(project: Dict, keywords: Iterable[str]) -> bool:
    """Return True if any keyword appears in the project's text.

    The check is case-insensitive and searches both title and description.
    """

    text = extract_project_text(project)
    if not text:
        return False

    lower_text = text.lower()
    for kw in keywords:
        if kw.lower() in lower_text:
            return True
    return False


def format_approval_date(project: Dict) -> str:
    """Extract a human-friendly approval date from the project payload."""

    raw_date = project.get("boardapprovaldate")
    if isinstance(raw_date, str) and raw_date:
        # Common API format is ISO8601; we simply trim to date portion.
        return raw_date.split("T", 1)[0]
    return "N/A"


def get_project_last_update(project: Dict) -> str:
    """Return a string marker for when the project was last updated.

    Prefers ``p2a_updated_date`` if present, and falls back to an empty
    string. The exact string is stored and compared; if it changes between
    runs, the project is treated as "updated" for alerting purposes.
    """

    updated = project.get("p2a_updated_date") or ""
    if isinstance(updated, str):
        return updated
    return ""


def format_total_amount(project: Dict) -> str:
    """Extract the total funding amount as a string.

    Prefers `totalamt`, falling back to `totalcommamt`.
    """

    amount = project.get("totalamt") or project.get("totalcommamt")
    if not isinstance(amount, str):
        return "N/A"

    # The API tends to return values like "200,000,000".
    return f"${amount}"


def get_project_url(project: Dict) -> str:
    """Return the World Bank project detail URL."""

    url = project.get("url")
    if isinstance(url, str) and url:
        return url

    project_id = str(project.get("id") or "").strip()
    if project_id:
        return (
            "https://projects.worldbank.org/en/projects-operations/"
            f"project-detail/{project_id}"
        )

    return "https://projects.worldbank.org/en/projects-operations/projects-overview"


def build_contractor_search_url(project_name: str) -> str:
    """Construct a Google search URL to discover potential contractors.

    Logic (extended):
    https://www.google.com/search?q=
      "World+Bank"+"[Project Name]"+<GIS terms>+"contractor"
    """

    # Construct the raw query with quotes around terms, then URL-encode it.
    terms: List[str] = [f'"World Bank"', f'"{project_name}"']
    for term in CONTRACTOR_SEARCH_TERMS:
        terms.append(f'"{term}"')
    terms.append('"contractor"')

    query = "+".join(terms)
    encoded_query = quote_plus(query)
    return f"https://www.google.com/search?q={encoded_query}"


def send_discord_heartbeat(
    total_projects: int,
    matching_projects: int,
    new_or_updated_projects: int,
) -> bool:
    """Send a periodic heartbeat message so we know the monitor is alive."""

    if not DISCORD_WEBHOOK_URL or "REPLACE_ME" in DISCORD_WEBHOOK_URL:
        LOGGER.warning(
            "DISCORD_WEBHOOK_URL is not configured; skipping heartbeat.",
        )
        return False

    description = (
        "System healthy: scanned projects for GIS opportunities in Nigeria."
    )

    embed = {
        "title": "World Bank GIS Monitor Heartbeat",
        "description": description,
        "color": 0x3498DB,  # Blue.
        "fields": [
            {
                "name": "Total Projects Scanned",
                "value": str(total_projects),
                "inline": True,
            },
            {
                "name": "GIS-Related Projects Found",
                "value": str(matching_projects),
                "inline": True,
            },
            {
                "name": "New/Updated Alerts This Run",
                "value": str(new_or_updated_projects),
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
        headers={"User-Agent": "WB-GIS-Monitor/1.0"},
    )
    if response is None:
        LOGGER.error("Discord heartbeat request failed after retries.")
        return False

    if 200 <= response.status_code < 300:
        LOGGER.info("Sent Discord heartbeat message.")
        return True

    LOGGER.error(
        "Discord heartbeat returned status %s: %s",
        response.status_code,
        response.text,
    )
    return False


# ---------------------------------------------------------------------------
# Discord webhook integration
# ---------------------------------------------------------------------------


def send_discord_embed(project: Dict) -> bool:
    """Send a single project summary as a Discord embed.

    Returns True if the request appears to succeed, False otherwise.
    """

    if not DISCORD_WEBHOOK_URL or "REPLACE_ME" in DISCORD_WEBHOOK_URL:
        LOGGER.warning(
            "DISCORD_WEBHOOK_URL is not configured; skipping send.",
        )
        return False

    project_name = project.get("project_name") or "(No title)"
    project_id = str(project.get("id") or "").strip() or "(unknown)"

    approval_date = format_approval_date(project)
    total_amount = format_total_amount(project)
    project_url = get_project_url(project)
    contractor_url = build_contractor_search_url(str(project_name))

    embed = {
        "title": project_name,
        "url": project_url,
        "color": 0x1ABC9C,  # Teal-ish color.
        "fields": [
            {
                "name": "Project ID",
                "value": project_id,
                "inline": True,
            },
            {
                "name": "Approval Date",
                "value": approval_date,
                "inline": True,
            },
            {
                "name": "Total Funding Amount",
                "value": total_amount,
                "inline": True,
            },
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
            "text": "World Bank GIS Monitor (Nigeria)",
        },
    }

    payload = {
        "username": "World Bank GIS Monitor",
        "embeds": [embed],
    }

    response = post_with_retries(
        DISCORD_WEBHOOK_URL,
        payload=payload,
        headers={"User-Agent": "WB-GIS-Monitor/1.0"},
    )
    if response is None:
        LOGGER.error(
            "Discord webhook request failed for project %s after retries.",
            project_id,
        )
        return False

    if 200 <= response.status_code < 300:
        LOGGER.info("Sent Discord notification for project %s.", project_id)
        return True

    LOGGER.error(
        "Discord webhook returned status %s: %s",
        response.status_code,
        response.text,
    )

    return False


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------


def run_monitor() -> None:
    """Main orchestration for fetching, filtering, and notifying."""

    LOGGER.info("Loading processed project metadata...")
    processed_projects = load_processed_projects()
    LOGGER.info("Loaded %d processed projects.", len(processed_projects))

    LOGGER.info("Loading monitor state (for heartbeat)...")
    state = load_monitor_state()

    LOGGER.info("Fetching active Nigeria projects from World Bank API...")
    projects = fetch_active_projects_for_nigeria()
    total_projects = len(projects)
    LOGGER.info("Retrieved %d Nigeria projects (Active).", total_projects)

    if not projects:
        LOGGER.info("No active projects returned; nothing to do.")

        # Even if there are no projects, consider sending a heartbeat
        # so you still know the monitor is alive.
        today = date.today().isoformat()
        last_heartbeat = state.get("last_heartbeat_date", "")
        if date.today().weekday() == 0 and last_heartbeat != today:
            sent = send_discord_heartbeat(0, 0, 0)
            if sent:
                state["last_heartbeat_date"] = today
                save_monitor_state(state)
        return

    # Filter for GIS-related projects.
    matching_projects: List[Dict] = []
    for project in projects:
        if project_matches_keywords(project, KEYWORDS):
            matching_projects.append(project)

    matching_count = len(matching_projects)
    LOGGER.info(
        "Found %d GIS-related projects in Nigeria.",
        matching_count,
    )

    if not matching_projects:
        LOGGER.info("No GIS-related projects found today.")

        # Still optionally send a heartbeat to confirm the system is alive.
        today = date.today().isoformat()
        last_heartbeat = state.get("last_heartbeat_date", "")
        if date.today().weekday() == 0 and last_heartbeat != today:
            sent = send_discord_heartbeat(total_projects, 0, 0)
            if sent:
                state["last_heartbeat_date"] = today
                save_monitor_state(state)
        return

    # Send notifications only for projects that are new or have a changed
    # last-update marker since the last run.
    new_or_updated_count = 0

    for project in matching_projects:
        project_id = str(project.get("id") or "").strip()
        if not project_id:
            continue

        current_update = get_project_last_update(project)
        previous_update = processed_projects.get(project_id)

        if previous_update is not None and previous_update == current_update:
            # Already seen this version of the project.
            continue

        if previous_update is None:
            LOGGER.info("Sending notification for NEW project %s...", project_id)
        else:
            LOGGER.info(
                "Sending notification for UPDATED project %s (was %s, now %s)...",
                project_id,
                previous_update,
                current_update,
            )

        sent = send_discord_embed(project)
        if sent:
            processed_projects[project_id] = current_update
            new_or_updated_count += 1

    LOGGER.info(
        "Completed notifications; %d new/updated projects were notified.",
        new_or_updated_count,
    )

    # Persist updated project state.
    save_processed_projects(processed_projects)
    LOGGER.info(
        "Saved %d processed projects to %s.",
        len(processed_projects),
        PROCESSED_IDS_FILE,
    )

    # Weekly heartbeat (e.g. every Monday) so you know the monitor is alive
    # even if there are zero new projects on some days.
    today = date.today().isoformat()
    last_heartbeat = state.get("last_heartbeat_date", "")
    if date.today().weekday() == 0 and last_heartbeat != today:
        sent = send_discord_heartbeat(total_projects, matching_count, new_or_updated_count)
        if sent:
            state["last_heartbeat_date"] = today
            save_monitor_state(state)


if __name__ == "__main__":
    # Basic logging configuration suitable for CLI/scheduled runs.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    # Allow running as a standalone script.
    try:
        run_monitor()
    except Exception as exc:  # noqa: BLE001
        # Catch-all safeguard so a single unexpected error does not
        # permanently break the scheduled task.
        LOGGER.exception("Unhandled exception in monitor: %s", exc)
