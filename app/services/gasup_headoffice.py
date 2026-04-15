"""
GasUp HeadOffice Wrapper — Automated Report Downloader

Connects to GasUp HeadOffice web portal, authenticates, navigates the
report menu, and downloads Excel reports programmatically. Feeds them
to the recaudo engine via /api/gasup/reporte or directly to the connector.

Architecture:
  HeadOffice (web) → this wrapper → Excel bytes → gasup_connector.ingest_excel_report()
                                                → OR POST /api/gasup/reporte (remote)

The portal is a classic server-rendered app (not SPA):
  - Cookie-based session (JSESSIONID or similar)
  - Form submissions via POST with CSRF-like fields
  - Breadcrumb navigation: CRM >> Reportes >> Reportes de ventas >> ...
  - "Seleccionar Estación" popup → date range → Buscar → Export Excel

Report paths (from REPORTES UTILIZADOS ACTUALMENTE GASDATA.docx):
  Reportes > Reportes de ventas > Ventas                    (resumen por forma de pago)
  Reportes > Reportes de ventas > Ventas detalladas tipo    (por tipo servicio)
  Reportes > Reportes de ventas > Ventas detalladas         (por medio de pago) ← PRIORITY
  Reportes > Reportes de ventas > Ventas por turno          (por turno/promotor)
  Reportes > Reportes de ventas > Conciliaciones            (cierre diario) ← PRIORITY
  Reportes > Reportes de ventas > Ventas por posición       (por manguera)
  Reportes > Reportes de ventas > Ventas por medio de pago  (consolidado)
  Reportes > Reportes de ventas > Ventas por forma de pago  (consolidado litros)
  Reportes > Reportes de ventas > Ventas anuladas           (fraude)
  Reportes > Reportes de ventas > Ventas cambio medio pago  (fraude)
  Reportes > Reportes de clientes > Fidelización            (puntos, bonos, movimientos)
  Reportes > Reportes de recaudos > Recaudos                ← PRIORITY (sobreprecio)
  Reportes > Reportes de recaudos > Cartera
  Reportes > Reportes de recaudos > Abonos

Usage:
  wrapper = GasUpHeadOffice(base_url, username, password)
  wrapper.login()
  excel_bytes = wrapper.download_report(
      report_type="ventas_detalladas",
      estacion_id="ECG-01",
      fecha_inicio="2026-04-01",
      fecha_fin="2026-04-13",
  )
  # Feed to connector
  connector.ingest_excel_report(excel_bytes)

Config via env vars:
  GASUP_HO_URL        = https://headoffice.gasup.co  (or whatever the actual URL is)
  GASUP_HO_USER       = admin_centralgas
  GASUP_HO_PASSWORD   = ***
  GASUP_API_URL       = https://cmu-decision.fly.dev  (our API, for remote mode)
  GASUP_WEBHOOK_SECRET = ***  (for authenticating to our own API)
"""

import os
import logging
import time
import hashlib
import json
import base64
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Tuple
from enum import Enum
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from .gasup_connector import GasUpConnector
except ImportError:
    GasUpConnector = None

logger = logging.getLogger("gasup_headoffice")


# ===== REPORT TYPE REGISTRY =====

class ReportCategory(Enum):
    VENTAS = "ventas"
    CLIENTES = "clientes"
    FIDELIZACION = "fidelizacion"
    RECAUDOS = "recaudos"
    REVISION = "revision"


@dataclass
class ReportDef:
    """Definition of a HeadOffice report type."""
    name: str                     # Internal name (matches gasup-webhook.ts tipos)
    category: ReportCategory
    menu_path: List[str]          # Navigation breadcrumb in HeadOffice
    description: str
    priority: int                 # 1=critical, 2=high, 3=medium, 4=low
    has_station_filter: bool = True
    has_date_filter: bool = True
    export_format: str = "xls"    # GasUp exports .xls (OLE2), not .xlsx
    # Selectors/form fields (to be mapped when we have portal access)
    menu_id: Optional[str] = None      # DOM id of menu link
    form_action: Optional[str] = None  # POST action URL
    export_button_id: Optional[str] = None


# Registry of all known reports with navigation paths
REPORT_REGISTRY: Dict[str, ReportDef] = {
    # === VENTAS (Priority reports) ===
    "ventas_detalladas": ReportDef(
        name="ventas_detalladas",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas detalladas"],
        description="Ventas transaccionales por medio de pago — placa, litros, precio, fecha",
        priority=1,
    ),
    "conciliacion_diaria": ReportDef(
        name="conciliacion_diaria",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Conciliaciones"],
        description="Conciliación diaria — resumen por forma de pago y totales",
        priority=1,
    ),
    "recaudos_financiera": ReportDef(
        name="recaudos_financiera",
        category=ReportCategory.RECAUDOS,
        menu_path=["Reportes", "Reportes de recaudos", "Recaudos"],
        description="Recaudos por financiera — sobreprecio acumulado por placa",
        priority=1,
    ),

    # === VENTAS (Secondary) ===
    "ventas_resumen": ReportDef(
        name="ventas_resumen",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas"],
        description="Resumen de ventas en litros y $ por forma de pago",
        priority=2,
    ),
    "ventas_tipo_servicio": ReportDef(
        name="ventas_tipo_servicio",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas detallada por tipo de servicio"],
        description="Ventas transaccionales clasificadas por tipo de servicio del vehículo",
        priority=3,
    ),
    "ventas_turno": ReportDef(
        name="ventas_turno",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas por turno"],
        description="Ventas por turno y promotor",
        priority=3,
    ),
    "ventas_posicion": ReportDef(
        name="ventas_posicion",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas por posición"],
        description="Ventas por posición de manguera/dispensador",
        priority=4,
    ),
    "ventas_medio_pago": ReportDef(
        name="ventas_medio_pago",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas por medio de pago"],
        description="Ventas consolidadas por medio de pago",
        priority=3,
    ),
    "ventas_forma_pago": ReportDef(
        name="ventas_forma_pago",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas por forma de pago"],
        description="Ventas consolidadas por forma de pago en litros",
        priority=4,
    ),
    "ventas_anuladas": ReportDef(
        name="ventas_anuladas",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas anuladas"],
        description="Historial de ventas anuladas — alerta de fraude",
        priority=2,
    ),
    "cambio_medio_pago": ReportDef(
        name="cambio_medio_pago",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas cambio medio de pago"],
        description="Cambios de medio de pago post-venta — alerta de fraude",
        priority=2,
    ),
    "ventas_facturadas": ReportDef(
        name="ventas_facturadas",
        category=ReportCategory.VENTAS,
        menu_path=["Reportes", "Reportes de ventas", "Ventas facturadas"],
        description="Ventas con número de factura",
        priority=3,
    ),

    # === CLIENTES / FIDELIZACIÓN ===
    "fidelizacion_movimientos": ReportDef(
        name="fidelizacion_movimientos",
        category=ReportCategory.FIDELIZACION,
        menu_path=["Reportes", "Reportes de clientes", "Fidelización", "Movimientos"],
        description="Movimientos de puntos de fidelización",
        priority=4,
    ),
    "fidelizacion_puntos": ReportDef(
        name="fidelizacion_puntos",
        category=ReportCategory.FIDELIZACION,
        menu_path=["Reportes", "Reportes de clientes", "Fidelización", "Puntos"],
        description="Puntos acumulados por placa/tarjeta",
        priority=4,
    ),
    "fidelizacion_bonos": ReportDef(
        name="fidelizacion_bonos",
        category=ReportCategory.FIDELIZACION,
        menu_path=["Reportes", "Reportes de clientes", "Fidelización", "Bonos"],
        description="Redención de bonos de fidelización",
        priority=4,
    ),

    # === RECAUDOS ===
    "recaudos_cliente": ReportDef(
        name="recaudos_cliente",
        category=ReportCategory.RECAUDOS,
        menu_path=["Reportes", "Reportes de recaudos", "Cartera de clientes"],
        description="Cartera y consumo de crédito por cliente",
        priority=2,
    ),
    "abonos": ReportDef(
        name="abonos",
        category=ReportCategory.RECAUDOS,
        menu_path=["Reportes", "Reportes de recaudos", "Abonos"],
        description="Abonos y prepago registrados",
        priority=3,
    ),

    # === REVISIÓN ===
    "revision_anual": ReportDef(
        name="revision_anual",
        category=ReportCategory.REVISION,
        menu_path=["Reportes", "Reportes de revisión anual"],
        description="Fechas de revisión técnico-mecánica por vehículo",
        priority=4,
        has_date_filter=False,
    ),
}


# ===== STATION MAPPING =====
# Maps our internal station IDs to HeadOffice station codes/names
# These will be populated when we get portal access or demo credentials

STATION_MAP: Dict[str, Dict[str, str]] = {
    "ECG-01": {"ho_code": "", "ho_name": "Parques Industriales", "ho_select_value": ""},
    "ECG-02": {"ho_code": "", "ho_name": "Oriente", "ho_select_value": ""},
    "ECG-03": {"ho_code": "", "ho_name": "Pensión/Nacozari", "ho_select_value": ""},
}


# ===== HEADOFFICE SESSION =====

@dataclass
class HeadOfficeSession:
    """Manages authenticated session with HeadOffice portal."""
    base_url: str
    username: str
    password: str
    session: Any = field(default=None, repr=False)
    authenticated: bool = False
    last_activity: Optional[datetime] = None
    session_timeout_minutes: int = 30
    _csrf_token: Optional[str] = None

    def __post_init__(self):
        if not HAS_REQUESTS:
            raise ImportError("requests and beautifulsoup4 required: pip install requests beautifulsoup4")
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "CentralGas-Wrapper/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
        })

    def login(self) -> bool:
        """Authenticate with HeadOffice portal."""
        try:
            # Step 1: GET login page to capture form fields / CSRF token
            login_url = f"{self.base_url}/login"
            resp = self.session.get(login_url, timeout=15)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            form = soup.find("form")

            # Extract hidden fields (CSRF, viewstate, etc.)
            hidden_fields = {}
            if form:
                for inp in form.find_all("input", {"type": "hidden"}):
                    name = inp.get("name", "")
                    value = inp.get("value", "")
                    if name:
                        hidden_fields[name] = value

            # Step 2: POST credentials
            login_data = {
                **hidden_fields,
                "username": self.username,
                "password": self.password,
                # Common field names in Java web apps:
                "j_username": self.username,
                "j_password": self.password,
                "login": self.username,
                "clave": self.password,
            }

            # Try common login form actions
            form_action = form.get("action", "/j_security_check") if form else "/j_security_check"
            if not form_action.startswith("http"):
                form_action = f"{self.base_url}{form_action}"

            resp = self.session.post(form_action, data=login_data, timeout=15, allow_redirects=True)

            # Check if login succeeded (redirected to home/dashboard, not back to login)
            if "/login" in resp.url or "error" in resp.url.lower():
                logger.error(f"Login failed — redirected to: {resp.url}")
                self.authenticated = False
                return False

            # Look for session indicators
            soup = BeautifulSoup(resp.text, "html.parser")
            if soup.find(string=lambda t: t and "Cerrar sesión" in t):
                logger.info("Login successful — 'Cerrar sesión' found")
                self.authenticated = True
            elif soup.find("a", {"class": "menu"}) or soup.find("div", {"id": "menu"}):
                logger.info("Login successful — menu structure found")
                self.authenticated = True
            else:
                # Optimistic: if we got cookies and no error, assume success
                if self.session.cookies:
                    logger.info(f"Login appears successful — {len(self.session.cookies)} cookies set")
                    self.authenticated = True
                else:
                    logger.warning("Login result uncertain — no cookies, no menu found")
                    self.authenticated = False

            self.last_activity = datetime.now()
            return self.authenticated

        except requests.RequestException as e:
            logger.error(f"Login request failed: {e}")
            self.authenticated = False
            return False

    def ensure_session(self) -> bool:
        """Re-login if session expired."""
        if not self.authenticated:
            return self.login()

        if self.last_activity:
            elapsed = (datetime.now() - self.last_activity).total_seconds() / 60
            if elapsed > self.session_timeout_minutes - 5:  # 5 min buffer
                logger.info(f"Session likely expired ({elapsed:.0f}m), re-authenticating")
                return self.login()

        return True

    def get(self, path: str, **kwargs) -> requests.Response:
        """GET request with session management."""
        self.ensure_session()
        url = f"{self.base_url}{path}" if not path.startswith("http") else path
        resp = self.session.get(url, timeout=30, **kwargs)
        self.last_activity = datetime.now()
        return resp

    def post(self, path: str, data: dict = None, **kwargs) -> requests.Response:
        """POST request with session management."""
        self.ensure_session()
        url = f"{self.base_url}{path}" if not path.startswith("http") else path
        resp = self.session.post(url, data=data, timeout=30, **kwargs)
        self.last_activity = datetime.now()
        return resp


# ===== REPORT DOWNLOADER =====

@dataclass
class DownloadResult:
    """Result of a report download attempt."""
    success: bool
    report_type: str
    estacion_id: Optional[str]
    fecha_inicio: Optional[str]
    fecha_fin: Optional[str]
    file_bytes: Optional[bytes] = None
    filename: Optional[str] = None
    file_hash: Optional[str] = None
    rows_estimate: int = 0
    error: Optional[str] = None
    duration_seconds: float = 0.0


class GasUpHeadOffice:
    """
    Wrapper around GasUp HeadOffice web portal.
    Automates login, navigation, and report download.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_url: Optional[str] = None,
        api_secret: Optional[str] = None,
        download_dir: Optional[str] = None,
    ):
        self.base_url = (base_url or os.getenv("GASUP_HO_URL", "")).rstrip("/")
        self.username = username or os.getenv("GASUP_HO_USER", "")
        self.password = password or os.getenv("GASUP_HO_PASSWORD", "")
        self.api_url = (api_url or os.getenv("GASUP_API_URL", "")).rstrip("/")
        self.api_secret = api_secret or os.getenv("GASUP_WEBHOOK_SECRET", "")
        self.download_dir = download_dir or os.getenv(
            "GASUP_DOWNLOAD_DIR",
            str(Path.home() / "gasup_reports")
        )

        self._session: Optional[HeadOfficeSession] = None
        self._connector: Optional[Any] = None  # GasUpConnector instance
        self._download_history: List[DownloadResult] = []
        self._file_hashes: set = set()  # Dedup downloaded files

        # Create download dir
        Path(self.download_dir).mkdir(parents=True, exist_ok=True)

    @property
    def session(self) -> HeadOfficeSession:
        if self._session is None:
            if not self.base_url or not self.username:
                raise ValueError(
                    "HeadOffice credentials not configured. Set GASUP_HO_URL, "
                    "GASUP_HO_USER, GASUP_HO_PASSWORD env vars."
                )
            self._session = HeadOfficeSession(
                base_url=self.base_url,
                username=self.username,
                password=self.password,
            )
        return self._session

    @property
    def connector(self):
        if self._connector is None and GasUpConnector is not None:
            self._connector = GasUpConnector()
        return self._connector

    def login(self) -> bool:
        """Authenticate with HeadOffice."""
        return self.session.login()

    def is_configured(self) -> bool:
        """Check if HeadOffice credentials are set."""
        return bool(self.base_url and self.username and self.password)

    # ===== REPORT DISCOVERY =====

    def list_reports(self, priority_max: int = 4) -> List[ReportDef]:
        """List available report types, filtered by priority."""
        return sorted(
            [r for r in REPORT_REGISTRY.values() if r.priority <= priority_max],
            key=lambda r: (r.priority, r.name),
        )

    def get_priority_reports(self) -> List[ReportDef]:
        """Get the 3 critical reports (priority=1)."""
        return self.list_reports(priority_max=1)

    # ===== NAVIGATION =====

    def _navigate_to_report(self, report_def: ReportDef) -> Optional[str]:
        """
        Navigate HeadOffice menu to reach the report page.
        Returns the URL of the report page, or None on failure.

        Strategy:
        1. GET the home/dashboard page
        2. Find and click menu links matching the breadcrumb path
        3. Return final page URL

        NOTE: Menu IDs and form actions need to be mapped with actual
        portal access. The current implementation uses href-based navigation
        following the breadcrumb pattern seen in the manual screenshots.
        """
        try:
            # Start from home
            resp = self.session.get("/")
            soup = BeautifulSoup(resp.text, "html.parser")

            current_url = resp.url

            # Follow menu path: Reportes > Reportes de ventas > Ventas detalladas
            for menu_item in report_def.menu_path:
                # Find link by text content
                link = soup.find("a", string=lambda t: t and menu_item.lower() in t.lower())

                if not link:
                    # Try partial match in all links
                    for a in soup.find_all("a"):
                        text = a.get_text(strip=True)
                        if menu_item.lower() in text.lower():
                            link = a
                            break

                if not link:
                    logger.warning(f"Menu item '{menu_item}' not found in page")
                    # Try clicking the menu section to expand it
                    continue

                href = link.get("href", "")
                if href and href != "#":
                    resp = self.session.get(href)
                    soup = BeautifulSoup(resp.text, "html.parser")
                    current_url = resp.url
                elif link.get("onclick"):
                    # JavaScript navigation — extract URL from onclick
                    onclick = link["onclick"]
                    # Common patterns: window.location='url', document.location.href='url'
                    for pattern in ["'", '"']:
                        if pattern in onclick:
                            parts = onclick.split(pattern)
                            if len(parts) >= 2:
                                candidate = parts[1]
                                if "/" in candidate:
                                    resp = self.session.get(candidate)
                                    soup = BeautifulSoup(resp.text, "html.parser")
                                    current_url = resp.url
                                    break

            return current_url

        except Exception as e:
            logger.error(f"Navigation failed for {report_def.name}: {e}")
            return None

    def _select_station(self, soup: BeautifulSoup, estacion_id: str) -> Optional[BeautifulSoup]:
        """
        Handle the 'Seleccionar Estación' popup/form.
        Returns updated soup after station selection.
        """
        station_info = STATION_MAP.get(estacion_id)
        if not station_info:
            logger.error(f"Unknown station: {estacion_id}")
            return None

        # Look for station selection form/popup
        # Pattern from manual: button "Seleccionar Estación" → popup with station list → click ✓
        select_btn = soup.find("button", string=lambda t: t and "Seleccionar" in t and "Estación" in t)
        if not select_btn:
            select_btn = soup.find("input", {"value": lambda v: v and "Seleccionar" in v})

        # Find station in the list by name or code
        station_name = station_info["ho_name"]
        station_row = soup.find("td", string=lambda t: t and station_name.lower() in t.lower())
        if station_row:
            # Find the select/checkmark icon in the same row
            row = station_row.find_parent("tr")
            if row:
                select_link = row.find("a") or row.find("input", {"type": "image"})
                if select_link:
                    href = select_link.get("href", select_link.get("onclick", ""))
                    if href:
                        resp = self.session.get(href if href.startswith("/") else f"/{href}")
                        return BeautifulSoup(resp.text, "html.parser")

        # Fallback: POST with station value
        form = soup.find("form")
        if form:
            form_data = {}
            for inp in form.find_all("input"):
                name = inp.get("name", "")
                value = inp.get("value", "")
                if name:
                    form_data[name] = value
            # Set station selector
            for select in form.find_all("select"):
                sname = select.get("name", "")
                if "estacion" in sname.lower() or "eds" in sname.lower():
                    form_data[sname] = station_info.get("ho_select_value", "")

            action = form.get("action", "")
            resp = self.session.post(action, data=form_data)
            return BeautifulSoup(resp.text, "html.parser")

        return None

    def _set_date_range(self, soup: BeautifulSoup, fecha_inicio: str, fecha_fin: str) -> Dict[str, str]:
        """
        Extract form fields and set date range.
        Returns dict of form fields to POST.
        """
        form_data = {}
        form = soup.find("form")
        if not form:
            return form_data

        # Collect all form fields
        for inp in form.find_all(["input", "select", "textarea"]):
            name = inp.get("name", "")
            if not name:
                continue
            if inp.name == "select":
                selected = inp.find("option", selected=True)
                form_data[name] = selected["value"] if selected else ""
            else:
                form_data[name] = inp.get("value", "")

        # Set date fields — common names in GasData
        date_field_patterns = {
            "inicio": fecha_inicio,
            "desde": fecha_inicio,
            "start": fecha_inicio,
            "fecha_ini": fecha_inicio,
            "fin": fecha_fin,
            "hasta": fecha_fin,
            "end": fecha_fin,
            "fecha_fin": fecha_fin,
        }
        for fname, fvalue in form_data.items():
            for pattern, date_value in date_field_patterns.items():
                if pattern in fname.lower():
                    # Convert YYYY-MM-DD to DD/MM/YYYY (Colombian date format in HeadOffice)
                    try:
                        d = datetime.strptime(date_value, "%Y-%m-%d")
                        form_data[fname] = d.strftime("%d/%m/%Y")
                    except ValueError:
                        form_data[fname] = date_value

        return form_data

    def _find_export_button(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Find the Excel export button/link on the report page.
        Returns the URL or form action for export.
        """
        # Look for common export patterns
        for text_pattern in ["Excel", "Exportar", "Export", "Descargar", "XLS"]:
            link = soup.find("a", string=lambda t: t and text_pattern.lower() in t.lower())
            if link:
                return link.get("href", "")

            btn = soup.find("input", {"value": lambda v: v and text_pattern.lower() in v.lower()})
            if btn:
                form = btn.find_parent("form")
                return form.get("action", "") if form else ""

            img = soup.find("img", {"alt": lambda a: a and text_pattern.lower() in a.lower()})
            if img:
                parent_link = img.find_parent("a")
                if parent_link:
                    return parent_link.get("href", "")

        return None

    # ===== DOWNLOAD =====

    def download_report(
        self,
        report_type: str,
        estacion_id: Optional[str] = None,
        fecha_inicio: Optional[str] = None,
        fecha_fin: Optional[str] = None,
        save_to_disk: bool = True,
    ) -> DownloadResult:
        """
        Download a specific report from HeadOffice.

        Args:
            report_type: Key from REPORT_REGISTRY (e.g., "ventas_detalladas")
            estacion_id: Station ID ("ECG-01", "ECG-02", "ECG-03") or None for all
            fecha_inicio: Start date YYYY-MM-DD (default: yesterday)
            fecha_fin: End date YYYY-MM-DD (default: today)
            save_to_disk: Save downloaded file to download_dir

        Returns:
            DownloadResult with file_bytes if successful
        """
        start_time = time.time()

        # Defaults
        if not fecha_fin:
            fecha_fin = date.today().isoformat()
        if not fecha_inicio:
            fecha_inicio = (date.today() - timedelta(days=1)).isoformat()

        report_def = REPORT_REGISTRY.get(report_type)
        if not report_def:
            return DownloadResult(
                success=False, report_type=report_type,
                estacion_id=estacion_id, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
                error=f"Unknown report type: {report_type}. Available: {list(REPORT_REGISTRY.keys())}",
            )

        try:
            # 1. Navigate to report page
            report_url = self._navigate_to_report(report_def)
            if not report_url:
                return DownloadResult(
                    success=False, report_type=report_type,
                    estacion_id=estacion_id, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
                    error="Failed to navigate to report page",
                    duration_seconds=time.time() - start_time,
                )

            resp = self.session.get(report_url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # 2. Select station (if applicable)
            if report_def.has_station_filter and estacion_id:
                station_soup = self._select_station(soup, estacion_id)
                if station_soup:
                    soup = station_soup

            # 3. Set date range and submit form
            if report_def.has_date_filter:
                form_data = self._set_date_range(soup, fecha_inicio, fecha_fin)
                if form_data:
                    form = soup.find("form")
                    action = form.get("action", report_url) if form else report_url
                    resp = self.session.post(action, data=form_data)
                    soup = BeautifulSoup(resp.text, "html.parser")

            # 4. Find and click export button
            export_url = self._find_export_button(soup)
            if not export_url:
                # Some reports auto-download on form submit, or the data is in an iframe
                logger.warning(f"No export button found for {report_type}, attempting direct download")
                # Try common export URL patterns
                for pattern in [
                    f"/reportes/export/{report_type}",
                    f"/export/excel/{report_type}",
                    f"/reportes/{report_type}/excel",
                ]:
                    try:
                        resp = self.session.get(pattern)
                        if resp.status_code == 200 and "spreadsheet" in resp.headers.get("Content-Type", ""):
                            break
                    except Exception:
                        continue
            else:
                resp = self.session.get(export_url)

            # 5. Validate response is an Excel file
            content_type = resp.headers.get("Content-Type", "")
            is_excel = any(t in content_type for t in [
                "spreadsheet", "excel", "octet-stream", "vnd.ms-excel",
                "vnd.openxmlformats",
            ])

            if not is_excel and len(resp.content) < 1000:
                return DownloadResult(
                    success=False, report_type=report_type,
                    estacion_id=estacion_id, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
                    error=f"Response doesn't look like Excel: Content-Type={content_type}, size={len(resp.content)}",
                    duration_seconds=time.time() - start_time,
                )

            file_bytes = resp.content
            file_hash = hashlib.sha256(file_bytes).hexdigest()[:16]

            # Dedup
            if file_hash in self._file_hashes:
                return DownloadResult(
                    success=True, report_type=report_type,
                    estacion_id=estacion_id, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
                    file_hash=file_hash,
                    error="duplicate",
                    duration_seconds=time.time() - start_time,
                )
            self._file_hashes.add(file_hash)

            # Generate filename
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            station_suffix = f"_{estacion_id}" if estacion_id else "_ALL"
            # GasUp exports .xls — detect from content
            is_xls = file_bytes[:4] == b'\xd0\xcf\x11\xe0'
            ext = ".xls" if is_xls else ".xlsx"
            filename = f"{report_type}{station_suffix}_{fecha_inicio}_{fecha_fin}_{ts}{ext}"

            # Save to disk
            if save_to_disk:
                filepath = Path(self.download_dir) / filename
                filepath.write_bytes(file_bytes)
                logger.info(f"Saved: {filepath} ({len(file_bytes)} bytes)")

            result = DownloadResult(
                success=True, report_type=report_type,
                estacion_id=estacion_id, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
                file_bytes=file_bytes, filename=filename, file_hash=file_hash,
                duration_seconds=time.time() - start_time,
            )
            self._download_history.append(result)
            return result

        except Exception as e:
            logger.error(f"Download failed for {report_type}: {e}")
            return DownloadResult(
                success=False, report_type=report_type,
                estacion_id=estacion_id, fecha_inicio=fecha_inicio, fecha_fin=fecha_fin,
                error=str(e),
                duration_seconds=time.time() - start_time,
            )

    # ===== FEED TO ENGINE =====

    def feed_to_connector(self, result: DownloadResult) -> dict:
        """Feed downloaded report to the local GasUpConnector."""
        if not result.success or not result.file_bytes:
            return {"error": "No file to feed", "report": result.report_type}

        if self.connector is None:
            return {"error": "GasUpConnector not available"}

        # GasUp exports .xls (not .xlsx) — save with correct extension
        import tempfile
        # Detect format: .xls files start with bytes D0 CF 11 E0 (OLE2)
        is_xls = result.file_bytes[:4] == b'\xd0\xcf\x11\xe0'
        suffix = ".xls" if is_xls else ".xlsx"

        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
            f.write(result.file_bytes)
            temp_path = f.name

        try:
            count = self.connector.ingest_excel_report(
                Path(temp_path),
                report_type=result.report_type,
                estacion_id=result.estacion_id or "",
            )
            return {
                "success": True,
                "report_type": result.report_type,
                "format": suffix,
                "transactions_ingested": count,
                "file_hash": result.file_hash,
            }
        finally:
            os.unlink(temp_path)

    def feed_to_api(self, result: DownloadResult) -> dict:
        """Send downloaded report to our remote API (/api/gasup/reporte)."""
        if not result.success or not result.file_bytes:
            return {"error": "No file to feed", "report": result.report_type}

        if not self.api_url:
            return {"error": "GASUP_API_URL not configured"}

        payload = {
            "tipo": result.report_type,
            "fileBase64": base64.b64encode(result.file_bytes).decode("utf-8"),
            "filename": result.filename,
            "estacion_id": result.estacion_id,
            "periodo_inicio": result.fecha_inicio,
            "periodo_fin": result.fecha_fin,
        }

        try:
            resp = requests.post(
                f"{self.api_url}/api/gasup/reporte",
                json=payload,
                headers={"Authorization": f"Bearer {self.api_secret}"},
                timeout=60,
            )
            return resp.json()
        except Exception as e:
            return {"error": str(e)}

    # ===== BATCH / SCHEDULED OPERATIONS =====

    def pull_daily_reports(
        self,
        fecha: Optional[str] = None,
        estaciones: Optional[List[str]] = None,
        feed_mode: str = "connector",  # "connector" | "api" | "disk_only"
    ) -> List[dict]:
        """
        Pull all priority reports for a given day across all stations.
        This is the main entry point for scheduled/cron execution.

        Args:
            fecha: Date to pull (YYYY-MM-DD, default: yesterday)
            estaciones: List of station IDs (default: all 3)
            feed_mode: Where to send the data
        """
        if not fecha:
            fecha = (date.today() - timedelta(days=1)).isoformat()
        if not estaciones:
            estaciones = list(STATION_MAP.keys())

        results = []
        priority_reports = self.get_priority_reports()

        logger.info(f"Starting daily pull: {fecha} | {len(estaciones)} stations | {len(priority_reports)} reports")

        for report_def in priority_reports:
            for estacion_id in estaciones:
                logger.info(f"  Downloading: {report_def.name} / {estacion_id} / {fecha}")
                dl = self.download_report(
                    report_type=report_def.name,
                    estacion_id=estacion_id,
                    fecha_inicio=fecha,
                    fecha_fin=fecha,
                )

                feed_result = {"download": {
                    "success": dl.success,
                    "report_type": dl.report_type,
                    "estacion": estacion_id,
                    "fecha": fecha,
                    "error": dl.error,
                    "duration": f"{dl.duration_seconds:.1f}s",
                }}

                if dl.success and dl.file_bytes and dl.error != "duplicate":
                    if feed_mode == "connector":
                        feed_result["feed"] = self.feed_to_connector(dl)
                    elif feed_mode == "api":
                        feed_result["feed"] = self.feed_to_api(dl)
                    else:
                        feed_result["feed"] = {"mode": "disk_only", "path": f"{self.download_dir}/{dl.filename}"}

                results.append(feed_result)

        logger.info(f"Daily pull complete: {len(results)} downloads, "
                     f"{sum(1 for r in results if r['download']['success'])} successful")
        return results

    def pull_fraud_reports(
        self,
        fecha_inicio: Optional[str] = None,
        fecha_fin: Optional[str] = None,
    ) -> List[DownloadResult]:
        """Pull fraud-detection reports (anuladas + cambio medio pago)."""
        if not fecha_fin:
            fecha_fin = date.today().isoformat()
        if not fecha_inicio:
            fecha_inicio = (date.today() - timedelta(days=7)).isoformat()

        results = []
        for report_type in ["ventas_anuladas", "cambio_medio_pago"]:
            for estacion_id in STATION_MAP:
                dl = self.download_report(
                    report_type=report_type,
                    estacion_id=estacion_id,
                    fecha_inicio=fecha_inicio,
                    fecha_fin=fecha_fin,
                )
                results.append(dl)
        return results

    # ===== STATUS =====

    def stats(self) -> dict:
        """Status and statistics."""
        return {
            "configured": self.is_configured(),
            "authenticated": self._session.authenticated if self._session else False,
            "base_url": self.base_url or "(not set)",
            "stations": list(STATION_MAP.keys()),
            "reports_available": len(REPORT_REGISTRY),
            "priority_reports": [r.name for r in self.get_priority_reports()],
            "downloads_total": len(self._download_history),
            "downloads_successful": sum(1 for d in self._download_history if d.success),
            "files_in_dedup": len(self._file_hashes),
            "download_dir": self.download_dir,
        }


# ===== CLI ENTRY POINT =====

def main():
    """CLI for testing the wrapper."""
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    parser = argparse.ArgumentParser(description="GasUp HeadOffice Wrapper")
    parser.add_argument("command", choices=["status", "login", "pull", "download", "list"])
    parser.add_argument("--report", "-r", help="Report type to download")
    parser.add_argument("--station", "-s", help="Station ID (ECG-01, ECG-02, ECG-03)")
    parser.add_argument("--date", "-d", help="Date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--date-end", help="End date YYYY-MM-DD")
    parser.add_argument("--feed", choices=["connector", "api", "disk_only"], default="disk_only")
    args = parser.parse_args()

    wrapper = GasUpHeadOffice()

    if args.command == "status":
        print(json.dumps(wrapper.stats(), indent=2))

    elif args.command == "list":
        for r in wrapper.list_reports():
            stars = "★" * (5 - r.priority)
            print(f"  {stars:4s}  {r.name:30s}  {' > '.join(r.menu_path)}")

    elif args.command == "login":
        ok = wrapper.login()
        print(f"Login: {'OK' if ok else 'FAILED'}")

    elif args.command == "download":
        if not args.report:
            print("ERROR: --report required")
            return
        result = wrapper.download_report(
            report_type=args.report,
            estacion_id=args.station,
            fecha_inicio=args.date,
            fecha_fin=args.date_end,
        )
        print(json.dumps({
            "success": result.success,
            "filename": result.filename,
            "size": len(result.file_bytes) if result.file_bytes else 0,
            "hash": result.file_hash,
            "error": result.error,
            "duration": f"{result.duration_seconds:.1f}s",
        }, indent=2))

    elif args.command == "pull":
        results = wrapper.pull_daily_reports(
            fecha=args.date,
            estaciones=[args.station] if args.station else None,
            feed_mode=args.feed,
        )
        for r in results:
            dl = r["download"]
            status = "✓" if dl["success"] else "✗"
            print(f"  {status} {dl['report_type']:30s} {dl['estacion']:8s} {dl.get('error', 'OK')}")


if __name__ == "__main__":
    main()
