"""ERCT (Exam Resilience Control Tower) - Desktop Demo GUI.

Visual control tower for operational resilience and audit control plane.
Runs on tkinter (ttk + Canvas) and httpx only, communicating with ERCT API over HTTP.
Never opens SQLite directly and never imports from app/ or simulator/.
"""
from __future__ import annotations

import json
import os
import queue
import random
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import tkinter as tk
from tkinter import ttk

import httpx

# ---------------------------------------------------------------------------
# LOGGING CONFIGURATION
# ---------------------------------------------------------------------------
LOG_DIR = Path("data")
LOG_FILE = LOG_DIR / "gui.log"


def log_ascii(msg: str, exc: Optional[Exception] = None) -> None:
    """Safely log ASCII-only diagnostic messages and exceptions to data/gui.log."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        clean_msg = msg.encode("ascii", errors="replace").decode("ascii")
        with open(LOG_FILE, "a", encoding="ascii", errors="replace") as f:
            f.write(f"[{ts}] {clean_msg}\n")
            if exc is not None:
                tb = traceback.format_exc().encode("ascii", errors="replace").decode("ascii")
                f.write(f"{tb}\n")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# API CLIENT & EXCEPTION
# ---------------------------------------------------------------------------
class ApiException(Exception):
    """Clean API exception carrying HTTP status code and server detail message."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(f"HTTP {status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail


class ApiClient:
    """Wraps httpx.Client with base URL from env ERCT_API, timeout 4.0s (configurable via env ERCT_TIMEOUT)."""

    def __init__(self, base_url: Optional[str] = None, timeout: Optional[float] = None):
        self.base_url = (base_url or os.environ.get("ERCT_API", "http://127.0.0.1:8000")).rstrip("/")
        t_val = timeout if timeout is not None else float(os.environ.get("ERCT_TIMEOUT", "4.0"))
        self.client = httpx.Client(base_url=self.base_url, timeout=t_val)

    def _handle_response(self, response: httpx.Response) -> Any:
        if response.status_code >= 400:
            detail = response.text
            try:
                err_json = response.json()
                if isinstance(err_json, dict) and "detail" in err_json:
                    det = err_json["detail"]
                    if isinstance(det, dict) and "message" in det:
                        detail = str(det["message"])
                    else:
                        detail = str(det)
            except Exception:
                pass
            raise ApiException(response.status_code, detail)
        return response.json()

    def get(self, path: str) -> Any:
        url = path if path.startswith("/") else f"/{path}"
        resp = self.client.get(url)
        return self._handle_response(resp)

    def post(self, path: str, json: Any = None, headers: Optional[Dict[str, str]] = None) -> Any:
        url = path if path.startswith("/") else f"/{path}"
        resp = self.client.post(url, json=json, headers=headers)
        return self._handle_response(resp)

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# APP STATE
# ---------------------------------------------------------------------------
class AppState:
    """Central observable state store for the GUI control tower."""

    def __init__(self):
        self.health: Optional[Dict[str, Any]] = None
        self.centres: List[Dict[str, Any]] = []
        self.incidents: List[Dict[str, Any]] = []
        self.readiness: Optional[Dict[str, Any]] = None
        self.fairness: Optional[Dict[str, Any]] = None

        # Per-centre sessions list: centre_id -> List[session_dict]
        self.centre_sessions: Dict[str, List[Dict[str, Any]]] = {}

        self.api_ok: bool = False
        self.api_status: str = "DOWN"
        self.last_successful_poll_ts: float = 0.0
        self.last_error: Optional[str] = None

        self.events_count: int = 0
        self.previous_events_count: int = 0
        self.events_rate: float = 0.0
        self.last_events_ts: float = 0.0

        self.audit_ok: bool = True
        self.audit_total_entries: int = 0

        self.selected_incident: Optional[str] = None
        self.selected_candidate: Optional[str] = None
        self.selected_tab: str = "Incidents"
        self.incident_impacts: Dict[str, Dict[str, Any]] = {}
        self.decision_events: List[str] = []

    def handle_poll_result(self, name: str, data_or_error: Any, ts: float) -> None:
        """Process incoming poll data or record an error."""
        if isinstance(data_or_error, Exception):
            self.last_error = str(data_or_error)
            if self.last_successful_poll_ts == 0.0 or (time.time() - self.last_successful_poll_ts > 4.0):
                self.api_ok = False
                self.api_status = "DOWN"
            return

        data = data_or_error
        self.api_ok = True
        self.last_successful_poll_ts = ts
        self.last_error = None

        if name == "health":
            self.health = data
            raw_status = str(data.get("status", "healthy")).upper()
            self.api_status = raw_status if raw_status in ("HEALTHY", "DEGRADED") else "HEALTHY"
            curr_events = data.get("events_count", 0)
            if self.last_events_ts > 0 and ts > self.last_events_ts:
                dt = ts - self.last_events_ts
                if dt > 0:
                    delta = max(0, curr_events - self.previous_events_count)
                    self.events_rate = round(delta / dt, 1)
            self.previous_events_count = self.events_count
            self.events_count = curr_events
            self.last_events_ts = ts
            self.audit_ok = bool(data.get("audit_chain_ok", True))
            self.audit_total_entries = data.get("total_audit_entries", 0)

        elif name == "centres":
            if isinstance(data, list):
                self.centres = data

        elif name == "centre_sessions":
            if isinstance(data, dict):
                cid = data.get("centre_id")
                sessions = data.get("sessions")
                if cid and isinstance(sessions, list):
                    self.centre_sessions[cid] = sessions

        elif name == "incidents":
            if isinstance(data, list):
                self.incidents = data
                if not self.selected_incident and self.incidents:
                    self.selected_incident = self.incidents[0].get("incident_id")

        elif name == "incident_impact":
            if isinstance(data, dict):
                inc_id = data.get("incident_id")
                imp_data = data.get("data")
                if inc_id and isinstance(imp_data, dict):
                    prev_summary = (self.incident_impacts.get(inc_id) or {}).get("summary") or {}
                    curr_summary = imp_data.get("summary") or {}
                    prev_dec = prev_summary.get("decided", 0)
                    curr_dec = curr_summary.get("decided", 0)
                    if curr_dec > prev_dec:
                        msg = f"approved: {curr_dec} remedies by controller.sharma"
                        if msg not in self.decision_events:
                            self.decision_events.insert(0, msg)
                            if len(self.decision_events) > 5:
                                self.decision_events.pop()
                    self.incident_impacts[inc_id] = imp_data

        elif name == "readiness":
            if isinstance(data, dict):
                self.readiness = data

        elif name == "fairness":
            if isinstance(data, dict):
                self.fairness = data


# ---------------------------------------------------------------------------
# POLLER (BACKGROUND DAEMON THREAD)
# ---------------------------------------------------------------------------
class Poller(threading.Thread):
    """Fetches API endpoints at defined intervals and pushes results to a thread-safe Queue."""

    def __init__(self, api_client: ApiClient, out_queue: queue.Queue, state: AppState):
        super().__init__(daemon=True, name="ERCT-PollerThread")
        self.client = api_client
        self.queue = out_queue
        self.state = state
        self.stop_event = threading.Event()

        self.centre_ids = ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"]
        self.sessions_rot_idx = 0
        self.last_sessions_poll = 0.0
        self.last_impact_poll = 0.0

        # Endpoint configurations: (name, path, interval_seconds)
        self.endpoints: List[Tuple[str, str, float]] = [
            ("health", "/v1/health", 2.0),
            ("centres", "/v1/centres", 1.0),
            ("incidents", "/v1/incidents", 1.0),
            ("readiness", "/v1/readiness", 30.0),
            ("fairness", "/v1/fairness", 5.0),
        ]
        self.last_polled: Dict[str, float] = {name: 0.0 for name, _, _ in self.endpoints}

    def run(self) -> None:
        """Poll loop. Never crashes on HTTP or connection failures."""
        while not self.stop_event.is_set():
            now = time.time()

            # 1. Main endpoints
            for name, path, interval in self.endpoints:
                if self.stop_event.is_set():
                    break
                if now - self.last_polled[name] >= interval:
                    self.last_polled[name] = now
                    try:
                        data = self.client.get(path)
                        self.queue.put((name, data, time.time()))
                    except Exception as err:
                        self.queue.put((name, err, time.time()))

            # 2. Centre sessions rotation: 1 centre every 0.5s (full cycle ~2.5s)
            if not self.stop_event.is_set() and (now - self.last_sessions_poll >= 0.5):
                self.last_sessions_poll = now
                cid = self.centre_ids[self.sessions_rot_idx]
                self.sessions_rot_idx = (self.sessions_rot_idx + 1) % len(self.centre_ids)
                try:
                    data = self.client.get(f"/v1/centres/{cid}/sessions")
                    self.queue.put(("centre_sessions", {"centre_id": cid, "sessions": data}, time.time()))
                except Exception as err:
                    self.queue.put(("centre_sessions", {"centre_id": cid, "error": err}, time.time()))

            # 3. Selected Incident Impact polling (every 2.5s while tab is Impact or incident is resolved)
            if not self.stop_event.is_set() and self.state.selected_incident:
                if now - self.last_impact_poll >= 2.5:
                    self.last_impact_poll = now
                    inc_id = self.state.selected_incident
                    inc_obj = next((inc for inc in self.state.incidents if inc.get("incident_id") == inc_id), None)
                    is_resolved = (inc_obj.get("status") == "resolved") if inc_obj else False
                    is_impact_tab = (getattr(self.state, "selected_tab", "") == "Impact")
                    if is_resolved or is_impact_tab:
                        try:
                            data = self.client.get(f"/v1/incidents/{inc_id}/impact")
                            self.queue.put(("incident_impact", {"incident_id": inc_id, "data": data}, time.time()))
                        except Exception as err:
                            log_ascii(f"Error polling impact for {inc_id}", err)

            self.stop_event.wait(0.05)

    def stop(self) -> None:
        """Signal thread to terminate and close HTTP client resources."""
        self.stop_event.set()
        self.client.close()


# ---------------------------------------------------------------------------
# UI CONSTANTS & PALETTE
# ---------------------------------------------------------------------------
COLOR_BG = "#0b1426"
COLOR_PANEL = "#111d36"
COLOR_CARD = "#16254a"
COLOR_BORDER = "#243556"
COLOR_TEXT = "#e6edf7"
COLOR_MUTED = "#8fa3c4"
COLOR_TEAL = "#2dd4bf"
COLOR_GREEN = "#34d399"
COLOR_AMBER = "#fbbf24"
COLOR_RED = "#f87171"
COLOR_BLUE = "#60a5fa"

FONT_UI = ("Segoe UI", 10)
FONT_UI_BOLD = ("Segoe UI", 10, "bold")
FONT_UI_SMALL = ("Segoe UI", 9)
FONT_UI_TINY = ("Segoe UI", 8)
FONT_UI_TINY_BOLD = ("Segoe UI", 8, "bold")
FONT_UI_HEADING = ("Segoe UI", 12, "bold")
FONT_MONO = ("Consolas", 10)
FONT_MONO_SMALL = ("Consolas", 9)
FONT_MONO_TINY = ("Consolas", 8)


def apply_dark_theme(style: ttk.Style) -> None:
    """Configure ttk styling with dark control tower palette."""
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure(".", background=COLOR_BG, foreground=COLOR_TEXT, font=FONT_UI)
    style.configure("TFrame", background=COLOR_BG)
    style.configure("Panel.TFrame", background=COLOR_PANEL)
    style.configure("Card.TFrame", background=COLOR_CARD)
    style.configure("TLabel", background=COLOR_BG, foreground=COLOR_TEXT, font=FONT_UI)
    style.configure("Muted.TLabel", foreground=COLOR_MUTED)

    # Notebook Tabs
    style.configure("TNotebook", background=COLOR_BG, borderwidth=0)
    style.configure(
        "TNotebook.Tab",
        background=COLOR_CARD,
        foreground=COLOR_MUTED,
        padding=[14, 6],
        font=FONT_UI_BOLD,
        borderwidth=1,
        bordercolor=COLOR_BORDER,
    )
    style.map(
        "TNotebook.Tab",
        background=[("selected", COLOR_PANEL), ("active", "#1c2e56")],
        foreground=[("selected", COLOR_TEAL), ("active", COLOR_TEXT)],
        bordercolor=[("selected", COLOR_BORDER)],
    )

    # Treeview
    style.configure(
        "Treeview",
        background=COLOR_PANEL,
        foreground=COLOR_TEXT,
        fieldbackground=COLOR_PANEL,
        rowheight=24,
        font=FONT_UI_SMALL,
        borderwidth=0,
    )
    style.configure(
        "Treeview.Heading",
        background=COLOR_CARD,
        foreground=COLOR_MUTED,
        font=FONT_UI_BOLD,
        borderwidth=1,
        relief="flat",
    )
    style.map("Treeview", background=[("selected", "#1e3a5f")], foreground=[("selected", "#ffffff")])

    # Button
    style.configure(
        "TButton",
        background=COLOR_CARD,
        foreground=COLOR_TEXT,
        borderwidth=1,
        bordercolor=COLOR_BORDER,
        padding=[10, 4],
        font=FONT_UI_SMALL,
    )
    style.map(
        "TButton",
        background=[("pressed", "#0f172a"), ("active", COLOR_BORDER)],
        foreground=[("active", COLOR_TEAL)],
        bordercolor=[("active", COLOR_TEAL)],
    )

    # Entry & Combobox
    style.configure(
        "TEntry",
        fieldbackground=COLOR_CARD,
        foreground=COLOR_TEXT,
        insertcolor=COLOR_TEAL,
        borderwidth=1,
        bordercolor=COLOR_BORDER,
    )
    style.configure(
        "TCombobox",
        fieldbackground=COLOR_CARD,
        foreground=COLOR_TEXT,
        selectbackground=COLOR_CARD,
        selectforeground=COLOR_TEXT,
        borderwidth=1,
        bordercolor=COLOR_BORDER,
    )

    # Scrollbars
    style.configure(
        "Vertical.TScrollbar",
        background=COLOR_CARD,
        troughcolor=COLOR_BG,
        bordercolor=COLOR_BG,
        arrowcolor=COLOR_MUTED,
    )
    style.configure(
        "Horizontal.TScrollbar",
        background=COLOR_CARD,
        troughcolor=COLOR_BG,
        bordercolor=COLOR_BG,
        arrowcolor=COLOR_MUTED,
    )


# ---------------------------------------------------------------------------
# TOP BAR VIEW
# ---------------------------------------------------------------------------
class ChevronStrip(tk.Canvas):
    """Five-segment chevron progress strip: Prevention > Detection > Response > Recovery > Trust."""

    STAGES = ["Prevention", "Detection", "Recovery", "Response", "Trust"]

    def __init__(self, parent: tk.Widget, width: int = 440, height: int = 30):
        super().__init__(parent, width=width, height=height, bg=COLOR_BG, highlightthickness=0)
        self.active_stage = "Prevention"
        self.draw_chevrons()

    def set_active_stage(self, stage: str) -> None:
        if stage in self.STAGES and stage != self.active_stage:
            self.active_stage = stage
            self.draw_chevrons()

    def draw_chevrons(self) -> None:
        self.delete("all")
        seg_w = 84
        arrow_dx = 10
        y_top = 3
        y_bot = 27
        y_mid = 15

        for i, stage in enumerate(self.STAGES):
            x_start = i * (seg_w - 2) + 2
            x_end = x_start + seg_w

            if i == 0:
                coords = [
                    x_start, y_top,
                    x_end, y_top,
                    x_end + arrow_dx, y_mid,
                    x_end, y_bot,
                    x_start, y_bot,
                ]
            else:
                coords = [
                    x_start, y_top,
                    x_end, y_top,
                    x_end + arrow_dx, y_mid,
                    x_end, y_bot,
                    x_start, y_bot,
                    x_start + arrow_dx, y_mid,
                ]

            is_active = (stage == self.active_stage)
            fill_color = COLOR_TEAL if is_active else COLOR_CARD
            outline_color = COLOR_TEAL if is_active else COLOR_BORDER
            text_color = "#0b1426" if is_active else COLOR_MUTED
            text_font = ("Segoe UI", 8, "bold") if is_active else ("Segoe UI", 8)

            self.create_polygon(coords, fill=fill_color, outline=outline_color, width=1)
            text_x = x_start + (seg_w + arrow_dx) / 2
            self.create_text(text_x, y_mid, text=stage, fill=text_color, font=text_font)


class TopBar(tk.Frame):
    """Top bar containing brand title, chevron ribbon, and telemetry status pills."""

    def __init__(self, parent: tk.Widget):
        super().__init__(parent, bg=COLOR_BG, height=48, pady=6, padx=12)
        self.pack_propagate(False)

        # 1. Left Title
        self.title_frame = tk.Frame(self, bg=COLOR_BG)
        self.title_frame.pack(side=tk.LEFT, fill=tk.Y)
        self.title_label = tk.Label(
            self.title_frame,
            text="Exam Resilience Control Tower",
            font=("Segoe UI", 12, "bold"),
            fg=COLOR_TEXT,
            bg=COLOR_BG,
        )
        self.title_label.pack(anchor="w")

        # 2. Middle Chevron
        self.chevron = ChevronStrip(self, width=440, height=30)
        self.chevron.pack(side=tk.LEFT, expand=True)

        # 3. Right Pills Frame
        self.pills_frame = tk.Frame(self, bg=COLOR_BG)
        self.pills_frame.pack(side=tk.RIGHT, fill=tk.Y)

        # Simulation Pill
        self.sim_pill = tk.Label(
            self.pills_frame,
            text="SIMULATED exam environment",
            font=FONT_MONO_SMALL,
            fg=COLOR_AMBER,
            bg="#2a1f0a",
            padx=8,
            pady=3,
            relief="solid",
            borderwidth=1,
            highlightbackground=COLOR_AMBER,
        )
        self.sim_pill.pack(side=tk.RIGHT, padx=4)

        # Audit Pill
        self.audit_pill = tk.Label(
            self.pills_frame,
            text="audit: OK (0)",
            font=FONT_MONO_SMALL,
            fg=COLOR_GREEN,
            bg="#0d2b20",
            padx=8,
            pady=3,
            relief="solid",
            borderwidth=1,
        )
        self.audit_pill.pack(side=tk.RIGHT, padx=4)

        # Events Pill
        self.events_pill = tk.Label(
            self.pills_frame,
            text="events: 0",
            font=FONT_MONO_SMALL,
            fg=COLOR_TEXT,
            bg=COLOR_CARD,
            padx=8,
            pady=3,
            relief="solid",
            borderwidth=1,
        )
        self.events_pill.pack(side=tk.RIGHT, padx=4)

        # API State Pill
        self.api_pill = tk.Label(
            self.pills_frame,
            text="API: DOWN",
            font=FONT_MONO_SMALL,
            fg=COLOR_RED,
            bg="#361010",
            padx=8,
            pady=3,
            relief="solid",
            borderwidth=1,
        )
        self.api_pill.pack(side=tk.RIGHT, padx=4)

    def update_view(self, state: AppState) -> None:
        """Reflect latest state on all pills."""
        if state.api_ok:
            if state.api_status == "HEALTHY":
                self.api_pill.config(text="API: HEALTHY", fg=COLOR_GREEN, bg="#0d2b20")
            elif state.api_status == "DEGRADED":
                self.api_pill.config(text="API: DEGRADED", fg=COLOR_AMBER, bg="#2a1f0a")
            else:
                self.api_pill.config(text=f"API: {state.api_status}", fg=COLOR_GREEN, bg="#0d2b20")
        else:
            self.api_pill.config(text="API: DOWN", fg=COLOR_RED, bg="#361010")

        self.events_pill.config(text=f"events: {state.events_count}")

        if state.audit_ok:
            self.audit_pill.config(
                text=f"audit: OK ({state.audit_total_entries})",
                fg=COLOR_GREEN,
                bg="#0d2b20",
            )
        else:
            self.audit_pill.config(
                text="audit: BROKEN",
                fg=COLOR_RED,
                bg="#361010",
            )

        # Drive the top chevron from real state
        if not state.incidents:
            active_stage = "Prevention"
        elif any(inc.get("status") == "open" for inc in state.incidents):
            active_stage = "Detection"
        elif any(inc.get("status") == "recovering" for inc in state.incidents):
            active_stage = "Recovery"
        else:
            # Resolved incident(s) present: check pending decisions
            target_inc_id = state.selected_incident or (state.incidents[0].get("incident_id") if state.incidents else None)
            impact = state.incident_impacts.get(target_inc_id) if target_inc_id else None
            if impact and impact.get("summary"):
                pending = impact["summary"].get("pending_decisions", 0)
                active_stage = "Trust" if pending == 0 else "Response"
            else:
                active_stage = "Response"
        self.chevron.set_active_stage(active_stage)


# ---------------------------------------------------------------------------
# FACILITY SCENE (CANVAS DRAWING & REAL-TIME ANIMATION)
# ---------------------------------------------------------------------------
class FacilityScene:
    """Manages full architectural facility layout, items dictionary, and animation loop."""

    def __init__(self, canvas: tk.Canvas):
        self.canvas = canvas
        self.width = 900
        self.height = 520

        # State storage and tracking
        self.state: Optional[AppState] = None
        self.selected_candidate: Optional[str] = None
        self.selected_item_id: Optional[int] = None

        # Diagnostic metrics
        self.frame_times: List[float] = []
        self.avg_frame_ms: float = 0.0
        self.item_count: int = 0

        # Canvas item dictionaries
        self.server_items: Dict[str, Any] = {}
        self.grid_items: Dict[str, Any] = {}
        self.lab_items: Dict[str, Dict[str, Any]] = {}
        self.pcs: Dict[Tuple[str, int], Dict[str, Any]] = {}  # (cid, pc_idx) -> dict

        # Dynamic animation particles
        self.power_dots: List[Dict[str, Any]] = []
        self.network_links: Dict[str, Dict[str, Any]] = {}
        self.network_packets: Dict[str, List[Dict[str, Any]]] = {}

        # Interactive tooltip
        self.tooltip_rect: Optional[int] = None
        self.tooltip_text: Optional[int] = None
        self.hovered_target: Optional[Tuple[str, Any]] = None

    def build_scene(self, width: int, height: int) -> None:
        """Create every canvas item a single time and register item IDs."""
        self.width = max(900, width)
        self.height = max(520, height)
        self.canvas.delete("all")

        self.server_items.clear()
        self.grid_items.clear()
        self.lab_items.clear()
        self.pcs.clear()
        self.power_dots.clear()
        self.network_links.clear()
        self.network_packets.clear()

        # 1. Background Grid Pattern
        grid_step = 40
        for x in range(0, self.width, grid_step):
            self.canvas.create_line(x, 0, x, self.height, fill="#0d1830", width=1)
        for y in range(0, self.height, grid_step):
            self.canvas.create_line(0, y, self.width, y, fill="#0d1830", width=1)

        # 2. Server Room (Top Center)
        srv_w = 320
        srv_h = 84
        srv_x0 = (self.width - srv_w) // 2
        srv_x1 = srv_x0 + srv_w
        srv_y0 = 12
        srv_y1 = srv_y0 + srv_h

        # Rounded room panel
        srv_panel = self._create_rounded_rect(srv_x0, srv_y0, srv_x1, srv_y1, r=8, fill=COLOR_PANEL, outline=COLOR_BORDER, width=1.5)
        srv_title = self.canvas.create_text(
            srv_x0 + 12, srv_y0 + 12, text="ERCT API  |  Control Tower", font=("Segoe UI", 9, "bold"), fill=COLOR_TEAL, anchor="w"
        )

        # Server Racks (3 racks with 6 LED dots each = 18 LEDs)
        led_ids = []
        rack_w = 34
        rack_h = 46
        racks_start_x = srv_x0 + 18
        for r_idx in range(3):
            rx0 = racks_start_x + r_idx * (rack_w + 10)
            rx1 = rx0 + rack_w
            ry0 = srv_y0 + 26
            ry1 = ry0 + rack_h
            self.canvas.create_rectangle(rx0, ry0, rx1, ry1, fill="#0b1426", outline="#1c2d52", width=1)
            # Slot lines
            for slot_y in range(ry0 + 8, ry1 - 4, 8):
                self.canvas.create_line(rx0 + 2, slot_y, rx1 - 2, slot_y, fill="#12203d", width=1)
            # 6 LED dots (2 columns of 3)
            for col in range(2):
                for row in range(3):
                    lx = rx0 + 8 + col * 16
                    ly = ry0 + 12 + row * 12
                    led = self.canvas.create_oval(lx - 2, ly - 2, lx + 2, ly + 2, fill=COLOR_GREEN, outline="")
                    led_ids.append(led)

        # Events/s caption
        events_rate_text = self.canvas.create_text(
            srv_x0 + 64, srv_y1 - 6, text="events/s: 0.0", font=FONT_MONO_TINY, fill=COLOR_MUTED, anchor="center"
        )

        # Ingest Hub Icon
        hub_x = srv_x1 - 54
        hub_y = srv_y0 + 44
        hub_radius = 16
        hub_circle = self.canvas.create_oval(
            hub_x - hub_radius, hub_y - hub_radius, hub_x + hub_radius, hub_y + hub_radius, fill="#0b1426", outline=COLOR_TEAL, width=2
        )
        hub_core = self.canvas.create_oval(hub_x - 5, hub_y - 5, hub_x + 5, hub_y + 5, fill=COLOR_TEAL, outline="")
        hub_lbl = self.canvas.create_text(hub_x, hub_y + hub_radius + 6, text="Ingest Hub", font=("Segoe UI", 7, "bold"), fill=COLOR_MUTED, anchor="center")

        # API Offline overlay text
        offline_label = self.canvas.create_text(
            (srv_x0 + srv_x1) // 2, (srv_y0 + srv_y1) // 2, text="[ API OFFLINE ]", font=("Segoe UI", 12, "bold"), fill=COLOR_RED, state="hidden"
        )

        self.server_items = {
            "panel": srv_panel,
            "title": srv_title,
            "leds": led_ids,
            "events_rate": events_rate_text,
            "hub_x": hub_x,
            "hub_y": hub_y,
            "hub_circle": hub_circle,
            "hub_core": hub_core,
            "offline_label": offline_label,
        }

        # 3. Electrical GRID (Top Left)
        grid_cx = 46
        grid_top_y = 20
        # Pylon Icon (A-frame tower)
        pylon_legs = self.canvas.create_line(grid_cx, grid_top_y, grid_cx - 18, grid_top_y + 52, fill=COLOR_AMBER, width=1.5)
        pylon_legs2 = self.canvas.create_line(grid_cx, grid_top_y, grid_cx + 18, grid_top_y + 52, fill=COLOR_AMBER, width=1.5)
        crossarm1 = self.canvas.create_line(grid_cx - 24, grid_top_y + 20, grid_cx + 24, grid_top_y + 20, fill=COLOR_AMBER, width=1.5)
        crossarm2 = self.canvas.create_line(grid_cx - 30, grid_top_y + 36, grid_cx + 30, grid_top_y + 36, fill=COLOR_AMBER, width=1.5)
        truss1 = self.canvas.create_line(grid_cx - 12, grid_top_y + 20, grid_cx + 15, grid_top_y + 36, fill=COLOR_AMBER, width=1)
        truss2 = self.canvas.create_line(grid_cx + 12, grid_top_y + 20, grid_cx - 15, grid_top_y + 36, fill=COLOR_AMBER, width=1)
        grid_lbl = self.canvas.create_text(grid_cx, grid_top_y + 60, text="GRID", font=("Segoe UI", 8, "bold"), fill=COLOR_AMBER, anchor="center")

        self.grid_items = {
            "x": grid_cx,
            "y": grid_top_y,
            "out_x": grid_cx + 30,
            "out_y": grid_top_y + 36,
        }

        # 4. Five Labs (C-BPL-01..05)
        lab_y0 = 120
        lab_y1 = self.height - 48
        num_labs = 5
        margin_x = 18
        gap_x = 14
        avail_w = self.width - 2 * margin_x - (num_labs - 1) * gap_x
        lab_w = avail_w / num_labs

        centres_def = [
            ("C-BPL-01", "North Tech Zone"),
            ("C-BPL-02", "South Digital Hub"),
            ("C-BPL-03", "East Assessment Lab"),
            ("C-BPL-04", "West Exam Center"),
            ("C-BPL-05", "Central Institute"),
        ]

        # Power bus line
        bus_y = 42
        power_bus_id = self.canvas.create_line(self.grid_items["out_x"], bus_y, self.width - 24, bus_y, fill="#1c2d52", width=3)
        self.grid_items["bus_line"] = power_bus_id

        for i, (cid, cname) in enumerate(centres_def):
            lx0 = margin_x + i * (lab_w + gap_x)
            lx1 = lx0 + lab_w

            # Lab container with bottom door notch
            notch_w = 24
            notch_x0 = (lx0 + lx1 - notch_w) / 2
            notch_x1 = notch_x0 + notch_w
            notch_depth = 6
            room_pts = [
                lx0, lab_y0,
                lx1, lab_y0,
                lx1, lab_y1,
                notch_x1, lab_y1,
                notch_x1, lab_y1 - notch_depth,
                notch_x0, lab_y1 - notch_depth,
                notch_x0, lab_y1,
                lx0, lab_y1,
            ]
            room_poly = self.canvas.create_polygon(room_pts, fill=COLOR_PANEL, outline=COLOR_GREEN, width=1.5)
            highlight_poly = self.canvas.create_polygon(
                room_pts, fill="", outline=COLOR_TEAL, width=2, dash=(4, 4), state="hidden"
            )
            # Door swing line
            door_swing = self.canvas.create_arc(
                notch_x0 - 8, lab_y1 - 16, notch_x0 + 16, lab_y1 + 8, start=0, extent=-90, style="arc", outline=COLOR_MUTED, dash=(2, 2)
            )

            # Lab title & subtitle
            t_id = self.canvas.create_text(lx0 + 10, lab_y0 + 12, text=cid, font=("Segoe UI", 10, "bold"), fill=COLOR_TEXT, anchor="w")
            sub_id = self.canvas.create_text(lx0 + 10, lab_y0 + 26, text=cname, font=("Segoe UI", 7), fill=COLOR_MUTED, anchor="w")

            # Incident badge (initially hidden)
            inc_badge_rect = self.canvas.create_rectangle(lx1 - 76, lab_y0 + 6, lx1 - 6, lab_y0 + 22, fill="#361010", outline=COLOR_RED, state="hidden")
            inc_badge_text = self.canvas.create_text(lx1 - 41, lab_y0 + 14, text="", font=FONT_MONO_TINY, fill=COLOR_RED, state="hidden")

            # Equipment Corner: UPS, Switch, Router
            eq_y = lab_y0 + 44
            # 1. UPS
            ups_x = lx0 + 20
            ups_rect = self.canvas.create_rectangle(ups_x - 9, eq_y - 7, ups_x + 9, eq_y + 7, fill="#0b1426", outline="#1c2d52", width=1)
            ups_cap = self.canvas.create_rectangle(ups_x - 4, eq_y - 9, ups_x + 4, eq_y - 7, fill="#1c2d52", outline="")
            ups_bar = self.canvas.create_rectangle(ups_x - 7, eq_y - 4, ups_x + 7, eq_y + 4, fill=COLOR_GREEN, outline="")
            # Power drop line from bus to UPS
            p_drop = self.canvas.create_line(ups_x, bus_y, ups_x, eq_y - 9, fill="#1c2d52", width=2)

            # 2. Switch
            sw_x = lx0 + 46
            sw_rect = self.canvas.create_rectangle(sw_x - 9, eq_y - 6, sw_x + 9, eq_y + 6, fill="#0b1426", outline="#1c2d52", width=1)
            sw_dots = []
            for sp in range(4):
                px = sw_x - 6 + sp * 4
                sw_dots.append(self.canvas.create_oval(px - 1, eq_y - 1, px + 1, eq_y + 1, fill=COLOR_GREEN, outline=""))

            # 3. Router
            rt_x = lx0 + 72
            rt_rect = self.canvas.create_rectangle(rt_x - 8, eq_y - 5, rt_x + 8, eq_y + 5, fill="#0b1426", outline="#1c2d52", width=1)
            rt_ant = self.canvas.create_line(rt_x, eq_y - 5, rt_x, eq_y - 14, fill=COLOR_TEAL, width=1.5)
            rt_arc1 = self.canvas.create_arc(rt_x - 6, eq_y - 18, rt_x + 6, eq_y - 6, start=30, extent=120, style="arc", outline=COLOR_TEAL)

            # Network line to Ingest Hub
            net_line = self.canvas.create_line(rt_x, eq_y - 14, hub_x, hub_y, fill="#172c4e", width=1.5)
            self.network_links[cid] = {
                "line": net_line,
                "start_x": rt_x,
                "start_y": eq_y - 14,
                "end_x": hub_x,
                "end_y": hub_y,
            }
            # Pre-allocate packet dot items
            self.network_packets[cid] = []
            for _ in range(6):
                p_dot = self.canvas.create_oval(0, 0, 0, 0, fill=COLOR_TEAL, outline="", state="hidden")
                self.network_packets[cid].append({"id": p_dot, "dist": -1.0, "active": False, "speed": 0.035})

            # Fault Visuals: Broken-link "X" icon at midpoint of network line
            mx = (rt_x + hub_x) / 2
            my = (eq_y - 14 + hub_y) / 2
            x_bg = self.canvas.create_oval(mx - 8, my - 8, mx + 8, my + 8, fill="#1c0f14", outline=COLOR_RED, width=1.5, state="hidden")
            x_l1 = self.canvas.create_line(mx - 4, my - 4, mx + 4, my + 4, fill=COLOR_RED, width=2, state="hidden")
            x_l2 = self.canvas.create_line(mx - 4, my + 4, mx + 4, my - 4, fill=COLOR_RED, width=2, state="hidden")

            # Fault Visuals: Buffering Badge near router
            badge_x0 = rt_x - 24
            badge_y0 = eq_y - 30
            badge_x1 = rt_x + 84
            badge_y1 = eq_y - 14
            badge_rect = self.canvas.create_rectangle(badge_x0, badge_y0, badge_x1, badge_y1, fill="#1a0f24", outline=COLOR_AMBER, width=1, state="hidden")
            badge_text = self.canvas.create_text((badge_x0 + badge_x1) / 2, (badge_y0 + badge_y1) / 2, text="link down, events buffering", font=FONT_UI_TINY, fill=COLOR_AMBER, state="hidden")

            # Fault Visuals: Banner across lab
            banner_x0 = lx0 + 8
            banner_x1 = lx1 - 8
            banner_y0 = lab_y0 + 64
            banner_y1 = lab_y0 + 84
            banner_rect = self.canvas.create_rectangle(banner_x0, banner_y0, banner_x1, banner_y1, fill="#4a0e0e", outline=COLOR_RED, width=1.5, state="hidden")
            banner_text = self.canvas.create_text((banner_x0 + banner_x1) / 2, (banner_y0 + banner_y1) / 2, text="", font=FONT_UI_BOLD, fill=COLOR_TEXT, state="hidden")

            # Fault Visuals: Lightning Bolt icon
            bx = (lx0 + lx1) / 2
            by = lab_y0 + 74
            bolt_pts = [
                bx + 2, by - 14,
                bx - 7, by + 1,
                bx - 1, by + 1,
                bx - 4, by + 14,
                bx + 7, by - 1,
                bx + 1, by - 1,
            ]
            bolt_id = self.canvas.create_polygon(bolt_pts, fill=COLOR_AMBER, outline="#d97706", width=1.5, state="hidden")

            # Fault Visuals: Expanding Red Ring animation oval
            ring_cx = (lx0 + lx1) / 2
            ring_cy = (lab_y0 + lab_y1) / 2
            ring_id = self.canvas.create_oval(ring_cx, ring_cy, ring_cx, ring_cy, outline=COLOR_RED, width=2.5, state="hidden")

            # Workstations Grid (8 cols x 5 rows = 40 PCs)
            ws_top = lab_y0 + 94
            ws_bot = lab_y1 - 22
            grid_cols = 8
            grid_rows = 5
            spacing_x = (lab_w - 24) / grid_cols
            spacing_y = (ws_bot - ws_top) / grid_rows

            for row in range(grid_rows):
                for col in range(grid_cols):
                    pc_idx = row * grid_cols + col
                    cx = lx0 + 16 + col * spacing_x + spacing_x / 2
                    cy = ws_top + row * spacing_y + spacing_y / 2

                    # Workstation geometry (14x11 monitor + stand + desk)
                    mon_w, mon_h = 14, 10
                    mx0, my0 = cx - mon_w / 2, cy - mon_h / 2
                    mx1, my1 = cx + mon_w / 2, cy + mon_h / 2

                    desk = self.canvas.create_line(cx - 9, cy + 9, cx + 9, cy + 9, fill="#1c2d52", width=1.5)
                    stand_neck = self.canvas.create_line(cx, my1, cx, cy + 7, fill="#243556", width=1.5)
                    stand_base = self.canvas.create_line(cx - 4, cy + 7, cx + 4, cy + 7, fill="#243556", width=1.5)
                    mon_frame = self.canvas.create_rectangle(mx0, my0, mx1, my1, fill="#0f172a", outline=COLOR_GREEN, width=1)
                    screen = self.canvas.create_rectangle(mx0 + 2, my0 + 2, mx1 - 2, my1 - 2, fill=COLOR_GREEN, outline="")
                    select_ring = self.canvas.create_rectangle(mx0 - 2, my0 - 2, mx1 + 2, cy + 9, outline=COLOR_TEAL, width=1.5, state="hidden")

                    self.pcs[(cid, pc_idx)] = {
                        "centre_id": cid,
                        "pc_idx": pc_idx,
                        "candidate_id": f"CAND-{(i * 40 + pc_idx + 1):06d}",
                        "state": "registered",
                        "last_ingested_age_s": None,
                        "remaining_s": 7200,
                        "last_saved_seq": 0,
                        "bbox": (mx0 - 4, my0 - 4, mx1 + 4, cy + 12),
                        "frame_id": mon_frame,
                        "screen_id": screen,
                        "select_id": select_ring,
                        "can_pulse": False,
                        "pulse_timer": random.randint(15, 60),
                        "base_screen_col": COLOR_GREEN,
                        "healthy": True,
                    }

            # Caption Under Lab
            cap_y = lab_y1 + 10
            caption_text = self.canvas.create_text(
                (lx0 + lx1) / 2, cap_y, text="40 PCs  |  40 reporting  |  v4.2.1", font=FONT_MONO_TINY, fill=COLOR_MUTED, anchor="center"
            )

            # Blocked Padlock & Warning Text
            padlock_x = lx0 + 14
            pad_body = self.canvas.create_rectangle(padlock_x - 4, cap_y + 14, padlock_x + 4, cap_y + 22, fill=COLOR_RED, outline="", state="hidden")
            pad_arc = self.canvas.create_arc(padlock_x - 3, cap_y + 10, padlock_x + 3, cap_y + 16, start=0, extent=180, style="arc", outline=COLOR_RED, width=1.5, state="hidden")
            blocked_text = self.canvas.create_text(
                (lx0 + lx1) / 2 + 6, cap_y + 18, text="", font=FONT_UI_TINY_BOLD, fill=COLOR_RED, anchor="center", state="hidden"
            )

            self.lab_items[cid] = {
                "room": room_poly,
                "highlight_poly": highlight_poly,
                "title": t_id,
                "subtitle": sub_id,
                "inc_rect": inc_badge_rect,
                "inc_text": inc_badge_text,
                "ups_bar": ups_bar,
                "p_drop": p_drop,
                "sw_dots": sw_dots,
                "rt_ant": rt_ant,
                "rt_arc1": rt_arc1,
                "net_line": net_line,
                "broken_x": [x_bg, x_l1, x_l2],
                "badge_rect": badge_rect,
                "badge_text": badge_text,
                "banner_rect": banner_rect,
                "banner_text": banner_text,
                "bolt": bolt_id,
                "ring_id": ring_id,
                "ring_anim": {
                    "active": False,
                    "start_time": 0.0,
                    "cx": ring_cx,
                    "cy": ring_cy,
                    "max_r": (lx1 - lx0) * 0.85,
                },
                "prev_inc_status": None,
                "has_power_loss": False,
                "resolved_hide_time": 0.0,
                "burst_active": False,
                "burst_end_time": 0.0,
                "caption": caption_text,
                "pad_body": pad_body,
                "pad_arc": pad_arc,
                "blocked_text": blocked_text,
                "header_bbox": (lx0, lab_y0, lx1, lab_y0 + 34),
            }

        # 5. Pre-allocate power flow dots along bus
        for p_idx in range(12):
            dot = self.canvas.create_oval(0, 0, 0, 0, fill=COLOR_TEAL, outline="", state="hidden")
            self.power_dots.append({"id": dot, "x": self.grid_items["out_x"] + p_idx * 65, "y": bus_y})

        # 6. Global Tooltip Overlay (single reusable canvas widget)
        self.tooltip_rect = self.canvas.create_rectangle(0, 0, 0, 0, fill="#12203d", outline=COLOR_TEAL, width=1.5, state="hidden")
        self.tooltip_text = self.canvas.create_text(0, 0, text="", font=FONT_MONO_SMALL, fill=COLOR_TEXT, anchor="nw", state="hidden")

        # Record total items for acceptance monitoring
        self.item_count = len(self.canvas.find_all())

    # -----------------------------------------------------------------------
    # DATA BINDING & UPDATES
    # -----------------------------------------------------------------------
    def update_data(self, state: AppState) -> None:
        """Bind live API responses to canvas attributes without deleting items."""
        self.state = state

        # 1. Server Room telemetry
        rate_str = f"events/s: {state.events_rate:.1f}"
        self.canvas.itemconfig(self.server_items["events_rate"], text=rate_str)

        # If API is down: preserve last visuals and show offline overlay
        if not state.api_ok:
            self.canvas.itemconfig(self.server_items["offline_label"], state="normal")
            for led in self.server_items["leds"]:
                self.canvas.itemconfig(led, fill=COLOR_RED)
            return

        self.canvas.itemconfig(self.server_items["offline_label"], state="hidden")

        # 2. Centre Status & Fault Visuals
        readiness_map = {}
        if state.readiness and isinstance(state.readiness.get("centres"), list):
            readiness_map = {c["centre_id"]: c for c in state.readiness["centres"]}

        centres_data_map = {c["centre_id"]: c for c in state.centres}

        # Match selected incident centre for teal dashed highlight outline
        selected_cid = None
        if state.selected_incident:
            for inc in state.incidents:
                if inc.get("incident_id") == state.selected_incident:
                    selected_cid = inc.get("centre_id")
                    break

        for cid, items in self.lab_items.items():
            # Highlight matching lab
            h_id = items.get("highlight_poly")
            if h_id:
                if cid == selected_cid:
                    self.canvas.itemconfig(h_id, state="normal")
                    self.canvas.tag_raise(h_id)
                else:
                    self.canvas.itemconfig(h_id, state="hidden")
            c_info = centres_data_map.get(cid, {})
            c_status = c_info.get("status", "healthy")
            s_total = c_info.get("sessions_total", 40)
            s_silent = c_info.get("sessions_silent", 0)
            s_reporting = max(0, s_total - s_silent)
            version = c_info.get("software_version", "4.2.1")

            # Match active or recent incident
            cid_incidents = [inc for inc in state.incidents if inc.get("centre_id") == cid]
            active_inc = next((inc for inc in cid_incidents if inc.get("status") in ("open", "recovering")), None)
            if not active_inc and c_info.get("open_incident_id"):
                active_inc = next((inc for inc in cid_incidents if inc.get("incident_id") == c_info.get("open_incident_id")), None)

            now_t = time.time()
            prev_status = items["prev_inc_status"]
            recent_resolved = None

            if active_inc is None:
                latest_res = next((inc for inc in cid_incidents if inc.get("status") == "resolved"), None)
                if latest_res:
                    if prev_status in ("open", "recovering"):
                        items["resolved_hide_time"] = now_t + 6.0
                        recent_resolved = latest_res
                    elif now_t < items.get("resolved_hide_time", 0.0):
                        recent_resolved = latest_res

            inc_to_show = active_inc or recent_resolved
            inc_type = (inc_to_show.get("type") or "").lower() if inc_to_show else ""
            inc_status = (active_inc.get("status") if active_inc else ("resolved" if recent_resolved else "")).lower()
            inc_id = active_inc.get("incident_id") if active_inc else (recent_resolved.get("incident_id") if recent_resolved else None)

            # Detect state transitions
            if inc_status == "open" and prev_status != "open":
                items["ring_anim"]["active"] = True
                items["ring_anim"]["start_time"] = now_t

            # Network burst on recovery: fires when transitioning from open/recovering to recovering/resolved
            is_net_burst = ("network" in inc_type and prev_status in ("open", "recovering") and (inc_status in ("recovering", "resolved") or c_status == "healthy"))
            if is_net_burst and not items.get("burst_active") and items.get("last_burst_inc") != (inc_id or "burst"):
                items["burst_active"] = True
                items["burst_end_time"] = now_t + 1.5
                items["last_burst_inc"] = (inc_id or "burst")
                for idx, p in enumerate(self.network_packets[cid]):
                    p["active"] = True
                    p["dist"] = idx * 0.15
                    p["speed"] = 0.06

            items["prev_inc_status"] = inc_status

            # Lab border colour follows centre status
            if c_status == "down":
                outline_col = COLOR_RED
            elif c_status == "degraded":
                outline_col = COLOR_AMBER
            else:
                outline_col = COLOR_GREEN

            # Incident Badge on lab header
            badge_id = inc_id or c_info.get("open_incident_id")
            if badge_id:
                self.canvas.itemconfig(items["inc_rect"], state="normal")
                self.canvas.itemconfig(items["inc_text"], text=badge_id, state="normal")
            else:
                self.canvas.itemconfig(items["inc_rect"], state="hidden")
                self.canvas.itemconfig(items["inc_text"], state="hidden")

            # Branch A: POWER LOSS
            is_power_fault = ("power" in inc_type and inc_status in ("open", "recovering"))
            items["has_power_loss"] = (is_power_fault and inc_status == "open")

            if is_power_fault and inc_status == "open":
                self.canvas.itemconfig(items["room"], fill="#070b14", outline=outline_col)
                self.canvas.itemconfig(items["bolt"], state="normal")
                self.canvas.itemconfig(items["banner_rect"], fill="#4a0e0e", outline=COLOR_RED, state="normal")
                self.canvas.itemconfig(items["banner_text"], text="POWER LOSS", fill=COLOR_RED, state="normal")
                self.canvas.itemconfig(items["ups_bar"], fill="#121e33")
                for d in items["sw_dots"]:
                    self.canvas.itemconfig(d, fill="#121e33")
                self.canvas.itemconfig(items["rt_ant"], fill="#121e33")
                self.canvas.itemconfig(items["rt_arc1"], outline="#121e33")
                self.canvas.itemconfig(items["net_line"], fill="#475569", dash=(4, 4))
                for elem in items["broken_x"]:
                    self.canvas.itemconfig(elem, state="hidden")
                self.canvas.itemconfig(items["badge_rect"], state="hidden")
                self.canvas.itemconfig(items["badge_text"], state="hidden")
                # Drop all monitors in this lab to dark grey with red outline
                for (c_id, pc_idx), pc in self.pcs.items():
                    if c_id == cid:
                        self.canvas.itemconfig(pc["frame_id"], fill="#1e293b", outline=COLOR_RED)
                        self.canvas.itemconfig(pc["screen_id"], fill="#0f172a")
                        pc["can_pulse"] = False
                        pc["healthy"] = False

            elif is_power_fault and inc_status == "recovering":
                self.canvas.itemconfig(items["room"], fill=COLOR_PANEL, outline=outline_col)
                self.canvas.itemconfig(items["bolt"], state="hidden")
                self.canvas.itemconfig(items["banner_rect"], fill="#3b2307", outline=COLOR_AMBER, state="normal")
                self.canvas.itemconfig(items["banner_text"], text="RECOVERING", fill=COLOR_AMBER, state="normal")
                self.canvas.itemconfig(items["ups_bar"], fill=COLOR_AMBER)
                for d in items["sw_dots"]:
                    self.canvas.itemconfig(d, fill=COLOR_GREEN)
                self.canvas.itemconfig(items["rt_ant"], fill=COLOR_TEAL)
                self.canvas.itemconfig(items["rt_arc1"], outline=COLOR_TEAL)
                self.canvas.itemconfig(items["net_line"], fill="#172c4e", dash=())
                self.canvas.itemconfig(items["p_drop"], fill="#1c2d52", width=2)
                for elem in items["broken_x"]:
                    self.canvas.itemconfig(elem, state="hidden")
                self.canvas.itemconfig(items["badge_rect"], state="hidden")
                self.canvas.itemconfig(items["badge_text"], state="hidden")

            # Branch B: NETWORK DROP
            elif ("network" in inc_type and inc_status in ("open", "recovering")) or (c_status == "down" and not is_power_fault):
                if inc_status == "open" or c_status == "down":
                    self.canvas.itemconfig(items["room"], fill=COLOR_PANEL, outline=outline_col)
                    self.canvas.itemconfig(items["bolt"], state="hidden")
                    self.canvas.itemconfig(items["banner_rect"], state="hidden")
                    self.canvas.itemconfig(items["banner_text"], state="hidden")
                    self.canvas.itemconfig(items["ups_bar"], fill=COLOR_GREEN if c_status == "healthy" else COLOR_AMBER)
                    for d in items["sw_dots"]:
                        self.canvas.itemconfig(d, fill=COLOR_GREEN)
                    self.canvas.itemconfig(items["rt_ant"], fill=COLOR_TEAL)
                    self.canvas.itemconfig(items["rt_arc1"], outline=COLOR_TEAL)
                    self.canvas.itemconfig(items["p_drop"], fill="#1c2d52", width=2)
                    self.canvas.itemconfig(items["net_line"], fill=COLOR_RED, dash=(4, 4))
                    for elem in items["broken_x"]:
                        self.canvas.itemconfig(elem, state="normal")
                    ev = active_inc.get("evidence") or {} if active_inc else {}
                    late_cnt = ev.get("late_events")
                    if isinstance(late_cnt, (int, float)) and late_cnt > 0:
                        b_text = f"link down, events buffering ({int(late_cnt)})"
                    else:
                        b_text = "link down, events buffering"
                    self.canvas.itemconfig(items["badge_text"], text=b_text, state="normal")
                    self.canvas.itemconfig(items["badge_rect"], state="normal")

                else:  # recovering
                    self.canvas.itemconfig(items["room"], fill=COLOR_PANEL, outline=outline_col)
                    self.canvas.itemconfig(items["bolt"], state="hidden")
                    self.canvas.itemconfig(items["banner_rect"], fill="#3b2307", outline=COLOR_AMBER, state="normal")
                    self.canvas.itemconfig(items["banner_text"], text="RECOVERING", fill=COLOR_AMBER, state="normal")
                    for elem in items["broken_x"]:
                        self.canvas.itemconfig(elem, state="hidden")
                    self.canvas.itemconfig(items["badge_rect"], state="hidden")
                    self.canvas.itemconfig(items["badge_text"], state="hidden")
                    self.canvas.itemconfig(items["net_line"], fill=COLOR_TEAL, dash=())

            # Branch C: RESOLVED / NORMAL
            else:
                room_bg = "#20111a" if c_status == "down" else ("#16223b" if c_status == "degraded" else COLOR_PANEL)
                self.canvas.itemconfig(items["room"], fill=room_bg, outline=outline_col)
                self.canvas.itemconfig(items["bolt"], state="hidden")
                for elem in items["broken_x"]:
                    self.canvas.itemconfig(elem, state="hidden")
                self.canvas.itemconfig(items["badge_rect"], state="hidden")
                self.canvas.itemconfig(items["badge_text"], state="hidden")
                self.canvas.itemconfig(items["net_line"], fill="#172c4e", dash=())
                self.canvas.itemconfig(items["p_drop"], fill="#1c2d52", width=2)
                self.canvas.itemconfig(items["ups_bar"], fill=COLOR_RED if c_status == "down" else (COLOR_AMBER if c_status == "degraded" else COLOR_GREEN))
                for d in items["sw_dots"]:
                    self.canvas.itemconfig(d, fill=COLOR_GREEN)
                self.canvas.itemconfig(items["rt_ant"], fill=COLOR_TEAL)
                self.canvas.itemconfig(items["rt_arc1"], outline=COLOR_TEAL)

                if time.time() < items.get("resolved_hide_time", 0.0):
                    self.canvas.itemconfig(items["banner_rect"], fill="#07301c", outline=COLOR_GREEN, state="normal")
                    self.canvas.itemconfig(items["banner_text"], text="✓ RESOLVED", fill=COLOR_GREEN, state="normal")
                else:
                    self.canvas.itemconfig(items["banner_rect"], state="hidden")
                    self.canvas.itemconfig(items["banner_text"], state="hidden")

            # Caption line
            cap_str = f"{s_total} PCs  |  {s_reporting} reporting  |  v{version}"
            self.canvas.itemconfig(items["caption"], text=cap_str)

            # Readiness Blocked Warning
            r_info = readiness_map.get(cid, {})
            if r_info.get("passed") is False:
                v_checks = r_info.get("checks", {}).get("version", {})
                have_v = v_checks.get("have", version)
                need_v = v_checks.get("need", state.readiness.get("required_version", "4.2.1")) if state.readiness else "4.2.1"
                self.canvas.itemconfig(items["blocked_text"], text=f"BLOCKED: version {have_v} (need {need_v})", state="normal")
                self.canvas.itemconfig(items["pad_body"], state="normal")
                self.canvas.itemconfig(items["pad_arc"], state="normal")
            else:
                self.canvas.itemconfig(items["blocked_text"], state="hidden")
                self.canvas.itemconfig(items["pad_body"], state="hidden")
                self.canvas.itemconfig(items["pad_arc"], state="hidden")

        # 3. Workstations per-PC State (unless lab has open power loss)
        for (cid, pc_idx), pc in self.pcs.items():
            if self.lab_items[cid].get("has_power_loss"):
                continue

            sessions = state.centre_sessions.get(cid, [])
            if pc_idx < len(sessions):
                s_row = sessions[pc_idx]
                st = s_row.get("state", "registered")
                age_s = s_row.get("last_ingested_age_s")
                rem_s = s_row.get("remaining_s", 7200)
                seq = s_row.get("last_saved_seq", 0)
                cand_id = s_row.get("candidate_id", pc["candidate_id"])

                pc["candidate_id"] = cand_id
                pc["state"] = st
                pc["last_ingested_age_s"] = age_s
                pc["remaining_s"] = rem_s
                pc["last_saved_seq"] = seq

                is_silent = (age_s is None or age_s > 6.0) if st in ("active", "resumed", "interrupted") else False

                if st == "submitted":
                    f_col, s_col, o_col = "#1e3a5f", COLOR_BLUE, "#3b82f6"
                    pulse = False
                    healthy = True
                elif st == "under_review":
                    f_col, s_col, o_col = "#38280d", COLOR_AMBER, "#d97706"
                    pulse = False
                    healthy = True
                elif st == "interrupted" or is_silent:
                    f_col, s_col, o_col = "#1e293b", "#0f172a", COLOR_RED
                    pulse = False
                    healthy = False
                elif st == "resumed":
                    f_col, s_col, o_col = "#042f2e", COLOR_TEAL, "#14b8a6"
                    pulse = True
                    healthy = True
                elif st == "active":
                    f_col, s_col, o_col = "#064e3b", COLOR_GREEN, "#10b981"
                    pulse = True
                    healthy = True
                else:  # registered
                    f_col, s_col, o_col = "#0f172a", "#1e293b", "#334155"
                    pulse = False
                    healthy = False

                pc["can_pulse"] = pulse
                pc["base_screen_col"] = s_col
                pc["healthy"] = healthy

                self.canvas.itemconfig(pc["frame_id"], fill=f_col, outline=o_col)
                self.canvas.itemconfig(pc["screen_id"], fill=s_col)

        # 4. Workstation Selection Ring Synchronisation
        sel_cand = state.selected_candidate
        if sel_cand:
            matched = False
            for (cid, pc_idx), pc in self.pcs.items():
                if pc.get("candidate_id") == sel_cand:
                    if self.selected_item_id and self.selected_item_id != pc["select_id"]:
                        self.canvas.itemconfig(self.selected_item_id, state="hidden")
                    self.selected_item_id = pc["select_id"]
                    self.canvas.itemconfig(pc["select_id"], state="normal")
                    self.canvas.tag_raise(pc["select_id"])
                    matched = True
                    break
            if not matched and self.selected_item_id:
                self.canvas.itemconfig(self.selected_item_id, state="hidden")
                self.selected_item_id = None
        else:
            if self.selected_item_id:
                self.canvas.itemconfig(self.selected_item_id, state="hidden")
                self.selected_item_id = None

    # -----------------------------------------------------------------------
    # ANIMATION LOOP (~16 FPS / 60 MS)
    # -----------------------------------------------------------------------
    def animate(self) -> None:
        """Execute fast, incremental particle and LED updates."""
        t_start = time.perf_counter()

        if self.state and self.state.api_ok:
            # 1. Server Rack LEDs (Flicker proportional to events/s)
            ev_rate = getattr(self.state, "events_rate", 0.0)
            flicker_prob = min(0.7, 0.15 + ev_rate * 0.02)
            for led in self.server_items["leds"]:
                if random.random() < flicker_prob:
                    col = random.choice([COLOR_GREEN, COLOR_TEAL, "#059669"])
                    self.canvas.itemconfig(led, fill=col)

            # 2. Power Line Dots (Move along top bus from GRID)
            bus_end_x = self.width - 24
            for p in self.power_dots:
                p["x"] += 2.0
                if p["x"] > bus_end_x:
                    p["x"] = self.grid_items["out_x"]
                self.canvas.coords(p["id"], p["x"] - 2, p["y"] - 2, p["x"] + 2, p["y"] + 2)
                self.canvas.itemconfig(p["id"], state="normal")

            # 3. Lab-level dynamic animations: Power flicker, red rings, resolved hide, network packets & bursts
            now_t = time.time()
            for cid, items in self.lab_items.items():
                # Power branch flicker (alternate colour every 200 ms)
                if items.get("has_power_loss"):
                    flicker = int(now_t / 0.200) % 2 == 0
                    col = COLOR_RED if flicker else "#451010"
                    self.canvas.itemconfig(items["p_drop"], fill=col, width=2.5)

                # Expanding red ring (1.2 s)
                r_anim = items.get("ring_anim")
                if r_anim and r_anim.get("active"):
                    elapsed = now_t - r_anim["start_time"]
                    if elapsed >= 1.2:
                        r_anim["active"] = False
                        self.canvas.itemconfig(items["ring_id"], state="hidden")
                    else:
                        prog = elapsed / 1.2
                        r = 10 + (r_anim["max_r"] - 10) * prog
                        cx, cy = r_anim["cx"], r_anim["cy"]
                        self.canvas.coords(items["ring_id"], cx - r, cy - r, cx + r, cy + r)
                        self.canvas.itemconfig(items["ring_id"], state="normal")
                        self.canvas.tag_raise(items["ring_id"])

                # Auto-hide resolved banner after 6 s
                res_time = items.get("resolved_hide_time", 0.0)
                if res_time > 0 and now_t >= res_time:
                    items["resolved_hide_time"] = 0.0
                    if not items.get("prev_inc_status") in ("open", "recovering"):
                        self.canvas.itemconfig(items["banner_rect"], state="hidden")
                        self.canvas.itemconfig(items["banner_text"], state="hidden")

                # Network Link Packets & Packet Burst
                link = self.network_links[cid]
                c_data = next((c for c in self.state.centres if c["centre_id"] == cid), {})
                c_status = c_data.get("status", "healthy")
                s_frac = c_data.get("silent_fraction", 0.0)
                is_link_blocked = (c_status == "down" or items.get("has_power_loss") or (items.get("prev_inc_status") == "open"))

                if items.get("burst_active"):
                    if now_t >= items.get("burst_end_time", 0.0):
                        items["burst_active"] = False
                    else:
                        sx, sy = link["start_x"], link["start_y"]
                        ex, ey = link["end_x"], link["end_y"]
                        for p in self.network_packets[cid]:
                            if not p["active"]:
                                p["active"] = True
                                p["dist"] = 0.0
                                p["speed"] = 0.06
                            p["dist"] += p["speed"]
                            if p["dist"] >= 1.0:
                                p["dist"] = 0.0
                            px = sx + (ex - sx) * p["dist"]
                            py = sy + (ey - sy) * p["dist"]
                            self.canvas.coords(p["id"], px - 2.5, py - 2.5, px + 2.5, py + 2.5)
                            self.canvas.itemconfig(p["id"], fill=COLOR_TEAL, state="normal")
                        continue

                # Normal packet spawn
                if not is_link_blocked and s_frac < 0.6:
                    spawn_rate = min(0.35, max(0.04, ev_rate * 0.015))
                    active_cnt = sum(1 for p in self.network_packets[cid] if p["active"])
                    if active_cnt < 6 and random.random() < spawn_rate:
                        for p in self.network_packets[cid]:
                            if not p["active"]:
                                p["active"] = True
                                p["dist"] = 0.0
                                p["speed"] = random.uniform(0.025, 0.045)
                                break
                elif is_link_blocked:
                    for p in self.network_packets[cid]:
                        if p["active"]:
                            p["active"] = False
                            self.canvas.itemconfig(p["id"], state="hidden")

                # Advance normal active packets toward hub
                sx, sy = link["start_x"], link["start_y"]
                ex, ey = link["end_x"], link["end_y"]
                for p in self.network_packets[cid]:
                    if p["active"]:
                        p["dist"] += p["speed"]
                        if p["dist"] >= 1.0:
                            p["active"] = False
                            self.canvas.itemconfig(p["id"], state="hidden")
                        else:
                            px = sx + (ex - sx) * p["dist"]
                            py = sy + (ey - sy) * p["dist"]
                            self.canvas.coords(p["id"], px - 2.5, py - 2.5, px + 2.5, py + 2.5)
                            self.canvas.itemconfig(p["id"], state="normal")

            # 4. Workstation Brighten Pulse (Staggered on healthy PCs)
            for pc in self.pcs.values():
                if not pc["can_pulse"] or not pc["healthy"]:
                    continue
                pc["pulse_timer"] -= 1
                if pc["pulse_timer"] <= 0:
                    self.canvas.itemconfig(pc["screen_id"], fill="#a7f3d0")
                    pc["pulse_timer"] = random.randint(35, 60)
                elif pc["pulse_timer"] == 33:
                    self.canvas.itemconfig(pc["screen_id"], fill=pc["base_screen_col"])

        else:
            # Hide moving particles when API is offline
            for p in self.power_dots:
                self.canvas.itemconfig(p["id"], state="hidden")
            for cid in self.network_packets:
                for p in self.network_packets[cid]:
                    if p["active"]:
                        p["active"] = False
                        self.canvas.itemconfig(p["id"], state="hidden")

        # Performance timing calculation
        t_end = time.perf_counter()
        dur_ms = (t_end - t_start) * 1000.0
        self.frame_times.append(dur_ms)
        if len(self.frame_times) > 60:
            self.frame_times.pop(0)
        self.avg_frame_ms = sum(self.frame_times) / len(self.frame_times)

    # -----------------------------------------------------------------------
    # INTERACTION (HOVER & SELECTION)
    # -----------------------------------------------------------------------
    def on_mouse_move(self, event: tk.Event) -> None:
        """Render tooltip card when hovering over PC or lab header."""
        mx, my = event.x, event.y
        hovered_pc = None

        # Check PC collision
        for (cid, pc_idx), pc in self.pcs.items():
            bx0, by0, bx1, by1 = pc["bbox"]
            if bx0 <= mx <= bx1 and by0 <= my <= by1:
                hovered_pc = pc
                break

        if hovered_pc:
            rem_s = hovered_pc["remaining_s"]
            rem_m = rem_s // 60
            age = hovered_pc["last_ingested_age_s"]
            age_str = f"{age:.1f}s ago" if age is not None else "no telemetry"
            card_text = (
                f"Candidate : {hovered_pc['candidate_id']}\n"
                f"Centre    : {hovered_pc['centre_id']} (PC-{hovered_pc['pc_idx']+1:02d})\n"
                f"State     : {hovered_pc['state'].upper()}\n"
                f"Remaining : {rem_m}m {rem_s%60}s\n"
                f"Last Seen : {age_str}"
            )
            self._show_tooltip(mx + 12, my + 12, card_text)
            return

        # Check Lab header collision
        for cid, items in self.lab_items.items():
            hx0, hy0, hx1, hy1 = items["header_bbox"]
            if hx0 <= mx <= hx1 and hy0 <= my <= hy1:
                r_info = {}
                if self.state and self.state.readiness:
                    r_info = next((c for c in self.state.readiness.get("centres", []) if c["centre_id"] == cid), {})
                v_chk = r_info.get("checks", {}).get("version", {})
                p_chk = r_info.get("checks", {}).get("power_backup", {})
                c_chk = r_info.get("checks", {}).get("capacity", {})

                card_text = (
                    f"Centre    : {cid}\n"
                    f"Version   : {v_chk.get('have', '4.2.1')} (need {v_chk.get('need', '4.2.1')}) -> {'OK' if v_chk.get('ok') else 'FAIL'}\n"
                    f"Backup    : {p_chk.get('minutes', 60)} min -> {'OK' if p_chk.get('ok') else 'FAIL'}\n"
                    f"Capacity  : {c_chk.get('have', 40)} seats (need {c_chk.get('need', 40)}) -> {'OK' if c_chk.get('ok') else 'FAIL'}\n"
                    f"Readiness : {'PASSED' if r_info.get('passed', True) else 'BLOCKED'}"
                )
                self._show_tooltip(mx + 12, my + 12, card_text)
                return

        self._hide_tooltip()

    def on_mouse_click(self, event: tk.Event) -> None:
        """Handle workstation selection with a teal ring."""
        mx, my = event.x, event.y
        for (cid, pc_idx), pc in self.pcs.items():
            bx0, by0, bx1, by1 = pc["bbox"]
            if bx0 <= mx <= bx1 and by0 <= my <= by1:
                # Remove previous selection ring
                if self.selected_item_id:
                    self.canvas.itemconfig(self.selected_item_id, state="hidden")

                # Activate new ring
                self.selected_item_id = pc["select_id"]
                self.canvas.itemconfig(self.selected_item_id, state="normal")
                self.selected_candidate = pc["candidate_id"]
                if self.state:
                    self.state.selected_candidate = pc["candidate_id"]
                break

    def on_mouse_leave(self, _event: tk.Event) -> None:
        self._hide_tooltip()

    def _show_tooltip(self, x: int, y: int, text: str) -> None:
        self.canvas.itemconfig(self.tooltip_text, text=text, state="normal")
        self.canvas.coords(self.tooltip_text, x + 6, y + 6)
        bbox = self.canvas.bbox(self.tooltip_text)
        if bbox:
            bx0, by0, bx1, by1 = bbox
            self.canvas.coords(self.tooltip_rect, bx0 - 6, by0 - 4, bx1 + 6, by1 + 4)
            self.canvas.itemconfig(self.tooltip_rect, state="normal")
            self.canvas.tag_raise(self.tooltip_rect)
            self.canvas.tag_raise(self.tooltip_text)

    def _hide_tooltip(self) -> None:
        if self.tooltip_rect and self.tooltip_text:
            self.canvas.itemconfig(self.tooltip_rect, state="hidden")
            self.canvas.itemconfig(self.tooltip_text, state="hidden")

    def _create_rounded_rect(self, x0: float, y0: float, x1: float, y1: float, r: float = 6, **kwargs) -> int:
        pts = [
            x0 + r, y0,
            x1 - r, y0,
            x1, y0,
            x1, y0 + r,
            x1, y1 - r,
            x1, y1,
            x1 - r, y1,
            x0 + r, y1,
            x0, y1,
            x0, y1 - r,
            x0, y0 + r,
            x0, y0,
        ]
        return self.canvas.create_polygon(pts, smooth=True, **kwargs)


# ---------------------------------------------------------------------------
# TOAST NOTIFICATION
# ---------------------------------------------------------------------------
class ToastNotification(tk.Frame):
    """Floating notification toast at the top-center of the canvas."""

    def __init__(self, parent: tk.Widget):
        super().__init__(
            parent,
            bg="#0e172a",
            highlightthickness=1,
            highlightbackground=COLOR_TEAL,
            padx=16,
            pady=8,
        )
        self.label = tk.Label(
            self,
            text="",
            font=FONT_UI_BOLD,
            fg=COLOR_TEAL,
            bg="#0e172a",
        )
        self.label.pack()
        self._hide_after_id: Optional[str] = None

    def show(self, text: str, is_error: bool = False) -> None:
        if self._hide_after_id:
            try:
                self.after_cancel(self._hide_after_id)
            except Exception:
                pass
            self._hide_after_id = None

        col = COLOR_RED if is_error else COLOR_TEAL
        self.configure(highlightbackground=col)
        self.label.configure(text=text, fg=col)
        self.place(relx=0.5, y=14, anchor="n")
        self.lift()
        self._hide_after_id = self.after(4000, self.hide)

    def hide(self) -> None:
        self._hide_after_id = None
        self.place_forget()


# ---------------------------------------------------------------------------
# FAULT PANEL (FLOATING CARD AT BOTTOM-LEFT)
# ---------------------------------------------------------------------------
class FaultPanel(tk.Frame):
    """Floating dark card anchored bottom-left of FloorCanvas for fault injection."""

    def __init__(self, parent: tk.Widget, on_inject_cb: Any):
        super().__init__(
            parent,
            bg="#0e172a",
            highlightthickness=1,
            highlightbackground=COLOR_BORDER,
            padx=12,
            pady=10,
        )
        self.on_inject_cb = on_inject_cb

        # Stopwatch internal state
        self.sw_active = False
        self.sw_target_centre: Optional[str] = None
        self.sw_press_time = 0.0
        self.sw_press_utc: Optional[datetime] = None
        self.sw_opened = False
        self.sw_resolved = False
        self.sw_incident_id: Optional[str] = None
        self.sw_open_s = 0.0

        # Title
        title_lbl = tk.Label(
            self,
            text="Fault injection",
            font=FONT_UI_BOLD,
            fg=COLOR_TEXT,
            bg="#0e172a",
            anchor="w",
        )
        title_lbl.pack(fill=tk.X, pady=(0, 6))

        # Grid of controls
        ctrl_frame = tk.Frame(self, bg="#0e172a")
        ctrl_frame.pack(fill=tk.X, pady=(0, 4))

        # Centre Combobox
        tk.Label(ctrl_frame, text="Centre:", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg="#0e172a").grid(
            row=0, column=0, sticky="w", padx=(0, 6), pady=2
        )
        self.centre_var = tk.StringVar(value="C-BPL-02")
        self.centre_combo = ttk.Combobox(
            ctrl_frame,
            textvariable=self.centre_var,
            values=["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"],
            state="readonly",
            width=11,
            font=FONT_UI_SMALL,
        )
        self.centre_combo.grid(row=0, column=1, sticky="w", pady=2)
        self.centre_combo.bind("<<ComboboxSelected>>", self._on_input_changed)

        # Fault Type Combobox
        tk.Label(ctrl_frame, text="Fault:", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg="#0e172a").grid(
            row=1, column=0, sticky="w", padx=(0, 6), pady=2
        )
        self.fault_var = tk.StringVar(value="Power loss")
        self.fault_combo = ttk.Combobox(
            ctrl_frame,
            textvariable=self.fault_var,
            values=["Power loss", "Network drop"],
            state="readonly",
            width=13,
            font=FONT_UI_SMALL,
        )
        self.fault_combo.grid(row=1, column=1, sticky="w", pady=2)

        # Duration Spinbox
        tk.Label(ctrl_frame, text="Duration:", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg="#0e172a").grid(
            row=2, column=0, sticky="w", padx=(0, 6), pady=2
        )
        dur_frame = tk.Frame(ctrl_frame, bg="#0e172a")
        dur_frame.grid(row=2, column=1, sticky="w", pady=2)

        self.dur_var = tk.IntVar(value=40)
        self.dur_spin = tk.Spinbox(
            dur_frame,
            from_=5,
            to=120,
            textvariable=self.dur_var,
            width=5,
            bg=COLOR_CARD,
            fg=COLOR_TEXT,
            insertbackground=COLOR_TEAL,
            buttonbackground=COLOR_PANEL,
            relief="solid",
            bd=1,
            font=FONT_MONO_SMALL,
        )
        self.dur_spin.pack(side=tk.LEFT)
        tk.Label(dur_frame, text="s", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg="#0e172a").pack(
            side=tk.LEFT, padx=(4, 0)
        )

        # INJECT Button
        btn_frame = tk.Frame(self, bg="#0e172a")
        btn_frame.pack(fill=tk.X, pady=(4, 2))

        self.inject_btn = tk.Button(
            btn_frame,
            text="INJECT",
            bg=COLOR_TEAL,
            fg="#0b1426",
            activebackground="#14b8a6",
            activeforeground="#0b1426",
            font=FONT_UI_BOLD,
            relief="flat",
            cursor="hand2",
            padx=14,
            pady=3,
            command=self.on_inject_clicked,
        )
        self.inject_btn.pack(side=tk.LEFT)

        # Disabled Reason Label
        self.reason_label = tk.Label(
            self,
            text="",
            font=FONT_UI_TINY,
            fg=COLOR_MUTED,
            bg="#0e172a",
            anchor="w",
            wraplength=270,
            justify="left",
        )
        self.reason_label.pack(fill=tk.X, pady=(2, 4))

        # Explanatory Note
        note_text = (
            "Commands go to the API; the simulator process executes them. "
            "Start the simulator first and wait about 15 s after it starts (detection start-up grace)."
        )
        self.note_label = tk.Label(
            self,
            text=note_text,
            font=FONT_UI_TINY,
            fg="#64748b",
            bg="#0e172a",
            wraplength=270,
            justify="left",
        )
        self.note_label.pack(fill=tk.X, pady=(2, 6))

        # Stopwatch Section
        self.sw_line1 = tk.Label(
            self,
            text="",
            font=("Consolas", 11, "bold"),
            fg=COLOR_TEAL,
            bg="#0e172a",
            anchor="w",
        )
        self.sw_line1.pack(fill=tk.X, pady=(2, 0))

        self.sw_line2 = tk.Label(
            self,
            text="",
            font=("Consolas", 9),
            fg=COLOR_GREEN,
            bg="#0e172a",
            anchor="w",
        )
        self.sw_line2.pack(fill=tk.X, pady=(1, 0))

    def _on_input_changed(self, _event=None) -> None:
        pass

    def set_inputs(self, centre_id: str, fault_type: str, duration_s: int) -> None:
        self.centre_var.set(centre_id)
        self.fault_var.set(fault_type)
        self.dur_var.set(duration_s)

    def on_inject_clicked(self) -> None:
        btn_st = str(self.inject_btn["state"])
        if btn_st == "disabled":
            log_ascii(f"on_inject_clicked ignored: button is disabled (reason: '{self.reason_label.cget('text')}')")
            return
        cid = self.centre_var.get()
        ftype_str = self.fault_var.get()
        ftype = "power_loss" if "power" in ftype_str.lower() else "network_drop"
        try:
            dur = int(self.dur_var.get())
            dur = max(5, min(120, dur))
        except Exception:
            dur = 40
        log_ascii(f"on_inject_clicked: injecting {ftype} at {cid} ({dur}s)")
        if self.on_inject_cb:
            self.on_inject_cb(cid, ftype, dur)

    def start_stopwatch(self, centre_id: str) -> None:
        self.sw_active = True
        self.sw_target_centre = centre_id
        self.sw_press_time = time.time()
        self.sw_press_utc = datetime.now(timezone.utc)
        self.sw_opened = False
        self.sw_resolved = False
        self.sw_incident_id = None
        self.sw_open_s = 0.0
        self.sw_line1.config(text="press to incident opened: 0.0 s", fg=COLOR_TEAL)
        self.sw_line2.config(text="")

    def reset_stopwatch_on_error(self) -> None:
        self.sw_active = False
        self.sw_line1.config(text="")
        self.sw_line2.config(text="")

    def update_stopwatch(self, state: AppState) -> None:
        if not self.sw_active:
            return

        now = time.time()
        if not self.sw_opened:
            elapsed = now - self.sw_press_time
            if elapsed > 60.0:
                self.sw_line1.config(text="no incident yet", fg=COLOR_AMBER)
                return

            self.sw_line1.config(text=f"press to incident opened: {elapsed:.1f} s", fg=COLOR_TEAL)

            for inc in state.incidents:
                if inc.get("centre_id") == self.sw_target_centre:
                    det_str = inc.get("detected_at")
                    if det_str:
                        try:
                            det_dt = datetime.fromisoformat(det_str.replace("Z", "+00:00"))
                            if self.sw_press_utc and (det_dt - self.sw_press_utc).total_seconds() >= -2.0:
                                self.sw_opened = True
                                self.sw_incident_id = inc.get("incident_id")
                                self.sw_open_s = elapsed
                                self.sw_line1.config(
                                    text=f"press to incident opened: {elapsed:.1f} s ({self.sw_incident_id})",
                                    fg=COLOR_TEAL,
                                )
                                break
                        except Exception:
                            pass

        if self.sw_opened and not self.sw_resolved and self.sw_incident_id:
            for inc in state.incidents:
                if inc.get("incident_id") == self.sw_incident_id:
                    if inc.get("status") == "resolved" and inc.get("resolved_at") and inc.get("detected_at"):
                        try:
                            det_dt = datetime.fromisoformat(inc["detected_at"].replace("Z", "+00:00"))
                            res_dt = datetime.fromisoformat(inc["resolved_at"].replace("Z", "+00:00"))
                            dur_s = max(0.0, (res_dt - det_dt).total_seconds())
                            self.sw_resolved = True
                            self.sw_line2.config(
                                text=f"incident resolved after {dur_s:.1f} s",
                                fg=COLOR_GREEN,
                            )
                            break
                        except Exception:
                            pass

    def update_state(self, state: AppState) -> None:
        # Update centre combobox values from API
        if state.centres:
            cids = [c["centre_id"] for c in state.centres if "centre_id" in c]
            if cids and list(self.centre_combo["values"]) != cids:
                self.centre_combo["values"] = cids

        cid = self.centre_var.get()
        if not state.api_ok:
            self.inject_btn.config(state="disabled", bg="#1e293b", fg="#64748b", cursor="arrow")
            self.reason_label.config(text="API is DOWN - fault injection disabled")
        else:
            c_info = next((c for c in state.centres if c.get("centre_id") == cid), {})
            open_id = c_info.get("open_incident_id")
            active_inc = next((inc for inc in state.incidents if inc.get("centre_id") == cid and inc.get("status") in ("open", "recovering")), None)
            if not active_inc and open_id:
                active_inc = next((inc for inc in state.incidents if inc.get("incident_id") == open_id), None)

            if active_inc or open_id:
                st = active_inc.get("status", "open") if active_inc else "open"
                inc_id = active_inc.get("incident_id") if active_inc else open_id
                self.inject_btn.config(state="disabled", bg="#1e293b", fg="#64748b", cursor="arrow")
                self.reason_label.config(text=f"{cid} has {st} incident ({inc_id})")
            else:
                self.inject_btn.config(state="normal", bg=COLOR_TEAL, fg="#0b1426", cursor="hand2")
                self.reason_label.config(text="")

        self.update_stopwatch(state)


# ---------------------------------------------------------------------------
# FLOOR CANVAS (LEFT 70% CONTAINER)
# ---------------------------------------------------------------------------
class FloorCanvas(tk.Frame):
    """Floor view displaying labs, machines, and live operational topology."""

    def __init__(self, parent: tk.Widget, on_inject: Optional[Any] = None, on_double_click: Optional[Any] = None):
        super().__init__(parent, bg=COLOR_PANEL, highlightthickness=1, highlightbackground=COLOR_BORDER)
        self.canvas = tk.Canvas(self, bg=COLOR_PANEL, highlightthickness=0)
        self.canvas.pack(fill=tk.BOTH, expand=True)

        self.scene = FacilityScene(self.canvas)
        self._resize_after_id: Optional[str] = None
        self._on_double_click_cb = on_double_click

        # Floating Toast and FaultPanel anchored on FloorCanvas
        self.toast = ToastNotification(self)
        self.fault_panel = FaultPanel(self, on_inject_cb=on_inject)
        self.fault_panel.place(relx=0.0, rely=1.0, x=10, y=-10, anchor="sw")

        self.canvas.bind("<Configure>", self._on_configure)
        self.canvas.bind("<Motion>", self.scene.on_mouse_move)
        self.canvas.bind("<Button-1>", self.scene.on_mouse_click)
        self.canvas.bind("<Double-Button-1>", self._on_double_click)
        self.canvas.bind("<Leave>", self.scene.on_mouse_leave)

    def _on_configure(self, _event: tk.Event) -> None:
        """Debounce resize rebuilds by 200 ms to prevent thrashing."""
        if self._resize_after_id:
            self.after_cancel(self._resize_after_id)
        self._resize_after_id = self.after(200, self._handle_debounced_resize)

    def _handle_debounced_resize(self) -> None:
        self._resize_after_id = None
        w = max(900, self.canvas.winfo_width())
        h = max(520, self.canvas.winfo_height())
        self.scene.build_scene(w, h)
        self.fault_panel.lift()
        self.toast.lift()
        if self.scene.state:
            self.scene.update_data(self.scene.state)

    def show_toast(self, text: str, is_error: bool = False) -> None:
        self.toast.show(text, is_error=is_error)

    def _on_double_click(self, event: tk.Event) -> None:
        """Double-click a workstation: select it and switch to Candidate tab."""
        mx, my = event.x, event.y
        for (cid, pc_idx), pc in self.scene.pcs.items():
            bx0, by0, bx1, by1 = pc["bbox"]
            if bx0 <= mx <= bx1 and by0 <= my <= by1:
                # Select the workstation (same as single click)
                self.scene.on_mouse_click(event)
                # Trigger callback to switch to Candidate tab
                if self._on_double_click_cb:
                    self._on_double_click_cb(pc["candidate_id"])
                return

    def update_view(self, state: AppState) -> None:
        """Bind state updates into scene and fault panel."""
        if not self.scene.lab_items:
            w = max(900, self.canvas.winfo_width())
            h = max(520, self.canvas.winfo_height())
            self.scene.build_scene(w, h)
            self.fault_panel.lift()
            self.toast.lift()
        self.scene.update_data(state)
        self.fault_panel.update_state(state)


# ---------------------------------------------------------------------------
# SCROLLABLE FRAME HELPER
# ---------------------------------------------------------------------------
class ScrollableFrame(tk.Frame):
    """Scrollable canvas container for dynamic-height tab views."""

    def __init__(self, parent: tk.Widget, bg: str = COLOR_PANEL):
        super().__init__(parent, bg=bg)
        self.canvas = tk.Canvas(self, bg=bg, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.inner = tk.Frame(self.canvas, bg=bg)

        self.inner_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.inner.bind("<Configure>", self._on_inner_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

    def _on_inner_configure(self, _event=None) -> None:
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event: tk.Event) -> None:
        self.canvas.itemconfig(self.inner_id, width=event.width)

    def _on_mousewheel(self, event: tk.Event) -> None:
        try:
            x, y = self.canvas.winfo_pointerxy()
            widget = self.canvas.winfo_containing(x, y)
            if widget and (widget == self.canvas or str(widget).startswith(str(self.inner))):
                self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# INCIDENT CARD & INCIDENTS TAB
# ---------------------------------------------------------------------------
class IncidentCard(tk.Frame):
    """Individual card for an incident with badges, timeline, and selection outline."""

    def __init__(self, parent: tk.Widget, on_select_cb: Any):
        super().__init__(
            parent,
            bg=COLOR_CARD,
            highlightbackground=COLOR_BORDER,
            highlightthickness=1,
            padx=10,
            pady=8,
            cursor="hand2",
        )
        self.on_select_cb = on_select_cb
        self.incident_id: Optional[str] = None

        # Top row: Incident ID, Centre, Badges
        top_frame = tk.Frame(self, bg=COLOR_CARD)
        top_frame.pack(fill=tk.X, pady=(0, 4))

        self.lbl_id = tk.Label(top_frame, text="", font=FONT_UI_BOLD, fg=COLOR_TEXT, bg=COLOR_CARD)
        self.lbl_id.pack(side=tk.LEFT, padx=(0, 6))

        self.lbl_centre = tk.Label(top_frame, text="", font=FONT_UI_BOLD, fg=COLOR_TEAL, bg=COLOR_CARD)
        self.lbl_centre.pack(side=tk.LEFT, padx=(0, 8))

        # Badges frame on right
        badges_frame = tk.Frame(top_frame, bg=COLOR_CARD)
        badges_frame.pack(side=tk.RIGHT)

        self.badge_status = tk.Label(badges_frame, text="", font=FONT_MONO_TINY, padx=5, pady=1)
        self.badge_status.pack(side=tk.RIGHT, padx=(4, 0))

        self.badge_sev = tk.Label(badges_frame, text="", font=FONT_MONO_TINY, padx=5, pady=1)
        self.badge_sev.pack(side=tk.RIGHT, padx=(4, 0))

        self.badge_type = tk.Label(badges_frame, text="", font=FONT_MONO_TINY, padx=5, pady=1)
        self.badge_type.pack(side=tk.RIGHT, padx=(4, 0))

        # Middle row: Rule, Confidence, Times
        mid_frame = tk.Frame(self, bg=COLOR_CARD)
        mid_frame.pack(fill=tk.X, pady=(0, 4))

        self.lbl_meta = tk.Label(mid_frame, text="", font=FONT_MONO_TINY, fg=COLOR_MUTED, bg=COLOR_CARD, anchor="w")
        self.lbl_meta.pack(fill=tk.X)

        # Timeline Box: Scrollable Consolas box
        self.timeline_text = tk.Text(
            self,
            height=3,
            bg="#0a1020",
            fg=COLOR_TEXT,
            font=FONT_MONO_TINY,
            relief="flat",
            wrap="none",
            highlightthickness=1,
            highlightbackground="#1c2d52",
        )
        self.timeline_text.pack(fill=tk.X, pady=(4, 0))
        self.timeline_text.config(state="disabled")

        # Recursive click binding so clicking anywhere on the card selects it
        self._bind_click(self)

    def _bind_click(self, widget: tk.Widget) -> None:
        widget.bind("<Button-1>", self._on_click)
        for child in widget.winfo_children():
            self._bind_click(child)

    def _on_click(self, _event=None) -> None:
        if self.incident_id and self.on_select_cb:
            self.on_select_cb(self.incident_id)

    def update_data(self, inc: Dict[str, Any], is_selected: bool) -> None:
        self.incident_id = inc.get("incident_id")
        cid = inc.get("centre_id", "")
        itype = (inc.get("type") or "").lower()
        sev = (inc.get("severity") or "info").lower()
        st = (inc.get("status") or "open").lower()

        # Selection outline: teal if selected
        self.config(
            highlightbackground=COLOR_TEAL if is_selected else COLOR_BORDER,
            highlightthickness=2 if is_selected else 1,
        )

        self.lbl_id.config(text=self.incident_id or "")
        self.lbl_centre.config(text=cid)

        # Type badge
        if "power" in itype:
            self.badge_type.config(text="POWER", bg="#361010", fg=COLOR_RED)
        elif "network" in itype:
            self.badge_type.config(text="NETWORK", bg="#122544", fg=COLOR_BLUE)
        else:
            self.badge_type.config(text=itype.upper(), bg=COLOR_PANEL, fg=COLOR_MUTED)

        # Severity badge
        if sev == "critical":
            self.badge_sev.config(text="CRITICAL", bg="#361010", fg=COLOR_RED)
        elif sev == "high":
            self.badge_sev.config(text="HIGH", bg="#2a1f0a", fg=COLOR_AMBER)
        else:
            self.badge_sev.config(text=sev.upper(), bg="#122544", fg=COLOR_BLUE)

        # Status badge
        if st == "open":
            self.badge_status.config(text="OPEN", bg="#361010", fg=COLOR_RED)
        elif st == "recovering":
            self.badge_status.config(text="RECOVERING", bg="#2a1f0a", fg=COLOR_AMBER)
        elif st == "resolved":
            self.badge_status.config(text="RESOLVED", bg="#0d2b20", fg=COLOR_GREEN)
        else:
            self.badge_status.config(text=st.upper(), bg=COLOR_PANEL, fg=COLOR_MUTED)

        # Detection rule & confidence
        rule = inc.get("detection_rule", "-")
        ev = inc.get("evidence") or {}
        conf = ev.get("confidence") if isinstance(ev, dict) else None
        conf_str = f" | conf: {int(conf*100)}%" if isinstance(conf, (int, float)) else (f" | conf: {conf}" if conf else "")

        det_at = inc.get("detected_at", "")
        det_ts = det_at[11:19] if len(det_at) >= 19 else det_at
        res_at = inc.get("resolved_at") or ""
        res_ts = res_at[11:19] if len(res_at) >= 19 else res_at

        dur_str = ""
        if det_at and res_at:
            try:
                t0 = datetime.fromisoformat(det_at.replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(res_at.replace("Z", "+00:00"))
                ds = max(0, int((t1 - t0).total_seconds()))
                dur_str = f" ({ds}s)" if ds < 60 else f" ({ds // 60}m {ds % 60}s)"
            except Exception:
                pass

        time_part = f"det: {det_ts}"
        if res_ts:
            time_part += f"  res: {res_ts}{dur_str}"

        self.lbl_meta.config(text=f"Rule: {rule}{conf_str}  |  {time_part}")

        # Timeline entries
        tl_entries = inc.get("timeline") or []
        tl_lines = []
        for t in tl_entries:
            ts_str = t.get("ts", "")[11:19] if len(t.get("ts", "")) >= 19 else t.get("ts", "")
            kind = t.get("kind", "")
            detail = t.get("detail", {})
            if isinstance(detail, dict):
                msg = detail.get("message") or detail.get("action") or detail.get("state") or str(detail)
            else:
                msg = str(detail)
            msg = msg.replace("\n", " ")[:60]
            tl_lines.append(f"{ts_str}  {kind}: {msg}")

        tl_content = "\n".join(tl_lines) if tl_lines else "No timeline entries recorded"
        self.timeline_text.config(state="normal")
        self.timeline_text.delete("1.0", tk.END)
        self.timeline_text.insert("1.0", tl_content)
        self.timeline_text.config(state="disabled")


class IncidentsTab(tk.Frame):
    """Right-hand panel tab showing latest 6 incidents as interactive cards."""

    def __init__(self, parent: tk.Widget, app: Any):
        super().__init__(parent, bg=COLOR_PANEL)
        self.app = app

        self.scroll_frame = ScrollableFrame(self, bg=COLOR_PANEL)
        self.scroll_frame.pack(fill=tk.BOTH, expand=True)

        self.empty_label = tk.Label(
            self.scroll_frame.inner,
            text="No incidents yet. Inject a fault from the floor view.",
            font=FONT_UI,
            fg=COLOR_MUTED,
            bg=COLOR_PANEL,
            pady=40,
        )
        self.empty_label.pack(fill=tk.X, expand=True)

        self.cards: List[IncidentCard] = []

    def _on_card_select(self, incident_id: str) -> None:
        self.app.state.selected_incident = incident_id
        self.app.trigger_impact_refresh(incident_id)
        self.app.floor_view.update_view(self.app.state)
        self.update_view(self.app.state)

    def update_view(self, state: AppState) -> None:
        incidents = state.incidents or []
        if not incidents:
            self.empty_label.pack(fill=tk.X, expand=True)
            for c in self.cards:
                c.pack_forget()
            return

        self.empty_label.pack_forget()

        # Auto-select latest incident if nothing is selected
        if not state.selected_incident:
            state.selected_incident = incidents[0].get("incident_id")
            if state.selected_incident:
                self.app.trigger_impact_refresh(state.selected_incident)
                self.app.floor_view.update_view(state)

        latest_6 = incidents[:6]
        while len(self.cards) < len(latest_6):
            c = IncidentCard(self.scroll_frame.inner, on_select_cb=self._on_card_select)
            self.cards.append(c)

        for i, inc in enumerate(latest_6):
            card = self.cards[i]
            is_sel = (inc.get("incident_id") == state.selected_incident)
            card.update_data(inc, is_selected=is_sel)
            card.pack(fill=tk.X, padx=10, pady=6)

        for i in range(len(latest_6), len(self.cards)):
            self.cards[i].pack_forget()


# ---------------------------------------------------------------------------
# OVERRIDE DECISION MODAL
# ---------------------------------------------------------------------------
class OverrideModal(tk.Toplevel):
    """Small dark modal dialog to override a candidate's remedy with required reason."""

    def __init__(self, parent: tk.Widget, app: Any, incident_id: str, candidate_row: Dict[str, Any], decided_by: str):
        super().__init__(parent)
        self.app = app
        self.incident_id = incident_id
        self.candidate_row = candidate_row
        self.decided_by = decided_by

        self.title("Override Remedy")
        self.geometry("420x360")
        self.resizable(False, False)
        self.configure(bg=COLOR_BG)
        self.transient(parent.winfo_toplevel())
        self.grab_set()

        cand_id = candidate_row.get("candidate_id", "")
        rec_remedy = candidate_row.get("remedy_recommended", "")

        # Header
        hdr = tk.Label(
            self,
            text=f"Override Decision: {cand_id}",
            font=FONT_UI_BOLD,
            fg=COLOR_TEXT,
            bg=COLOR_BG,
            pady=10,
        )
        hdr.pack(fill=tk.X)

        sub_hdr = tk.Label(
            self,
            text=f"Incident: {incident_id}  |  Recommended: {rec_remedy}",
            font=FONT_MONO_TINY,
            fg=COLOR_MUTED,
            bg=COLOR_BG,
        )
        sub_hdr.pack(fill=tk.X, pady=(0, 10))

        form_frame = tk.Frame(self, bg=COLOR_BG, padx=20)
        form_frame.pack(fill=tk.BOTH, expand=True)

        # Remedy selector
        tk.Label(form_frame, text="Remedy:", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg=COLOR_BG).grid(
            row=0, column=0, sticky="w", pady=4
        )
        self.remedy_var = tk.StringVar(value="extra_time" if rec_remedy != "extra_time" else "resume")
        self.remedy_combo = ttk.Combobox(
            form_frame,
            textvariable=self.remedy_var,
            values=["resume", "extra_time", "retest", "no_compensation"],
            state="readonly",
            width=18,
            font=FONT_UI_SMALL,
        )
        self.remedy_combo.grid(row=0, column=1, sticky="w", pady=4, padx=6)
        self.remedy_combo.bind("<<ComboboxSelected>>", self._on_remedy_changed)

        # Extra seconds (enabled only for extra_time)
        tk.Label(form_frame, text="Extra seconds:", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg=COLOR_BG).grid(
            row=1, column=0, sticky="w", pady=4
        )
        self.extra_sec_var = tk.StringVar(value="120")
        self.extra_sec_entry = tk.Entry(
            form_frame,
            textvariable=self.extra_sec_var,
            bg=COLOR_CARD,
            fg=COLOR_TEXT,
            insertbackground=COLOR_TEAL,
            relief="solid",
            borderwidth=1,
            width=10,
            font=FONT_MONO_SMALL,
        )
        self.extra_sec_entry.grid(row=1, column=1, sticky="w", pady=4, padx=6)

        # Reason Text
        tk.Label(form_frame, text="Reason (min 10 chars):", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg=COLOR_BG).grid(
            row=2, column=0, sticky="nw", pady=6
        )
        self.reason_text = tk.Text(
            form_frame,
            height=4,
            width=26,
            bg=COLOR_CARD,
            fg=COLOR_TEXT,
            insertbackground=COLOR_TEAL,
            font=FONT_UI_SMALL,
            relief="solid",
            borderwidth=1,
            wrap="word",
        )
        self.reason_text.insert("1.0", "Overridden after controller manual review")
        self.reason_text.grid(row=2, column=1, sticky="w", pady=6, padx=6)

        # Error label for verbatim errors
        self.error_lbl = tk.Label(
            self,
            text="",
            font=FONT_UI_TINY,
            fg=COLOR_RED,
            bg=COLOR_BG,
            wraplength=380,
            justify="center",
        )
        self.error_lbl.pack(fill=tk.X, padx=10, pady=(2, 6))

        # Action buttons
        btn_frame = tk.Frame(self, bg=COLOR_BG, pady=10)
        btn_frame.pack(fill=tk.X)

        self.btn_cancel = tk.Button(
            btn_frame,
            text="Cancel",
            font=FONT_UI_SMALL,
            bg=COLOR_CARD,
            fg=COLOR_MUTED,
            relief="solid",
            borderwidth=1,
            padx=12,
            pady=3,
            command=self.destroy,
        )
        self.btn_cancel.pack(side=tk.RIGHT, padx=16)

        self.btn_submit = tk.Button(
            btn_frame,
            text="Confirm Override",
            font=FONT_UI_SMALL,
            bg="#2a1f0a",
            fg=COLOR_AMBER,
            relief="solid",
            borderwidth=1,
            padx=14,
            pady=3,
            command=self._on_submit,
        )
        self.btn_submit.pack(side=tk.RIGHT, padx=6)

        self._on_remedy_changed()

    def _on_remedy_changed(self, _event=None) -> None:
        if self.remedy_var.get() == "extra_time":
            self.extra_sec_entry.config(state="normal")
            if not self.extra_sec_var.get() or self.extra_sec_var.get() == "0":
                self.extra_sec_var.set("120")
        else:
            self.extra_sec_entry.config(state="disabled")

    def _on_submit(self) -> None:
        remedy = self.remedy_var.get()
        cand_id = self.candidate_row.get("candidate_id", "")
        reason = self.reason_text.get("1.0", tk.END).strip()

        extra_s = 0
        if remedy == "extra_time":
            try:
                extra_s = int(self.extra_sec_var.get().strip())
            except ValueError:
                self.error_lbl.config(text="extra_time requires a valid integer for extra_seconds")
                return

        def _bg_post():
            try:
                payload = {
                    "incident_id": self.incident_id,
                    "candidate_id": cand_id,
                    "action": "override",
                    "remedy": remedy,
                    "extra_seconds": extra_s,
                    "decided_by": self.decided_by,
                    "reason": reason,
                }
                headers = {"X-Controller-Key": "demo-controller-key"}
                self.app.api_client.post("/v1/decisions", json=payload, headers=headers)

                ev_msg = f"override: {remedy} for {cand_id} by {self.decided_by}"
                if ev_msg not in self.app.state.decision_events:
                    self.app.state.decision_events.insert(0, ev_msg)

                self.app.queue.put((
                    "override_result",
                    {
                        "ok": True,
                        "cand_id": cand_id,
                        "remedy": remedy,
                        "inc_id": self.incident_id,
                        "modal": self,
                    },
                    time.time(),
                ))

            except ApiException as ae:
                self.app.queue.put((
                    "override_result",
                    {
                        "ok": False,
                        "err": ae.detail,
                        "modal": self,
                    },
                    time.time(),
                ))

            except Exception as ex:
                self.app.queue.put((
                    "override_result",
                    {
                        "ok": False,
                        "err": str(ex),
                        "modal": self,
                    },
                    time.time(),
                ))

        threading.Thread(target=_bg_post, daemon=True).start()


# ---------------------------------------------------------------------------
# IMPACT TAB
# ---------------------------------------------------------------------------
class ImpactTab(tk.Frame):
    """Right-hand panel tab displaying computed impact, summary tiles, fairness strip, Treeview, and decisions."""

    def __init__(self, parent: tk.Widget, app: Any):
        super().__init__(parent, bg=COLOR_PANEL, padx=12, pady=10)
        self.app = app
        self.selected_row_data: Optional[Dict[str, Any]] = None

        # 1. Unselected frame
        self.unselected_frame = tk.Frame(self, bg=COLOR_PANEL)
        lbl_unsel = tk.Label(
            self.unselected_frame,
            text="No incident selected. Select an incident from the Incidents tab.",
            font=FONT_UI,
            fg=COLOR_MUTED,
            bg=COLOR_PANEL,
            pady=40,
        )
        lbl_unsel.pack(expand=True)

        # 2. Waiting Frame (while incident is not resolved / impact not computed)
        self.waiting_frame = tk.Frame(self, bg=COLOR_PANEL)
        self.lbl_wait_title = tk.Label(
            self.waiting_frame,
            text="Impact is computed from logs after the incident resolves",
            font=FONT_UI_BOLD,
            fg=COLOR_TEXT,
            bg=COLOR_PANEL,
            wraplength=340,
            justify="center",
            pady=20,
        )
        self.lbl_wait_title.pack(fill=tk.X)
        self.lbl_wait_status = tk.Label(
            self.waiting_frame,
            text="Live Status: OPEN",
            font=FONT_MONO,
            fg=COLOR_RED,
            bg=COLOR_PANEL,
        )
        self.lbl_wait_status.pack(pady=(0, 20))

        # 3. Computed View Container
        self.computed_frame = tk.Frame(self, bg=COLOR_PANEL)

        # 3a. Summary Tiles (Exposed, Decided, Pending, Fairness)
        tiles_container = tk.Frame(self.computed_frame, bg=COLOR_PANEL)
        tiles_container.pack(fill=tk.X, pady=(0, 8))
        tiles_container.grid_columnconfigure((0, 1, 2, 3), weight=1)

        self.tile_vals: Dict[str, tk.Label] = {}
        tile_defs = [
            ("EXPOSED", COLOR_TEXT),
            ("DECIDED", COLOR_GREEN),
            ("PENDING", COLOR_AMBER),
            ("FAIRNESS", COLOR_GREEN),
        ]
        for idx, (title, default_col) in enumerate(tile_defs):
            t_box = tk.Frame(
                tiles_container,
                bg=COLOR_CARD,
                highlightthickness=1,
                highlightbackground=COLOR_BORDER,
                padx=6,
                pady=6,
            )
            t_box.grid(row=0, column=idx, sticky="nsew", padx=3)
            lbl_t = tk.Label(t_box, text=title, font=FONT_UI_TINY, fg=COLOR_MUTED, bg=COLOR_CARD)
            lbl_t.pack(anchor="center")
            lbl_v = tk.Label(t_box, text="-", font=FONT_UI_HEADING, fg=default_col, bg=COLOR_CARD)
            lbl_v.pack(anchor="center")
            self.tile_vals[title] = lbl_v

        # 3b. Fairness Strip
        self.fairness_strip = tk.Frame(
            self.computed_frame,
            bg="#0d182e",
            highlightthickness=1,
            highlightbackground="#1e2d4a",
            padx=8,
            pady=4,
        )
        self.fairness_strip.pack(fill=tk.X, pady=(0, 8))

        self.fairness_badge = tk.Label(
            self.fairness_strip,
            text="[OK]",
            font=FONT_MONO_TINY,
            fg=COLOR_GREEN,
            bg="#0d182e",
        )
        self.fairness_badge.pack(side=tk.LEFT, padx=(0, 6))

        self.fairness_text = tk.Label(
            self.fairness_strip,
            text="No disparity flags across centres",
            font=FONT_UI_TINY,
            fg=COLOR_TEXT,
            bg="#0d182e",
            anchor="w",
        )
        self.fairness_text.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # 3c. Treeview Table Frame
        tree_frame = tk.Frame(self.computed_frame, bg=COLOR_PANEL)
        tree_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 6))

        cols = ("candidate", "lost_s", "unsaved", "evidence", "rule", "remedy", "decision")
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", height=8)

        self.tree.heading("candidate", text="Candidate", anchor="w")
        self.tree.heading("lost_s", text="Lost (s)", anchor="center")
        self.tree.heading("unsaved", text="Unsaved", anchor="center")
        self.tree.heading("evidence", text="Evidence", anchor="center")
        self.tree.heading("rule", text="Rule", anchor="center")
        self.tree.heading("remedy", text="Remedy", anchor="w")
        self.tree.heading("decision", text="Decision", anchor="center")

        self.tree.column("candidate", width=105, minwidth=85, anchor="w")
        self.tree.column("lost_s", width=55, minwidth=45, anchor="center")
        self.tree.column("unsaved", width=55, minwidth=45, anchor="center")
        self.tree.column("evidence", width=65, minwidth=50, anchor="center")
        self.tree.column("rule", width=45, minwidth=40, anchor="center")
        self.tree.column("remedy", width=130, minwidth=100, anchor="w")
        self.tree.column("decision", width=75, minwidth=60, anchor="center")

        tree_scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scroll_y.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        tree_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        # Configure rule tag colors
        self.tree.tag_configure("R1", foreground="#34d399")
        self.tree.tag_configure("R2", foreground="#60a5fa")
        self.tree.tag_configure("R3", foreground="#fbbf24")
        self.tree.tag_configure("R4", foreground="#f87171")
        self.tree.tag_configure("decided", foreground="#64748b")

        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

        # 3d. Details Box (under table)
        self.details_box = tk.Frame(
            self.computed_frame,
            bg="#0a1224",
            highlightthickness=1,
            highlightbackground=COLOR_BORDER,
            padx=10,
            pady=6,
        )
        self.details_box.pack(fill=tk.X, pady=(0, 6))

        self.lbl_detail_header = tk.Label(
            self.details_box,
            text="Select a candidate to view evidence details",
            font=FONT_UI_BOLD,
            fg=COLOR_TEAL,
            bg="#0a1224",
            anchor="w",
        )
        self.lbl_detail_header.pack(fill=tk.X)

        self.lbl_detail_rationale = tk.Label(
            self.details_box,
            text="",
            font=FONT_UI_SMALL,
            fg=COLOR_TEXT,
            bg="#0a1224",
            anchor="w",
            wraplength=380,
            justify="left",
        )
        self.lbl_detail_rationale.pack(fill=tk.X, pady=(2, 4))

        self.evidence_text = tk.Text(
            self.details_box,
            height=3,
            bg="#070c18",
            fg=COLOR_MUTED,
            font=FONT_MONO_TINY,
            relief="flat",
            wrap="word",
            highlightthickness=0,
        )
        self.evidence_text.pack(fill=tk.X)
        self.evidence_text.config(state="disabled")

        # 3e. Human Decisions Action Bar
        action_bar = tk.Frame(self.computed_frame, bg=COLOR_PANEL)
        action_bar.pack(fill=tk.X, pady=(2, 0))

        # Top row: Decided by + Acknowledge fairness checkbox
        inputs_row = tk.Frame(action_bar, bg=COLOR_PANEL)
        inputs_row.pack(fill=tk.X, pady=(0, 6))

        tk.Label(inputs_row, text="Decided by:", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg=COLOR_PANEL).pack(side=tk.LEFT, padx=(0, 4))
        self.decided_by_var = tk.StringVar(value="controller.sharma")
        self.decided_by_entry = tk.Entry(
            inputs_row,
            textvariable=self.decided_by_var,
            bg=COLOR_CARD,
            fg=COLOR_TEXT,
            insertbackground=COLOR_TEAL,
            relief="solid",
            borderwidth=1,
            width=16,
            font=FONT_UI_SMALL,
        )
        self.decided_by_entry.pack(side=tk.LEFT, padx=(0, 10))

        self.ack_fairness_var = tk.BooleanVar(value=False)
        self.ack_cb = tk.Checkbutton(
            inputs_row,
            text="Acknowledge fairness flags",
            variable=self.ack_fairness_var,
            font=FONT_UI_TINY,
            fg=COLOR_TEXT,
            bg=COLOR_PANEL,
            selectcolor=COLOR_CARD,
            activebackground=COLOR_PANEL,
            activeforeground=COLOR_TEXT,
        )
        self.ack_cb.pack(side=tk.LEFT)

        # Button row: Approve all, Approve selected, Override selected
        btn_row = tk.Frame(action_bar, bg=COLOR_PANEL)
        btn_row.pack(fill=tk.X, pady=(0, 4))

        self.btn_approve_all = tk.Button(
            btn_row,
            text="Approve all recommended",
            font=FONT_UI_SMALL,
            bg="#0d2b20",
            fg=COLOR_GREEN,
            relief="solid",
            borderwidth=1,
            padx=10,
            pady=3,
            command=self._on_approve_all,
        )
        self.btn_approve_all.pack(side=tk.LEFT, padx=(0, 6))

        self.btn_approve_selected = tk.Button(
            btn_row,
            text="Approve selected",
            font=FONT_UI_SMALL,
            bg=COLOR_CARD,
            fg=COLOR_TEXT,
            relief="solid",
            borderwidth=1,
            padx=8,
            pady=3,
            state="disabled",
            command=self._on_approve_selected,
        )
        self.btn_approve_selected.pack(side=tk.LEFT, padx=(0, 6))

        self.btn_override_selected = tk.Button(
            btn_row,
            text="Override selected",
            font=FONT_UI_SMALL,
            bg=COLOR_CARD,
            fg=COLOR_AMBER,
            relief="solid",
            borderwidth=1,
            padx=8,
            pady=3,
            state="disabled",
            command=self._on_override_selected,
        )
        self.btn_override_selected.pack(side=tk.LEFT)

        # Muted Note
        lbl_note = tk.Label(
            action_bar,
            text="Manual-review cases are never auto-approved. A human decides; who and why are written to the audit chain.",
            font=FONT_UI_TINY,
            fg=COLOR_MUTED,
            bg=COLOR_PANEL,
            anchor="w",
            wraplength=380,
            justify="left",
        )
        lbl_note.pack(fill=tk.X, pady=(2, 0))

    def _on_tree_select(self, _event=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        cid = sel[0]
        self.app.state.selected_candidate = cid
        self.app.floor_view.update_view(self.app.state)
        self._update_candidate_details(cid)

    def _update_candidate_details(self, cand_id: str) -> None:
        inc_id = self.app.state.selected_incident
        impact = self.app.state.incident_impacts.get(inc_id) if inc_id else None
        if not impact or not impact.get("rows"):
            return

        row = next((r for r in impact["rows"] if r.get("candidate_id") == cand_id), None)
        self.selected_row_data = row
        if not row:
            return

        rule_id = row.get("rule_id", "-")
        self.lbl_detail_header.config(text=f"Candidate: {cand_id}  |  Rule: {rule_id}")
        self.lbl_detail_rationale.config(text=f"Rationale: {row.get('rationale', 'No rationale available')}")

        ev = row.get("evidence") or {}
        ev_json = json.dumps(ev, indent=2)[:400]
        self.evidence_text.config(state="normal")
        self.evidence_text.delete("1.0", tk.END)
        self.evidence_text.insert("1.0", ev_json)
        self.evidence_text.config(state="disabled")

        # Update button states
        dec_info = row.get("decision") or {}
        dec_status = dec_info.get("status", "pending")
        rec_rem = row.get("remedy_recommended")

        # Approve selected disabled if manual_review or already decided
        if rec_rem == "manual_review" or dec_status in ("approved", "overridden"):
            self.btn_approve_selected.config(state="disabled", fg=COLOR_MUTED)
        else:
            self.btn_approve_selected.config(state="normal", fg=COLOR_TEXT)

        # Override selected enabled whenever a row is selected
        self.btn_override_selected.config(state="normal")

    def _on_approve_all(self) -> None:
        inc_id = self.app.state.selected_incident
        if not inc_id:
            return
        decided_by = self.decided_by_var.get().strip() or "controller.sharma"
        ack_fairness = bool(self.ack_fairness_var.get())

        def _bg():
            try:
                payload = {
                    "decided_by": decided_by,
                    "reason": "Approved recommended remedies after reviewing evidence",
                    "acknowledge_fairness": ack_fairness,
                }
                headers = {"X-Controller-Key": "demo-controller-key"}
                resp = self.app.api_client.post(
                    f"/v1/incidents/{inc_id}/decisions/approve-all",
                    json=payload,
                    headers=headers,
                )
                appr = resp.get("approved", 0)
                skip_mr = resp.get("skipped_manual_review", 0)

                ev_msg = f"approved: {appr} remedies by {decided_by}"
                if ev_msg not in self.app.state.decision_events:
                    self.app.state.decision_events.insert(0, ev_msg)

                toast_msg = f"Approved {appr} remedies ({skip_mr} left for manual review)"
                self.app.queue.put(("decision_result", {"ok": True, "msg": toast_msg, "inc_id": inc_id}, time.time()))

            except ApiException as ae:
                self.app.queue.put(("decision_result", {"ok": False, "msg": ae.detail, "inc_id": inc_id}, time.time()))

            except Exception as ex:
                self.app.queue.put(("decision_result", {"ok": False, "msg": str(ex), "inc_id": inc_id}, time.time()))

        threading.Thread(target=_bg, daemon=True).start()

    def _on_approve_selected(self) -> None:
        if not self.selected_row_data:
            return
        inc_id = self.app.state.selected_incident
        cand_id = self.selected_row_data.get("candidate_id")
        decided_by = self.decided_by_var.get().strip() or "controller.sharma"

        def _bg():
            try:
                payload = {
                    "incident_id": inc_id,
                    "candidate_id": cand_id,
                    "action": "approve",
                    "expected_rule_id": self.selected_row_data.get("rule_id"),
                    "expected_extra_seconds": self.selected_row_data.get("extra_seconds", 0),
                    "decided_by": decided_by,
                }
                headers = {"X-Controller-Key": "demo-controller-key"}
                self.app.api_client.post("/v1/decisions", json=payload, headers=headers)

                ev_msg = f"approved: 1 remedy ({cand_id}) by {decided_by}"
                if ev_msg not in self.app.state.decision_events:
                    self.app.state.decision_events.insert(0, ev_msg)

                self.app.queue.put(("decision_result", {"ok": True, "msg": f"Approved remedy for {cand_id}", "inc_id": inc_id}, time.time()))

            except ApiException as ae:
                self.app.queue.put(("decision_result", {"ok": False, "msg": ae.detail, "inc_id": inc_id}, time.time()))

            except Exception as ex:
                self.app.queue.put(("decision_result", {"ok": False, "msg": str(ex), "inc_id": inc_id}, time.time()))

        threading.Thread(target=_bg, daemon=True).start()

    def _on_override_selected(self) -> None:
        if not self.selected_row_data:
            return
        inc_id = self.app.state.selected_incident
        decided_by = self.decided_by_var.get().strip() or "controller.sharma"
        OverrideModal(self, self.app, inc_id, self.selected_row_data, decided_by)

    def update_view(self, state: AppState) -> None:
        inc_id = state.selected_incident
        if not inc_id:
            self.unselected_frame.pack(fill=tk.BOTH, expand=True)
            self.waiting_frame.pack_forget()
            self.computed_frame.pack_forget()
            return

        self.unselected_frame.pack_forget()

        inc_obj = next((i for i in state.incidents if i.get("incident_id") == inc_id), None)
        inc_status = (inc_obj.get("status") or "open").lower() if inc_obj else "open"
        impact = state.incident_impacts.get(inc_id)

        # If incident not resolved or computed_at null or rows empty
        is_computed = (
            inc_status == "resolved"
            and impact is not None
            and impact.get("computed_at") is not None
            and len(impact.get("rows", [])) > 0
        )

        if not is_computed:
            self.computed_frame.pack_forget()
            self.waiting_frame.pack(fill=tk.BOTH, expand=True)

            status_col = COLOR_RED if inc_status == "open" else (COLOR_AMBER if inc_status == "recovering" else COLOR_GREEN)
            self.lbl_wait_status.config(
                text=f"Live Incident Status: {inc_status.upper()}",
                fg=status_col,
            )
            return

        self.waiting_frame.pack_forget()
        self.computed_frame.pack(fill=tk.BOTH, expand=True)

        # 1. Summary Tiles
        summary = impact.get("summary") or {}
        exp = summary.get("exposed", 0)
        dec = summary.get("decided", 0)
        pend = summary.get("pending_decisions", max(0, exp - dec))
        fair_st = summary.get("fairness_status", "ok")

        self.tile_vals["EXPOSED"].config(text=str(exp))
        self.tile_vals["DECIDED"].config(text=str(dec))
        self.tile_vals["PENDING"].config(text=str(pend))
        self.tile_vals["FAIRNESS"].config(
            text=str(fair_st).upper(),
            fg=COLOR_GREEN if str(fair_st).lower() == "ok" else COLOR_AMBER,
        )

        # 2. Fairness Strip
        fairness_data = state.fairness or {}
        flags = fairness_data.get("flags") or []
        if flags:
            flag_msgs = [f.get("message", "") for f in flags if isinstance(f, dict)]
            self.fairness_badge.config(text="[FLAGGED]", fg=COLOR_AMBER)
            self.fairness_text.config(text=" | ".join(flag_msgs), fg=COLOR_AMBER)
        else:
            self.fairness_badge.config(text="[OK]", fg=COLOR_GREEN)
            self.fairness_text.config(text="No disparity flags across centres", fg=COLOR_TEXT)

        # 3. Treeview Table (Build once, update in place by candidate_id iid)
        rows = sorted(impact.get("rows", []), key=lambda r: r.get("candidate_id", ""))
        existing_iids = set(self.tree.get_children())
        seen_iids = set()

        for r in rows:
            cid = r.get("candidate_id", "")
            seen_iids.add(cid)

            lost_s = str(r.get("lost_seconds", 0))
            unsaved = str(r.get("unsaved_answers", 0))
            ev_q = str(r.get("evidence_quality", "-"))
            rule_id = str(r.get("rule_id", "-"))

            rec_rem = str(r.get("remedy_recommended", "-"))
            if rec_rem == "extra_time" and r.get("extra_seconds"):
                rec_rem = f"extra_time (+{r['extra_seconds']}s)"

            dec_info = r.get("decision") or {}
            dec_status = dec_info.get("status", "pending")

            tag = "decided" if dec_status in ("approved", "overridden") else (rule_id if rule_id in ("R1", "R2", "R3", "R4") else "R1")
            vals = (cid, lost_s, unsaved, ev_q, rule_id, rec_rem, dec_status)

            if cid in existing_iids:
                self.tree.item(cid, values=vals, tags=(tag,))
            else:
                self.tree.insert("", "end", iid=cid, values=vals, tags=(tag,))

        for old_iid in existing_iids - seen_iids:
            self.tree.delete(old_iid)

        # Synchronize candidate selection from AppState
        if state.selected_candidate and self.tree.exists(state.selected_candidate):
            curr_sel = self.tree.selection()
            if not curr_sel or curr_sel[0] != state.selected_candidate:
                self.tree.selection_set(state.selected_candidate)
                self.tree.see(state.selected_candidate)
                self._update_candidate_details(state.selected_candidate)
        elif not self.tree.selection() and rows:
            first_cid = rows[0].get("candidate_id")
            if first_cid and self.tree.exists(first_cid):
                self.tree.selection_set(first_cid)
                state.selected_candidate = first_cid
                self._update_candidate_details(first_cid)


# ---------------------------------------------------------------------------
# NOTEBOOK PANEL (RIGHT 30%)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# CANDIDATE STATUS TAB
# ---------------------------------------------------------------------------
class CandidateTab(tk.Frame):
    """Phone-style candidate status card with live polling."""

    POLL_MS = 2000  # 2 second refresh

    def __init__(self, parent: tk.Widget, app: Any):
        super().__init__(parent, bg=COLOR_PANEL)
        self.app = app
        self._current_cand: Optional[str] = None
        self._poll_after_id: Optional[str] = None
        self._last_status: Optional[Dict[str, Any]] = None
        self._last_notices: Optional[List[Dict[str, Any]]] = None
        self._widgets_built = False

        # --- Top lookup bar ---
        lookup_bar = tk.Frame(self, bg=COLOR_PANEL, pady=6, padx=8)
        lookup_bar.pack(fill=tk.X)
        tk.Label(lookup_bar, text="Candidate:", font=FONT_UI_SMALL, fg=COLOR_MUTED, bg=COLOR_PANEL).pack(side=tk.LEFT)
        self.lookup_var = tk.StringVar()
        self.lookup_entry = tk.Entry(
            lookup_bar, textvariable=self.lookup_var, font=FONT_MONO_SMALL,
            bg=COLOR_CARD, fg=COLOR_TEXT, insertbackground=COLOR_TEAL,
            borderwidth=1, relief="solid", width=18,
        )
        self.lookup_entry.pack(side=tk.LEFT, padx=(4, 4))
        self.lookup_entry.bind("<Return>", self._on_lookup)
        self.lookup_btn = tk.Button(
            lookup_bar, text="Look up", font=FONT_UI_SMALL,
            bg=COLOR_CARD, fg=COLOR_TEXT, activebackground=COLOR_BORDER,
            activeforeground=COLOR_TEAL, borderwidth=1, relief="solid",
            command=self._on_lookup,
        )
        self.lookup_btn.pack(side=tk.LEFT)
        self.error_lbl = tk.Label(lookup_bar, text="", font=FONT_UI_SMALL, fg=COLOR_RED, bg=COLOR_PANEL)
        self.error_lbl.pack(side=tk.LEFT, padx=(8, 0))

        # --- Scrollable card area ---
        self.card_scroll = ScrollableFrame(self, bg=COLOR_PANEL)
        self.card_scroll.pack(fill=tk.BOTH, expand=True)
        self.card_container = self.card_scroll.inner

        # --- Build persistent card widgets (hidden until data arrives) ---
        self._build_card_widgets()

    # -----------------------------------------------------------------------
    # WIDGET CONSTRUCTION (once)
    # -----------------------------------------------------------------------
    def _build_card_widgets(self) -> None:
        """Create all card widgets once; update text in place on refresh."""
        c = self.card_container

        # Outer card frame (phone-style, ~340px, centred)
        self.card_outer = tk.Frame(c, bg=COLOR_PANEL)
        self.card_outer.pack(fill=tk.X, pady=(8, 4), padx=8)

        # Teal top border bar
        self.teal_bar = tk.Frame(self.card_outer, bg=COLOR_TEAL, height=3)
        self.teal_bar.pack(fill=tk.X)

        # Dark card body
        self.card_body = tk.Frame(self.card_outer, bg=COLOR_CARD, padx=14, pady=10,
                                  highlightthickness=1, highlightbackground=COLOR_BORDER)
        self.card_body.pack(fill=tk.X)

        # Title line
        self.lbl_title = tk.Label(self.card_body, text="Candidate status", font=FONT_UI_BOLD,
                                   fg=COLOR_TEAL, bg=COLOR_CARD, anchor="w")
        self.lbl_title.pack(fill=tk.X)

        self.lbl_ids = tk.Label(self.card_body, text="", font=FONT_MONO_SMALL,
                                 fg=COLOR_MUTED, bg=COLOR_CARD, anchor="w")
        self.lbl_ids.pack(fill=tk.X, pady=(0, 6))

        # Big headline
        self.lbl_headline = tk.Label(self.card_body, text="", font=("Segoe UI", 11),
                                      fg=COLOR_TEXT, bg=COLOR_CARD, anchor="w",
                                      wraplength=310, justify="left")
        self.lbl_headline.pack(fill=tk.X, pady=(0, 10))

        # Vertical timeline container
        self.timeline_frame = tk.Frame(self.card_body, bg=COLOR_CARD)
        self.timeline_frame.pack(fill=tk.X, pady=(0, 8))

        # We build 3 timeline steps: Incident detected, Centre recovered, Remedy decided
        self.tl_steps: List[Dict[str, Any]] = []
        tl_labels = ["Incident detected", "Centre recovered", "Remedy decided"]
        for idx, label in enumerate(tl_labels):
            row_frame = tk.Frame(self.timeline_frame, bg=COLOR_CARD)
            row_frame.pack(fill=tk.X, pady=1)

            # Dot canvas (small, for a filled/hollow dot)
            dot_cvs = tk.Canvas(row_frame, width=16, height=16, bg=COLOR_CARD, highlightthickness=0)
            dot_cvs.pack(side=tk.LEFT, padx=(0, 4))
            dot_id = dot_cvs.create_oval(4, 4, 12, 12, fill=COLOR_BORDER, outline=COLOR_MUTED, width=1)

            # Connecting line below (except last)
            line_cvs = None
            if idx < len(tl_labels) - 1:
                line_cvs = tk.Canvas(self.timeline_frame, width=16, height=10, bg=COLOR_CARD, highlightthickness=0)
                line_cvs.pack(fill=tk.X, before=None)
                line_cvs.create_line(8, 0, 8, 10, fill=COLOR_BORDER, width=1, dash=(2, 2))

            lbl = tk.Label(row_frame, text=label, font=FONT_UI_SMALL, fg=COLOR_MUTED, bg=COLOR_CARD, anchor="w")
            lbl.pack(side=tk.LEFT, padx=(0, 6))

            time_lbl = tk.Label(row_frame, text="", font=FONT_MONO_TINY, fg=COLOR_MUTED, bg=COLOR_CARD, anchor="w")
            time_lbl.pack(side=tk.LEFT)

            self.tl_steps.append({
                "dot_cvs": dot_cvs,
                "dot_id": dot_id,
                "label": lbl,
                "time_lbl": time_lbl,
                "line_cvs": line_cvs,
            })

        # Separator
        tk.Frame(self.card_body, bg=COLOR_BORDER, height=1).pack(fill=tk.X, pady=(4, 6))

        # Notices header
        self.lbl_notices_hdr = tk.Label(self.card_body, text="Notices", font=FONT_UI_BOLD,
                                         fg=COLOR_MUTED, bg=COLOR_CARD, anchor="w")
        self.lbl_notices_hdr.pack(fill=tk.X)

        # Notices text (scrollable, read-only)
        self.notices_text = tk.Text(
            self.card_body, height=5, font=FONT_MONO_TINY, bg="#0b1426", fg=COLOR_TEXT,
            wrap="word", borderwidth=1, relief="solid", highlightthickness=0,
            insertbackground=COLOR_TEAL, state="disabled",
        )
        self.notices_text.pack(fill=tk.X, pady=(2, 6))

        # Separator
        tk.Frame(self.card_body, bg=COLOR_BORDER, height=1).pack(fill=tk.X, pady=(0, 6))

        # Raw fields block (Consolas, muted)
        self.lbl_raw_hdr = tk.Label(self.card_body, text="Raw session fields", font=FONT_UI_TINY_BOLD,
                                     fg=COLOR_MUTED, bg=COLOR_CARD, anchor="w")
        self.lbl_raw_hdr.pack(fill=tk.X)

        self.raw_text = tk.Text(
            self.card_body, height=4, font=FONT_MONO_TINY, bg="#0b1426", fg=COLOR_MUTED,
            wrap="word", borderwidth=1, relief="solid", highlightthickness=0,
            insertbackground=COLOR_TEAL, state="disabled",
        )
        self.raw_text.pack(fill=tk.X, pady=(2, 6))

        # Privacy footer
        self.lbl_footer = tk.Label(self.card_body, text="Pseudonymous id only. No personal data is stored.",
                                    font=FONT_UI_TINY, fg=COLOR_MUTED, bg=COLOR_CARD, anchor="w")
        self.lbl_footer.pack(fill=tk.X, pady=(2, 0))

        # Initially hidden until a candidate is selected
        self.card_outer.pack_forget()

        # Empty state label
        self.lbl_empty = tk.Label(
            c, text="Select a workstation or look up a candidate ID above.",
            font=FONT_UI, fg=COLOR_MUTED, bg=COLOR_PANEL,
        )
        self.lbl_empty.pack(expand=True)

        self._widgets_built = True

    # -----------------------------------------------------------------------
    # LOOKUP
    # -----------------------------------------------------------------------
    def _on_lookup(self, _event=None) -> None:
        raw = self.lookup_var.get().strip()
        if not raw:
            return
        self.error_lbl.config(text="")
        if self.app:
            self.app.state.selected_candidate = raw
            self.app.floor_view.update_view(self.app.state)
        self._set_candidate(raw)

    # -----------------------------------------------------------------------
    # CANDIDATE CHANGE
    # -----------------------------------------------------------------------
    def _set_candidate(self, cand_id: Optional[str]) -> None:
        if cand_id == self._current_cand:
            return
        self._current_cand = cand_id
        self._last_status = None
        self._last_notices = None
        self.error_lbl.config(text="")

        if not cand_id:
            self.card_outer.pack_forget()
            self.lbl_empty.pack(expand=True)
            self._cancel_poll()
            return

        # Show card, hide empty
        try:
            self.lbl_empty.pack_forget()
        except Exception:
            pass
        self.card_outer.pack(fill=tk.X, pady=(8, 4), padx=8)

        # Clear stale text
        self.lbl_ids.config(text=cand_id)
        self.lbl_headline.config(text="Loading...")
        for step in self.tl_steps:
            step["time_lbl"].config(text="")
            step["dot_cvs"].itemconfig(step["dot_id"], fill=COLOR_BORDER, outline=COLOR_MUTED)
        self.notices_text.config(state="normal")
        self.notices_text.delete("1.0", tk.END)
        self.notices_text.config(state="disabled")
        self.raw_text.config(state="normal")
        self.raw_text.delete("1.0", tk.END)
        self.raw_text.config(state="disabled")

        # Trigger immediate fetch
        self._do_poll()

    # -----------------------------------------------------------------------
    # POLLING
    # -----------------------------------------------------------------------
    def _cancel_poll(self) -> None:
        if self._poll_after_id:
            try:
                self.after_cancel(self._poll_after_id)
            except Exception:
                pass
            self._poll_after_id = None

    def _schedule_poll(self) -> None:
        self._cancel_poll()
        self._poll_after_id = self.after(self.POLL_MS, self._do_poll)

    def _do_poll(self) -> None:
        cand_id = self._current_cand
        if not cand_id or not self.app:
            return
        api = self.app.api_client

        def _bg():
            try:
                status = api.get(f"/v1/status/{cand_id}")
                notices = api.get(f"/v1/notices?audience=candidate&target_id={cand_id}")
                self.app.queue.put(("candidate_status", {"cand_id": cand_id, "status": status, "notices": notices}, time.time()))
            except ApiException as ae:
                self.app.queue.put(("candidate_status", {"cand_id": cand_id, "error": ae.detail}, time.time()))
            except Exception as ex:
                log_ascii(f"CandidateTab poll error for {cand_id}", ex)
                # Keep last good data; schedule next poll
                self.app.queue.put(("candidate_status", {"cand_id": cand_id, "poll_error": True}, time.time()))

        threading.Thread(target=_bg, daemon=True, name="CandStatusPoll").start()

    def _handle_poll_result(self, data: Dict[str, Any]) -> None:
        """Called on the main thread from _drain_queue_loop."""
        cand_id = data.get("cand_id")
        if cand_id != self._current_cand:
            return  # Stale response from a previous candidate

        error = data.get("error")
        if error:
            self.error_lbl.config(text=error)
            self.card_outer.pack_forget()
            try:
                self.lbl_empty.pack_forget()
            except Exception:
                pass
            self.lbl_empty.config(text=error)
            self.lbl_empty.pack(expand=True)
            # Don't schedule more polls for a 404
            return

        if data.get("poll_error"):
            # Keep last good card, just schedule next poll
            self._schedule_poll()
            return

        status = data.get("status", {})
        notices = data.get("notices", [])
        self._last_status = status
        self._last_notices = notices
        self._render_card(status, notices)
        self._schedule_poll()

    # -----------------------------------------------------------------------
    # RENDERING (update in place)
    # -----------------------------------------------------------------------
    def _render_card(self, status: Dict[str, Any], notices: List[Dict[str, Any]]) -> None:
        cand_id = status.get("candidate_id", "")
        centre_id = status.get("centre_id", "")
        session_state = status.get("session_state", "")
        incident = status.get("incident")
        remedy = status.get("remedy", {})
        remedy_status = remedy.get("status", "none")
        decision = remedy.get("decision")
        latest_message = status.get("latest_message", "")

        # --- IDs line ---
        self.lbl_ids.config(text=f"{cand_id}  |  {centre_id}")

        # --- Headline ---
        headline = self._build_headline(incident, remedy_status, decision, latest_message)
        self.lbl_headline.config(text=headline)

        # --- Timeline ---
        self._render_timeline(incident, decision, status)

        # --- Notices ---
        self._render_notices(notices)

        # --- Raw fields ---
        self._render_raw(status)

    def _build_headline(self, incident: Optional[Dict], remedy_status: str,
                        decision: Optional[Dict], latest_message: str) -> str:
        """Choose headline from real API fields. Use latest_message from the API when available,
        fall back to structured headlines."""
        if not incident:
            # Use the API's latest_message if it's meaningful
            if latest_message:
                return latest_message
            return "Your exam is running normally"

        inc_status = incident.get("status", "")

        if remedy_status == "decided" and decision:
            remedy_name = decision.get("remedy", "")
            extra_s = decision.get("extra_seconds") or 0
            if remedy_name == "resume":
                return "Your remedy: resume with restored time"
            elif remedy_name == "extra_time":
                mins = extra_s // 60
                secs = extra_s % 60
                if secs > 0:
                    return f"Your remedy: extra time of {mins} minutes and {secs} seconds"
                return f"Your remedy: extra time of {mins} minutes"
            elif remedy_name == "retest":
                return "Your remedy: retest scheduled"
            elif remedy_name == "no_compensation":
                return "Your remedy: no compensation required"
            else:
                return f"Your remedy: {remedy_name}"

        if remedy_status == "under_review":
            return "Your case is being reviewed by the exam controller"

        if remedy_status == "awaiting_decision":
            if latest_message:
                return latest_message
            return "Your exam was interrupted. Your remedy is being determined."

        if inc_status in ("open", "recovering"):
            if latest_message:
                return latest_message
            return "Your exam was interrupted. Please wait for instructions."

        # Resolved but no impact computed yet
        if latest_message:
            return latest_message
        return "Your exam was interrupted. The exam team is investigating."

    def _render_timeline(self, incident: Optional[Dict], decision: Optional[Dict],
                         status: Dict[str, Any]) -> None:
        """Update timeline dots and time labels."""
        # Step 0: Incident detected
        if incident:
            started = incident.get("started_at", "")
            ts_str = self._fmt_time(started)
            self.tl_steps[0]["time_lbl"].config(text=ts_str, fg=COLOR_TEXT)
            self.tl_steps[0]["dot_cvs"].itemconfig(self.tl_steps[0]["dot_id"],
                                                    fill=COLOR_TEAL, outline=COLOR_TEAL)

            # Step 1: Centre recovered
            ended = incident.get("ended_at", "")
            inc_status = incident.get("status", "")
            if ended and inc_status == "resolved":
                ts_str2 = self._fmt_time(ended)
                self.tl_steps[1]["time_lbl"].config(text=ts_str2, fg=COLOR_TEXT)
                self.tl_steps[1]["dot_cvs"].itemconfig(self.tl_steps[1]["dot_id"],
                                                        fill=COLOR_TEAL, outline=COLOR_TEAL)
            else:
                self.tl_steps[1]["time_lbl"].config(text="pending", fg=COLOR_MUTED)
                self.tl_steps[1]["dot_cvs"].itemconfig(self.tl_steps[1]["dot_id"],
                                                        fill="", outline=COLOR_MUTED)

            # Step 2: Remedy decided
            remedy = status.get("remedy", {})
            if remedy.get("status") == "decided" and decision:
                self.tl_steps[2]["time_lbl"].config(text="decided", fg=COLOR_TEXT)
                self.tl_steps[2]["dot_cvs"].itemconfig(self.tl_steps[2]["dot_id"],
                                                        fill=COLOR_TEAL, outline=COLOR_TEAL)
            elif remedy.get("status") == "under_review":
                self.tl_steps[2]["time_lbl"].config(text="under review", fg=COLOR_AMBER)
                self.tl_steps[2]["dot_cvs"].itemconfig(self.tl_steps[2]["dot_id"],
                                                        fill="", outline=COLOR_AMBER)
            else:
                self.tl_steps[2]["time_lbl"].config(text="pending", fg=COLOR_MUTED)
                self.tl_steps[2]["dot_cvs"].itemconfig(self.tl_steps[2]["dot_id"],
                                                        fill="", outline=COLOR_MUTED)
        else:
            # No incident: all grey/hidden
            for step in self.tl_steps:
                step["time_lbl"].config(text="", fg=COLOR_MUTED)
                step["dot_cvs"].itemconfig(step["dot_id"], fill=COLOR_BORDER, outline=COLOR_MUTED)

    def _render_notices(self, notices: List[Dict[str, Any]]) -> None:
        # Newest first
        sorted_notices = sorted(notices, key=lambda n: n.get("notice_id", 0), reverse=True)
        lines = []
        for n in sorted_notices:
            ts = self._fmt_time(n.get("created_at", ""))
            msg = n.get("message", "")
            lines.append(f"[{ts}] {msg}")
        content = "\n".join(lines) if lines else "No notices"
        self.notices_text.config(state="normal")
        self.notices_text.delete("1.0", tk.END)
        self.notices_text.insert("1.0", content)
        self.notices_text.config(state="disabled")

    def _render_raw(self, status: Dict[str, Any]) -> None:
        fields = [
            f"session_id      : {status.get('session_id', '-')}",
            f"session_state   : {status.get('session_state', '-')}",
            f"last_save_seq   : {status.get('last_confirmed_save_seq', 0)}",
            f"remedy_status   : {status.get('remedy', {}).get('status', 'none')}",
            f"generated_at    : {self._fmt_time(status.get('generated_at', ''))}",
        ]
        content = "\n".join(fields)
        self.raw_text.config(state="normal")
        self.raw_text.delete("1.0", tk.END)
        self.raw_text.insert("1.0", content)
        self.raw_text.config(state="disabled")

    @staticmethod
    def _fmt_time(iso_str: str) -> str:
        if not iso_str:
            return ""
        try:
            dt = datetime.fromisoformat(iso_str)
            return dt.strftime("%H:%M:%S")
        except Exception:
            return iso_str[:19]

    # -----------------------------------------------------------------------
    # PUBLIC: called from update_view
    # -----------------------------------------------------------------------
    def update_view(self, state: AppState) -> None:
        cand = state.selected_candidate
        if cand != self._current_cand:
            self.error_lbl.config(text="")
            self._set_candidate(cand)
            if cand:
                self.lookup_var.set(cand)
        # Only actively poll when the Candidate tab is visible
        if state.selected_tab != "Candidate":
            self._cancel_poll()


class NotebookPanel(tk.Frame):
    """Right tabbed inspection panel: Incidents, Impact, Audit, Candidate."""

    def __init__(self, parent: tk.Widget, app: Any = None):
        super().__init__(parent, bg=COLOR_BG)
        self.app = app
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Tab 1: Incidents Tab
        self.incidents_tab = IncidentsTab(self.notebook, app=self.app)
        self.notebook.add(self.incidents_tab, text="Incidents")

        # Tab 2: Impact Tab
        self.impact_tab = ImpactTab(self.notebook, app=self.app)
        self.notebook.add(self.impact_tab, text="Impact")

        # Tab 3: Audit (Placeholder for next stage)
        self.audit_frame = tk.Frame(self.notebook, bg=COLOR_PANEL, padx=16, pady=16)
        self.notebook.add(self.audit_frame, text="Audit")
        lbl_audit = tk.Label(
            self.audit_frame,
            text="Audit panel coming soon...",
            font=FONT_UI,
            fg=COLOR_MUTED,
            bg=COLOR_PANEL,
        )
        lbl_audit.pack(expand=True)

        # Tab 4: Candidate Status Tab
        self.candidate_tab = CandidateTab(self.notebook, app=self.app)
        self.notebook.add(self.candidate_tab, text="Candidate")

        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

    def _on_tab_changed(self, _event=None) -> None:
        sel_idx = self.notebook.index(self.notebook.select())
        tab_names = ["Incidents", "Impact", "Audit", "Candidate"]
        if sel_idx < len(tab_names) and self.app:
            self.app.state.selected_tab = tab_names[sel_idx]
            if tab_names[sel_idx] == "Impact" and self.app.state.selected_incident:
                self.app.trigger_impact_refresh(self.app.state.selected_incident)

    def select_candidate_tab(self) -> None:
        """Programmatically switch to the Candidate tab."""
        try:
            self.notebook.select(self.candidate_tab)
        except Exception:
            pass

    def update_view(self, state: AppState) -> None:
        self.incidents_tab.update_view(state)
        self.impact_tab.update_view(state)
        self.candidate_tab.update_view(state)


# ---------------------------------------------------------------------------
# BOTTOM TICKER STRIP
# ---------------------------------------------------------------------------
class TickerStrip(tk.Frame):
    """Single-line live timeline event ticker across the bottom of the window."""

    def __init__(self, parent: tk.Widget):
        super().__init__(parent, bg=COLOR_PANEL, height=28, padx=8, highlightthickness=1, highlightbackground=COLOR_BORDER)
        self.pack_propagate(False)

        self.badge = tk.Label(
            self,
            text="TIMELINE",
            font=("Consolas", 8, "bold"),
            fg=COLOR_TEAL,
            bg=COLOR_CARD,
            padx=6,
            pady=1,
        )
        self.badge.pack(side=tk.LEFT, padx=(0, 8))

        self.ticker_label = tk.Label(
            self,
            text="No incidents yet",
            font=FONT_MONO_SMALL,
            fg=COLOR_TEXT,
            bg=COLOR_PANEL,
            anchor="w",
        )
        self.ticker_label.pack(side=tk.LEFT, fill=tk.X, expand=True)

    def update_view(self, state: AppState) -> None:
        """Gather latest incident timeline entries across all incidents and render them newest first."""
        formatted: List[str] = []

        # 1. New decision activity
        for d_msg in state.decision_events:
            formatted.append(f"[DECISION] {d_msg}")

        # 2. Timeline entries
        all_entries: List[Dict[str, Any]] = []
        for inc in state.incidents:
            inc_id = inc.get("incident_id", "INC")
            centre_id = inc.get("centre_id", "CTR")
            for t in inc.get("timeline", []):
                all_entries.append({
                    "ts": t.get("ts", ""),
                    "incident_id": inc_id,
                    "centre_id": centre_id,
                    "kind": t.get("kind", ""),
                    "detail": t.get("detail", {}),
                })

        if not formatted and not all_entries:
            self.ticker_label.config(text="No incidents yet")
            return

        all_entries.sort(key=lambda e: e["ts"], reverse=True)
        for e in all_entries[:6]:
            ts_str = e["ts"][11:19] if len(e["ts"]) >= 19 else e["ts"]
            detail = e["detail"]
            if isinstance(detail, dict):
                msg = detail.get("message") or detail.get("action") or detail.get("state") or str(detail)
            else:
                msg = str(detail)
            formatted.append(f"[{ts_str}] {e['incident_id']} ({e['centre_id']}) {e['kind']}: {msg}")

        self.ticker_label.config(text="  |  ".join(formatted))


# ---------------------------------------------------------------------------
# MAIN APPLICATION WINDOW
# ---------------------------------------------------------------------------
class ControlTowerApp:
    """Root application class managing layout, styling, and safe queue draining."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("ERCT Control Tower")
        self.root.geometry("1280x720")
        self.root.minsize(1024, 600)
        self.root.configure(bg=COLOR_BG)

        # Safe exception handling for all Tk callbacks
        self.root.report_callback_exception = self._on_tk_exception

        # Style customization
        self.style = ttk.Style(self.root)
        apply_dark_theme(self.style)

        # State and background poller
        self.state = AppState()
        self.queue: queue.Queue = queue.Queue()
        self.api_client = ApiClient()
        self.poller = Poller(self.api_client, self.queue, self.state)
        self.is_closing = False

        # Build View Hierarchy
        self._build_layout()

        # Protocol handlers
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # Hotkeys for clean recording takes
        self.root.bind("<F3>", self._on_f3)
        self.root.bind("<F4>", self._on_f4)

        # Start background polling, GUI queue drain, and animation loops
        self.poller.start()
        self.root.after(100, self._drain_queue_loop)
        self.root.after(60, self._animate_loop)

    def _on_f3(self, _event=None) -> None:
        """F3 = inject a 40 s power loss at C-BPL-02."""
        if not self.state.api_ok:
            return
        self.floor_view.fault_panel.set_inputs("C-BPL-02", "Power loss", 40)
        self.floor_view.fault_panel.update_state(self.state)
        self.floor_view.fault_panel.on_inject_clicked()

    def _on_f4(self, _event=None) -> None:
        """F4 = inject a 30 s network drop at C-BPL-04."""
        if not self.state.api_ok:
            return
        self.floor_view.fault_panel.set_inputs("C-BPL-04", "Network drop", 30)
        self.floor_view.fault_panel.update_state(self.state)
        self.floor_view.fault_panel.on_inject_clicked()

    def inject_fault(self, centre_id: str, fault_type: str, duration_s: int) -> None:
        """Execute POST /v1/control/faults asynchronously without blocking Tk."""
        if not self.state.api_ok:
            self.floor_view.show_toast("API is DOWN - cannot inject fault", is_error=True)
            return

        has_active = any(
            inc.get("centre_id") == centre_id and inc.get("status") in ("open", "recovering")
            for inc in self.state.incidents
        )
        if has_active:
            self.floor_view.show_toast(f"{centre_id} already has an active incident", is_error=True)
            return

        # Start stopwatch
        self.floor_view.fault_panel.start_stopwatch(centre_id)

        # Background thread call
        def _bg_post():
            try:
                payload = {
                    "centre_id": centre_id,
                    "fault_type": fault_type,
                    "duration_s": duration_s,
                }
                headers = {"X-Control-Key": "ctrl-secret-key-2026"}
                resp = self.api_client.post("/v1/control/faults", json=payload, headers=headers)
                cmd_id = resp.get("id") if isinstance(resp, dict) else ""
                msg = f"Fault command #{cmd_id} queued: {fault_type} at {centre_id} ({duration_s}s)"
                self.queue.put(("fault_inject_result", {"ok": True, "msg": msg}, time.time()))
            except ApiException as ae:
                msg = f"HTTP {ae.status_code}: {ae.detail}"
                self.queue.put(("fault_inject_result", {"ok": False, "msg": msg}, time.time()))
            except Exception as ex:
                msg = str(ex)
                self.queue.put(("fault_inject_result", {"ok": False, "msg": msg}, time.time()))

        threading.Thread(target=_bg_post, daemon=True, name="FaultInjectThread").start()

    def _build_layout(self) -> None:
        """Construct top bar, 70/30 main area, and bottom ticker."""
        # Top Bar
        self.top_bar = TopBar(self.root)
        self.top_bar.pack(side=tk.TOP, fill=tk.X)

        sep_top = tk.Frame(self.root, bg=COLOR_BORDER, height=1)
        sep_top.pack(side=tk.TOP, fill=tk.X)

        # Bottom Ticker
        self.ticker = TickerStrip(self.root)
        self.ticker.pack(side=tk.BOTTOM, fill=tk.X)

        sep_bot = tk.Frame(self.root, bg=COLOR_BORDER, height=1)
        sep_bot.pack(side=tk.BOTTOM, fill=tk.X)

        # Main Center Container (70% Floor / 30% Notebook)
        self.main_container = tk.Frame(self.root, bg=COLOR_BG, padx=8, pady=8)
        self.main_container.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.main_container.grid_columnconfigure(0, weight=7)
        self.main_container.grid_columnconfigure(1, weight=3)
        self.main_container.grid_rowconfigure(0, weight=1)

        # Left Floor Canvas (70%)
        self.floor_view = FloorCanvas(self.main_container, on_inject=self.inject_fault,
                                       on_double_click=self._on_workstation_double_click)
        self.floor_view.grid(row=0, column=0, sticky="nsew", padx=(0, 4))

        # Right Notebook (30%)
        self.notebook_panel = NotebookPanel(self.main_container, app=self)
        self.notebook_panel.grid(row=0, column=1, sticky="nsew", padx=(4, 0))

    def _on_workstation_double_click(self, candidate_id: str) -> None:
        """Handle double-click on a workstation: select candidate and switch to Candidate tab."""
        self.state.selected_candidate = candidate_id
        self.notebook_panel.select_candidate_tab()

    def trigger_impact_refresh(self, incident_id: str) -> None:
        """Trigger an immediate asynchronous poll of incident impact."""
        def _bg():
            try:
                data = self.api_client.get(f"/v1/incidents/{incident_id}/impact")
                self.queue.put(("incident_impact", {"incident_id": incident_id, "data": data}, time.time()))
            except Exception as e:
                log_ascii(f"Error fetching impact for {incident_id}", e)
        threading.Thread(target=_bg, daemon=True, name="ImpactRefreshThread").start()

    def _drain_queue_loop(self) -> None:
        """Drain background poller results on the main Tkinter thread."""
        try:
            while True:
                try:
                    name, data_or_err, ts = self.queue.get_nowait()
                except queue.Empty:
                    break
                if name == "fault_inject_result":
                    if isinstance(data_or_err, dict):
                        ok = data_or_err.get("ok", False)
                        msg = data_or_err.get("msg", "")
                        self.floor_view.show_toast(msg, is_error=not ok)
                        if not ok:
                            self.floor_view.fault_panel.reset_stopwatch_on_error()
                        log_ascii(f"Fault inject result: ok={ok}, msg={msg}")
                elif name == "decision_result":
                    if isinstance(data_or_err, dict):
                        ok = data_or_err.get("ok", False)
                        msg = data_or_err.get("msg", "")
                        inc_id = data_or_err.get("inc_id")
                        self.floor_view.show_toast(msg, is_error=not ok)
                        if inc_id:
                            self.trigger_impact_refresh(inc_id)
                elif name == "override_result":
                    if isinstance(data_or_err, dict):
                        ok = data_or_err.get("ok", False)
                        modal = data_or_err.get("modal")
                        if ok:
                            cand_id = data_or_err.get("cand_id", "")
                            remedy = data_or_err.get("remedy", "")
                            inc_id = data_or_err.get("inc_id", "")
                            self.floor_view.show_toast(f"Overridden remedy for {cand_id}: {remedy}")
                            if inc_id:
                                self.trigger_impact_refresh(inc_id)
                            if modal:
                                try:
                                    modal.destroy()
                                except Exception:
                                    pass
                        else:
                            err_msg = data_or_err.get("err", "")
                            if modal:
                                try:
                                    modal.error_lbl.config(text=err_msg)
                                except Exception:
                                    pass
                            self.floor_view.show_toast(err_msg, is_error=True)
                elif name == "candidate_status":
                    if isinstance(data_or_err, dict):
                        self.notebook_panel.candidate_tab._handle_poll_result(data_or_err)
                else:
                    self.state.handle_poll_result(name, data_or_err, ts)

            if self.state.last_successful_poll_ts > 0 and (time.time() - self.state.last_successful_poll_ts > 4.0):
                self.state.api_ok = False
                self.state.api_status = "DOWN"

            self.top_bar.update_view(self.state)
            self.floor_view.update_view(self.state)
            self.notebook_panel.update_view(self.state)
            self.ticker.update_view(self.state)

        except Exception as e:
            log_ascii("Error in GUI queue drain loop", e)
        finally:
            if not self.is_closing:
                self.root.after(100, self._drain_queue_loop)

    def _animate_loop(self) -> None:
        """Step dynamic canvas particles and LED animations (~16 fps / 60 ms)."""
        try:
            self.floor_view.scene.animate()
            if self.floor_view.fault_panel.sw_active:
                self.floor_view.fault_panel.update_stopwatch(self.state)
        except Exception as e:
            log_ascii("Error in GUI animation loop", e)
        finally:
            if not self.is_closing:
                self.root.after(60, self._animate_loop)

    def _on_tk_exception(self, _exc: Any, val: Any, _tb: Any) -> None:
        """Global handler preventing any traceback from reaching the UI."""
        err_msg = f"Unhandled Tkinter callback exception: {val}"
        log_ascii(err_msg)

    def on_close(self) -> None:
        """Stop background worker cleanly and destroy root window."""
        self.is_closing = True
        try:
            self.poller.stop()
        except Exception as e:
            log_ascii("Error stopping poller thread during shutdown", e)
        try:
            self.root.destroy()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------
def main() -> None:
    """Launch the ERCT Control Tower application."""
    root = tk.Tk()
    _app = ControlTowerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
