"""ERCT (Exam Resilience Control Tower) - Desktop Demo GUI.

Visual control tower for operational resilience and audit control plane.
Runs on tkinter (ttk + Canvas) and httpx only, communicating with ERCT API over HTTP.
Never opens SQLite directly and never imports from app/ or simulator/.
"""
from __future__ import annotations

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
    """Wraps httpx.Client with base URL from env ERCT_API, timeout 2.0s."""

    def __init__(self, base_url: Optional[str] = None, timeout: float = 2.0):
        self.base_url = (base_url or os.environ.get("ERCT_API", "http://127.0.0.1:8000")).rstrip("/")
        self.client = httpx.Client(base_url=self.base_url, timeout=timeout)

    def _handle_response(self, response: httpx.Response) -> Any:
        if response.status_code >= 400:
            detail = response.text
            try:
                err_json = response.json()
                if isinstance(err_json, dict) and "detail" in err_json:
                    detail = str(err_json["detail"])
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

    def handle_poll_result(self, name: str, data_or_error: Any, ts: float) -> None:
        """Process incoming poll data or record an error."""
        if isinstance(data_or_error, Exception):
            if name == "health":
                self.api_ok = False
                self.api_status = "DOWN"
                self.last_error = str(data_or_error)
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

    def __init__(self, api_client: ApiClient, out_queue: queue.Queue):
        super().__init__(daemon=True, name="ERCT-PollerThread")
        self.client = api_client
        self.queue = out_queue
        self.stop_event = threading.Event()

        self.centre_ids = ["C-BPL-01", "C-BPL-02", "C-BPL-03", "C-BPL-04", "C-BPL-05"]
        self.sessions_rot_idx = 0
        self.last_sessions_poll = 0.0

        # Endpoint configurations: (name, path, interval_seconds)
        self.endpoints: List[Tuple[str, str, float]] = [
            ("health", "/v1/health", 1.5),
            ("centres", "/v1/centres", 1.0),
            ("incidents", "/v1/incidents", 1.5),
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
                        self.queue.put((name, data, now))
                    except Exception as err:
                        self.queue.put((name, err, now))

            # 2. Centre sessions rotation: 1 centre every 0.5s (full cycle ~2.5s)
            if not self.stop_event.is_set() and (now - self.last_sessions_poll >= 0.5):
                self.last_sessions_poll = now
                cid = self.centre_ids[self.sessions_rot_idx]
                self.sessions_rot_idx = (self.sessions_rot_idx + 1) % len(self.centre_ids)
                try:
                    data = self.client.get(f"/v1/centres/{cid}/sessions")
                    self.queue.put(("centre_sessions", {"centre_id": cid, "sessions": data}, now))
                except Exception as err:
                    self.queue.put(("centre_sessions", {"centre_id": cid, "error": err}, now))

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

    STAGES = ["Prevention", "Detection", "Response", "Recovery", "Trust"]

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
                self.network_packets[cid].append({"id": p_dot, "dist": -1.0, "active": False})

            # Workstations Grid (8 cols x 5 rows = 40 PCs)
            ws_top = eq_y + 24
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
                "title": t_id,
                "subtitle": sub_id,
                "inc_rect": inc_badge_rect,
                "inc_text": inc_badge_text,
                "ups_bar": ups_bar,
                "p_drop": p_drop,
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

        if not state.api_ok:
            self.canvas.itemconfig(self.server_items["offline_label"], state="normal")
            for led in self.server_items["leds"]:
                self.canvas.itemconfig(led, fill=COLOR_RED)
        else:
            self.canvas.itemconfig(self.server_items["offline_label"], state="hidden")

        # 2. Centre Status & Readiness
        readiness_map = {}
        if state.readiness and isinstance(state.readiness.get("centres"), list):
            readiness_map = {c["centre_id"]: c for c in state.readiness["centres"]}

        centres_data_map = {c["centre_id"]: c for c in state.centres}

        for cid, items in self.lab_items.items():
            c_info = centres_data_map.get(cid, {})
            c_status = c_info.get("status", "healthy")
            s_total = c_info.get("sessions_total", 40)
            s_silent = c_info.get("sessions_silent", 0)
            s_reporting = max(0, s_total - s_silent)
            version = c_info.get("software_version", "4.2.1")
            inc_id = c_info.get("open_incident_id")

            # Lab border & fill styling
            if not state.api_ok or c_status == "down":
                self.canvas.itemconfig(items["room"], outline=COLOR_RED, fill="#20111a")
                self.canvas.itemconfig(items["ups_bar"], fill=COLOR_RED)
            elif c_status == "degraded":
                self.canvas.itemconfig(items["room"], outline=COLOR_AMBER, fill="#16223b")
                self.canvas.itemconfig(items["ups_bar"], fill=COLOR_AMBER)
            else:
                self.canvas.itemconfig(items["room"], outline=COLOR_GREEN, fill=COLOR_PANEL)
                self.canvas.itemconfig(items["ups_bar"], fill=COLOR_GREEN)

            # Incident Badge
            if inc_id:
                self.canvas.itemconfig(items["inc_rect"], state="normal")
                self.canvas.itemconfig(items["inc_text"], text=inc_id, state="normal")
            else:
                self.canvas.itemconfig(items["inc_rect"], state="hidden")
                self.canvas.itemconfig(items["inc_text"], state="hidden")

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

        # 3. Workstations per-PC State
        for (cid, pc_idx), pc in self.pcs.items():
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

            # 3. Network Link Packets
            for cid, link in self.network_links.items():
                c_data = next((c for c in self.state.centres if c["centre_id"] == cid), {})
                c_status = c_data.get("status", "healthy")
                s_frac = c_data.get("silent_fraction", 0.0)

                # Spawn packets if centre is online
                if c_status != "down" and s_frac < 0.6:
                    spawn_rate = min(0.35, max(0.04, ev_rate * 0.015))
                    active_cnt = sum(1 for p in self.network_packets[cid] if p["active"])
                    if active_cnt < 6 and random.random() < spawn_rate:
                        for p in self.network_packets[cid]:
                            if not p["active"]:
                                p["active"] = True
                                p["dist"] = 0.0
                                p["speed"] = random.uniform(0.025, 0.045)
                                break

                # Advance active packets toward hub
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
# FLOOR CANVAS (LEFT 70% CONTAINER)
# ---------------------------------------------------------------------------
class FloorCanvas(tk.Frame):
    """Floor view displaying labs, machines, and live operational topology."""

    def __init__(self, parent: tk.Widget):
        super().__init__(parent, bg=COLOR_PANEL, highlightthickness=1, highlightbackground=COLOR_BORDER)
        self.canvas = tk.Canvas(self, bg=COLOR_PANEL, highlightthickness=0)
        self.canvas.pack(fill=tk.BOTH, expand=True)

        self.scene = FacilityScene(self.canvas)
        self._resize_after_id: Optional[str] = None

        self.canvas.bind("<Configure>", self._on_configure)
        self.canvas.bind("<Motion>", self.scene.on_mouse_move)
        self.canvas.bind("<Button-1>", self.scene.on_mouse_click)
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
        if self.scene.state:
            self.scene.update_data(self.scene.state)

    def update_view(self, state: AppState) -> None:
        """Bind state updates into scene."""
        if not self.scene.lab_items:
            w = max(900, self.canvas.winfo_width())
            h = max(520, self.canvas.winfo_height())
            self.scene.build_scene(w, h)
        self.scene.update_data(state)


# ---------------------------------------------------------------------------
# NOTEBOOK PANEL (RIGHT 30%)
# ---------------------------------------------------------------------------
class NotebookPanel(tk.Frame):
    """Right tabbed inspection panel: Incidents, Impact, Audit, Candidate."""

    def __init__(self, parent: tk.Widget):
        super().__init__(parent, bg=COLOR_BG)
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.tabs: Dict[str, tk.Frame] = {}
        for tab_name in ["Incidents", "Impact", "Audit", "Candidate"]:
            tab_frame = tk.Frame(self.notebook, bg=COLOR_PANEL, padx=16, pady=16)
            self.notebook.add(tab_frame, text=tab_name)
            self.tabs[tab_name] = tab_frame

            lbl = tk.Label(
                tab_frame,
                text=f"{tab_name} panel coming soon...",
                font=FONT_UI,
                fg=COLOR_MUTED,
                bg=COLOR_PANEL,
            )
            lbl.pack(expand=True)

    def update_view(self, _state: AppState) -> None:
        """Notebook view updates will hook in here in future steps."""
        pass


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

        if not all_entries:
            self.ticker_label.config(text="No incidents yet")
            return

        all_entries.sort(key=lambda e: e["ts"], reverse=True)
        formatted: List[str] = []
        for e in all_entries[:8]:
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
        self.poller = Poller(self.api_client, self.queue)
        self.is_closing = False

        # Build View Hierarchy
        self._build_layout()

        # Protocol handlers
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # Start background polling, GUI queue drain, and animation loops
        self.poller.start()
        self.root.after(100, self._drain_queue_loop)
        self.root.after(60, self._animate_loop)

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
        self.floor_view = FloorCanvas(self.main_container)
        self.floor_view.grid(row=0, column=0, sticky="nsew", padx=(0, 4))

        # Right Notebook (30%)
        self.notebook_panel = NotebookPanel(self.main_container)
        self.notebook_panel.grid(row=0, column=1, sticky="nsew", padx=(4, 0))

    def _drain_queue_loop(self) -> None:
        """Drain background poller results on the main Tkinter thread."""
        try:
            while True:
                try:
                    name, data_or_err, ts = self.queue.get_nowait()
                except queue.Empty:
                    break
                self.state.handle_poll_result(name, data_or_err, ts)

            if time.time() - self.state.last_successful_poll_ts > 3.0:
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
