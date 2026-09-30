"""ERCT (Exam Resilience Control Tower) - Desktop Demo GUI.

Visual control tower for operational resilience and audit control plane.
Runs on tkinter (ttk + Canvas) and httpx only, communicating with ERCT API over HTTP.
Never opens SQLite directly and never imports from app/ or simulator/.
"""
from __future__ import annotations

import os
import queue
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
                self.events_rate = max(0.0, (curr_events - self.events_count) / dt)
            self.previous_events_count = self.events_count
            self.events_count = curr_events
            self.last_events_ts = ts
            self.audit_ok = bool(data.get("audit_chain_ok", True))
            self.audit_total_entries = data.get("total_audit_entries", 0)

        elif name == "centres":
            if isinstance(data, list):
                self.centres = data

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
FONT_UI_HEADING = ("Segoe UI", 12, "bold")
FONT_MONO = ("Consolas", 10)
FONT_MONO_SMALL = ("Consolas", 9)


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

            # Build polygon coordinates
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
        # API Status Pill
        if state.api_ok:
            if state.api_status == "HEALTHY":
                self.api_pill.config(text="API: HEALTHY", fg=COLOR_GREEN, bg="#0d2b20")
            elif state.api_status == "DEGRADED":
                self.api_pill.config(text="API: DEGRADED", fg=COLOR_AMBER, bg="#2a1f0a")
            else:
                self.api_pill.config(text=f"API: {state.api_status}", fg=COLOR_GREEN, bg="#0d2b20")
        else:
            self.api_pill.config(text="API: DOWN", fg=COLOR_RED, bg="#361010")

        # Events Pill
        self.events_pill.config(text=f"events: {state.events_count}")

        # Audit Pill
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
# FLOOR CANVAS (LEFT 70%)
# ---------------------------------------------------------------------------
class FloorCanvas(tk.Frame):
    """Floor view displaying labs, machines, and network lines in upcoming step."""

    def __init__(self, parent: tk.Widget):
        super().__init__(parent, bg=COLOR_PANEL, highlightthickness=1, highlightbackground=COLOR_BORDER)
        self.canvas = tk.Canvas(self, bg=COLOR_PANEL, highlightthickness=0)
        self.canvas.pack(fill=tk.BOTH, expand=True)
        self.canvas.bind("<Configure>", self._on_resize)

    def _on_resize(self, _event: Optional[tk.Event] = None) -> None:
        self.draw_placeholder()

    def draw_placeholder(self) -> None:
        w = self.canvas.winfo_width()
        h = self.canvas.winfo_height()
        if w <= 1 or h <= 1:
            return

        self.canvas.delete("all")
        # Draw faint grid
        grid_size = 40
        for x in range(0, w, grid_size):
            self.canvas.create_line(x, 0, x, h, fill="#142240", width=1)
        for y in range(0, h, grid_size):
            self.canvas.create_line(0, y, w, y, fill="#142240", width=1)

        # Centered placeholder message
        self.canvas.create_text(
            w // 2,
            h // 2,
            text="Floor view coming in the next step",
            fill=COLOR_MUTED,
            font=("Segoe UI", 13),
        )

    def update_view(self, _state: AppState) -> None:
        """Floor canvas data updates will hook in here in the next step."""
        pass


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

        # Start background polling and GUI queue drain loop
        self.poller.start()
        self.root.after(100, self._drain_queue_loop)

    def _build_layout(self) -> None:
        """Construct top bar, 70/30 main area, and bottom ticker."""
        # Top Bar
        self.top_bar = TopBar(self.root)
        self.top_bar.pack(side=tk.TOP, fill=tk.X)

        # Border separator below top bar
        sep_top = tk.Frame(self.root, bg=COLOR_BORDER, height=1)
        sep_top.pack(side=tk.TOP, fill=tk.X)

        # Bottom Ticker
        self.ticker = TickerStrip(self.root)
        self.ticker.pack(side=tk.BOTTOM, fill=tk.X)

        # Border separator above ticker
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
            # Drain all pending items without blocking
            while True:
                try:
                    name, data_or_err, ts = self.queue.get_nowait()
                except queue.Empty:
                    break
                self.state.handle_poll_result(name, data_or_err, ts)

            # Mark DOWN if no successful poll has arrived in 3 seconds
            if time.time() - self.state.last_successful_poll_ts > 3.0:
                self.state.api_ok = False
                self.state.api_status = "DOWN"

            # Update views
            self.top_bar.update_view(self.state)
            self.floor_view.update_view(self.state)
            self.notebook_panel.update_view(self.state)
            self.ticker.update_view(self.state)

        except Exception as e:
            log_ascii("Error in GUI queue drain loop", e)
        finally:
            if not self.is_closing:
                self.root.after(100, self._drain_queue_loop)

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
