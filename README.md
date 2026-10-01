# Exam Resilience Control Tower (ERCT)

**MPOnline Idea & Innovation Hackathon 2026** — *Problem Statement 6: Exam Operations Resilience & Integrity*

ERCT is an end-to-end operational resilience, automated incident classification, and audit control plane for high-stakes online examinations. It provides real-time multi-centre telemetry ingestion, edge failure detection, deterministic rule-based candidate remedy evaluation (Rules R1–R4), and a tamper-evident hash chain (not tamper-proof) audit trail.

---

## Architecture Overview

1. **FastAPI Operational Backend (`app/`)**: High-throughput REST API backed by SQLite in Write-Ahead Logging (WAL) mode with monotonic event sequencing, idempotency deduplication, store-and-forward buffers, and tamper-evident hash chain (not tamper-proof) audit verification.
2. **Multi-Agent Edge Simulator (`simulator/`)**: Autonomous emulator generating realistic candidate heartbeats, answer saves, biometric verifications, and controlled power/network disruptions across 5 regional exam centres (`C-BPL-01` to `C-BPL-05`).
3. **Desktop Control Tower GUI (`gui.py`)**: Real-time visual control tower built exclusively with Python's standard `tkinter` (`ttk` + `Canvas`) and `httpx`, providing floor views, interactive fault panels, impact decisions, candidate status views, and tamper-evident hash chain (not tamper-proof) exploration.

---

## Demo GUI

The ERCT Control Tower provides a visual operations plane designed for examination controllers, invigilators, and technical evaluators.

### What It Shows

- **Interactive Exam Floor Canvas**: Live visual floor plan displaying all 5 exam centres, candidate seat pods, live heartbeat pulses, connection states, and readiness padlock indicators.
- **Fault Injection & Precision Stopwatch**: Manual injection panel for power outages and network drops with stopwatch (seconds, one decimal) from pressing Inject to the incident opening. Detection time shown in the GUI includes the simulator picking up the fault command; the 2.1 s figure in the documents is last good heartbeat to incident opened.
- **Chevron Lifecycle Tracker**: Automatic state-driven progress across *Prevention*, *Detection*, *Recovery*, *Impact Analysis*, *Decision*, and *Resolution*.
- **Incidents & Classification Tab**: Real-time incident feed showing fault classification, rule triggers (e.g., `R_POWER_LOSS`), and confidence scoring.
- **Impact & Fairness Tab**: Candidate-level impact fact matrix detailing lost time, disconnect counts, recommended remedies (Rules R1–R4), and human override workflows with fairness acknowledgement gates.
- **Candidate Perspective Tab**: Real-time candidate view reflecting transparent incident notifications, session state, and allotted compensatory time.
- **Audit Strip**: Visual representation of the tamper-evident hash chain (not tamper-proof) with live verification sweeps, pinpoint block cracking on simulated database tampering, and instant restoration.

---

### How to Run

#### Option A: One-Click Launcher (Recommended on Windows)
Run the root batch launcher:
```cmd
run_demo.bat
```
`run_demo.bat` performs the complete startup sequence automatically:
1. Starts the API server in its own console window (`python -m uvicorn app.main:app --host 127.0.0.1 --port 8000`).
2. Polls `/v1/health` using an inline Python one-liner until the API answers healthy (up to 40 s).
3. Starts the Multi-Agent Simulator in its own console window (`python -m simulator.agent_runner`).
4. Displays a 20-second countdown for the edge detection start-up grace period.
5. Launches `python gui.py`.
6. Upon closing the GUI, displays a clean reminder to close the background API and simulator console windows.

#### Option B: Manual Three-Terminal Startup
In three separate terminal windows:
```cmd
# Terminal 1: Start API server
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000

# Terminal 2: Start multi-agent simulator
python -m simulator.agent_runner

# Terminal 3: Start desktop GUI
python gui.py
```

#### Resetting Demo Data
To start a clean demonstration or recording take from an empty database:
```cmd
reset_demo_data.bat
```
After confirmation, this script deletes only `data\erct.db*` and `data\simulator_buffer.db*`. It never deletes run outputs or log files.

---

### Guided vs Free Mode and Keybindings

The top bar features a **"Guided / Free"** mode toggle button (default: `Free`).

- **Guided Mode**:
  - Activates a floating, bottom-centre glassmorphic control bar over the floor view.
  - Displays a **"Step N of 7"** progress indicator, status badges, and 20 pt Segoe UI plain-language captions describing what will happen next.
  - Controls include a large teal **NEXT STEP** button, a hidden **Retry** button (shown only on network failure or wait timeouts > 120 s), and a **Reset demo** button.
  - Greys out the manual fault injection panel with the notice *"Guided mode: manual fault injection disabled"* to prevent accidental test interference.
  - Walks through all 7 operational resilience milestones:
    1. *All five centres are live*: Verifies 5 connected centres and grace expiry.
    2. *One lab is blocked before the exam*: Highlights `C-BPL-03` software mismatch lock (`4.2.0` vs required `4.2.1`).
    3. *Cut power at C-BPL-02*: Injects a 30 s outage and measures time to detection.
    4. *Incident detected*: Tracks incident progression from `open` to `recovering` to `resolved`.
    5. *Who was affected, and what is fair*: Reviews impact matrix, approves recommended remedies, and opens pre-filled override modal for manual review candidate (+120 s buffer).
    6. *What the candidate sees*: Displays the candidate perspective and notice banner.
    7. *Prove nothing was changed*: Executes green audit verification, tampers 4th visible block in SQLite, observes `audit: BROKEN`, verifies detection at the exact altered sequence, restores row, and re-verifies green.
- **Free Mode**: Restores full manual control over fault injection, comboboxes, and workstation double-click inspection.

#### Keyboard Shortcuts & CLI Flags

| Key / Flag | Scope | Function |
| :--- | :--- | :--- |
| <kbd>Space</kbd> | Guided Mode | Triggers **NEXT STEP** when ready. |
| <kbd>F3</kbd> | Free Mode | Injects a 40 s power loss fault at `C-BPL-02`. |
| <kbd>F4</kbd> | Free Mode | Injects a 30 s network drop fault at `C-BPL-04`. |
| <kbd>F11</kbd> | Global | Toggles Fullscreen window mode (<kbd>Escape</kbd> exits fullscreen). |
| `--scale <factor>` | CLI Flag | Scales typography, button dimensions, and canvas coordinates for high-DPI displays (e.g., `python gui.py --scale 1.25`). |

---

### Note on Demo Keys

Control keys (`X-Control-Key: ctrl-secret-key-2026`) and controller authorization keys (`X-Controller-Key: demo-controller-key`) are embedded specifically for the local simulator, demo scripts, and evaluation harness. In production deployments, these are provisioned via external secret management and hardware tokens.

---

### System Limits & Design Assumptions (MVP)

1. **Simulated Environment**: Telemetry, heartbeat streams, and hardware disruptions originate from the multi-agent simulator process.
2. **Decision Support**: Automated remedy rules (R1–R4) provide transparent decision proposals; human controller override remains available and mandatory for weak-evidence cases.
3. **Config-Based Readiness**: Pre-exam environment prerequisites (software versions, battery benchmarks) are evaluated declaratively against `config.yaml`.
4. **Candidate Status Endpoint**: In this MVP, `GET /v1/status/{candidate_id}` operates as a transparent status query without candidate authentication tokens.

---

## Automated Test Suite

Run the full test suite with 85 passing tests, run with pytest -q while no demo processes are running:
```cmd
python -m pytest -q
```
**Verification status:** 85 passing tests covering tamper-evident hash chain (not tamper-proof) audit integrity, multi-agent fault injection, classification edge cases, store-and-forward failover, and kill-restart recovery.
