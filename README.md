# Exam Resilience Control Tower (ERCT)

**A resilience layer above any online exam platform: predict, detect, decide fairly, prove it.**

MPOnline Idea and Innovation Hackathon 2026 | Technical Track | Problem Statement 6: Resilient and Trustworthy Online Assessment Ecosystem

**Team XORO**: Dev Bandil (lead), Mayank Jain, Divyansh Chaurey, Saurabh Goyal

> Simulated exam environment. ERCT is decision support, not a replacement exam platform, and not proctoring.

---

## The problem

Large online exams keep failing in the same few ways: centre power cuts, unsafe software rollouts, overload, slow manual triage (complaints instead of logs) and records nobody can verify. Recent public reports include a power failure at Jaipur centres during NEET-PG 2026 (2,445 candidates, re-exam ordered), an AIAPGET 2026 power failure (49 of 192 candidates at one centre) and faulty server updates in JAMB UTME 2025 (379,997 candidates). Figures are as reported by the sources listed in our Problem Statement document.

Exam platforms protect one candidate's session (autosave, offline buffers). Above the platform, an exam body still tends to lack fleet-level monitoring, evidence-based identification of who was affected, an explainable remedy, and a verifiable audit trail. This is our own analysis; we do not claim nothing similar exists.

## What ERCT does

Prevention, Detection, Response, Recovery, Trust:

| Stage | What ERCT does | In this MVP |
|---|---|---|
| Prevention | Readiness score and software-version gate per centre, computed from `config.yaml` | Implemented (config-based, no live probing) |
| Detection | Heartbeat and centre-event monitoring; classifies power, network and software incidents within seconds | Implemented |
| Response | Finds affected candidates from logs, recommends a remedy with rule and evidence, a human approves or overrides | Implemented |
| Recovery | Server-side session state, store-and-forward replay, restart-safe incidents | Implemented |
| Trust | Hash-chained audit log; `verify` names the first altered entry | Implemented |

## How it works

```
 Candidate clients      Centre agents        Exam-platform adapter
 (heartbeats, saves)    (power, version)     (simulator in the MVP)
          \                   |                    /
           +------------------+-------------------+
                              v
              Ingestion API (FastAPI): schema check, per-centre API key,
              idempotent event_id, one transaction per batch
                              |
        +---------------------+----------------------+
        v                     v                      v
 Detection engine      Session ledger          Hash-chained audit log
 (heartbeat gap,       and event store         (every event, incident,
  power / network)     (SQLite, WAL)            impact and decision)
        v
 Incident manager (one active incident per centre, upgrade on late events)
        v
 Impact and remedy engine (affected list from logs, rules R3, R4, R1, R2)
        v
 Review queue + decisions (human approves or overrides) + fairness check
        v
 Candidate status and notices        Control-tower GUI (gui.py)
```

Key design choices:

- **Evidence, not memory.** Impact is computed per candidate from heartbeats and saves. Silence is measured with server receive time and impact with event time, so a network backlog (no loss) and a power cut (real loss) that look alike at first get different remedies.
- **Rules, not a black box.** Every recommendation stores the rule id, the evidence events and a plain-language reason. Weak evidence is never auto-decided.
- **Human in the loop.** ERCT recommends; a controller approves or overrides, and who and why are written to the audit chain.
- **Tamper-evident record.** SHA-256 hash chain over every entry. Verification returns the first altered sequence number. It is tamper-evident, not tamper-proof: an attacker with full database write access could rebuild the chain, which is why the head hash is meant to be checked outside the database.
- **Resilient by construction.** At-least-once delivery with idempotent event ids, a disk-backed buffer with replay, a dead-letter path for invalid events, and incidents that survive an API restart.
- **Privacy by design.** Pseudonymous candidate ids only; no names or contact data in events or audit payloads.

### Remedy rules (first match wins, in the order R3, R4, R1, R2)

| Rule | Condition | Recommendation |
|---|---|---|
| R3 | Evidence is not strong | `manual_review` (never auto-decided) |
| R4 | At least half of the centre affected and an integrity flag is set | `retest_recommended` (centre-level suggestion to the controller only) |
| R1 | Lost time at most 300 s and at most 2 unsaved answers | `resume`, remaining time restored |
| R2 | Otherwise | `extra_time` = lost time + 120 s buffer |

After remedies are computed, average extra time is compared across centres and large gaps are flagged to the controller before approval (fairness check).

## Repository layout

```
app/         FastAPI backend: api/ (routes), core/ (detection, impact, audit chain, fairness), models/, config.py, db.py, main.py
simulator/   Multi-agent exam simulator (5 centres x 40 candidates), fault injection, store-and-forward buffer
gui.py       Desktop control tower (tkinter + httpx; talks to the API over HTTP only)
demo/        Command-line live fault demo
tests/       Automated tests (pytest)
docs/        Design notes and decision log
config.yaml  Centres, thresholds, readiness weights, remedy settings, demo keys
run_demo.bat / reset_demo_data.bat   Windows launcher and fresh-data reset
```

## Quick start (Windows, Python 3.12)

```
pip install -r requirements.txt
run_demo.bat
```

`run_demo.bat` starts the API and the simulator in their own windows, waits for the API to answer, counts down the 20 s detection start-up grace, then opens the GUI. If port 8000 is already in use it tells you which process holds it and stops.

Manual start, in three terminals:

```
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
python -m simulator.agent_runner
python gui.py
```

For a clean take, close all demo windows and run `reset_demo_data.bat` first. It deletes only `data\erct.db*` and `data\simulator_buffer.db*`. Do not leave the demo running for a long time: the audit chain grows with every event and `/v1/health` re-verifies it.

Command-line fault demos (each run uses an isolated temporary database and prints the incident, evidence and audit verification):

```
python demo\live_fault.py --type power_loss --centre C-BPL-02 --duration 60
python demo\live_fault.py --type network_drop --centre C-BPL-04 --duration 30
python demo\live_fault.py --type power_loss --centre C-BPL-02 --duration 60 --kill-api-at 25
python -m app.core.audit_chain --verify
```

## The control-tower GUI

`gui.py` is a desktop app that shows the five labs and the server room, with power and network links. Every number and colour comes from an API response; nothing is faked.

- **Floor view:** 5 labs x 40 PCs coloured by real session state; padlock on a centre blocked by the version gate.
- **Fault injection:** cut power or drop the network at one lab; a stopwatch shows seconds from pressing Inject to the incident opening (this includes the simulator picking up the command; the last-heartbeat-to-incident time is measured separately).
- **Incidents, Impact, Candidate and Audit tabs:** incident timeline; per-candidate lost time, unsaved answers, rule and rationale; approve or override with a reason; the candidate-facing status; the hash chain with Verify, a demo-only Tamper and Restore.
- **Guided mode:** one NEXT STEP button (Space) walks through seven steps: centres live, a blocked lab, power cut, incident detected, who was affected and what is fair, what the candidate sees, prove nothing was changed.
- **Keys:** `F3` power loss at C-BPL-02 (40 s), `F4` network drop at C-BPL-04 (30 s), `F11` fullscreen, `python gui.py --scale 1.25` for high-DPI screens.

The control and controller keys in `config.yaml` are demo keys for the local simulator.

## API surface

| Method and path | Purpose |
|---|---|
| `POST /v1/events` | Ingest one event or a batch (idempotent by `event_id`, per-centre API key) |
| `GET /v1/health` | Service status, event count, audit chain status |
| `GET /v1/centres` | Centre status and live session counts |
| `GET /v1/centres/{id}/sessions` | Per-candidate session state for a centre |
| `GET /v1/readiness` | Readiness score and version gate per centre |
| `GET /v1/incidents`, `GET /v1/incidents/{id}` | Incidents with evidence and timeline |
| `GET /v1/incidents/{id}/impact` | Affected candidates, lost time, remedy, rule, rationale, decision |
| `GET /v1/review-queue` | Cases waiting for human review |
| `POST /v1/decisions`, `POST /v1/incidents/{id}/decisions/approve-all` | Approve or override (controller key; who and why recorded) |
| `GET /v1/status/{candidate_id}`, `GET /v1/notices` | Candidate-facing status and notices |
| `GET /v1/fairness` | Cross-centre remedy fairness check |
| `GET /v1/audit/verify`, `GET /v1/audit/trail` | Verify the hash chain; recent entries |
| `POST /v1/audit/tamper`, `POST /v1/audit/restore` | Demo only (control key) |
| `POST /v1/control/faults` | Queue a fault for the simulator (control key) |

## Measured results (simulated environment, one laptop)

| Metric | Result |
|---|---|
| Event ingestion (5,000 events, batches of 500) | 3,965 to 4,667 events/s |
| Replay of a 6,000-event backlog after an outage | 1.39 s, audit chain valid |
| Power loss: last good heartbeat to incident opened | 2.1 s and 3.1 s in two live runs |
| Network drop | Opened about 10 s after the link dropped (low confidence); upgraded 1.1 s after the link returned; 921 late events recovered across 40 sessions |
| API process killed and restarted during an incident | Same incident id before and after |
| Real kill/restart replay test | 160 events sent, 160 unique rows, 0 duplicates, audit verify OK |
| Audit chain in a 90 s live run | 11,325 entries, verified |
| Quiet run, 60 s, no fault | 0 incidents in 63 detection ticks |

These are simulator results, not claims about a production deployment.

## Tests

```
python -m pytest -q
```

The suite takes about five and a half minutes because several tests start real processes. Run it with no demo processes running (API, simulator or GUI), otherwise tests can collide on the port and the database files.

## Scope, limits and roadmap

Not in this MVP:

- Real vendor or exam-platform integration (a simulator adapter stands in).
- Role-based login. The candidate status endpoint has no authentication in the MVP and uses sequential pseudonymous ids.
- Early-warning risk score, latency and error-rate anomaly detection, HTML audit report export, independent watchdog, web dashboard.
- Production storage and transport: PostgreSQL, a stream broker (NATS JetStream), an external audit anchor (immudb). Scaling beyond one machine is a design path, not a measurement.

Roadmap: real adapters (Moodle or TAO plug-ins, a vendor API), the items above, a pilot with a state-level examination body, repeat-failure analytics by vendor and centre, multi-region deployment and regulator export formats.

## Third-party software and AI assistance

Open-source libraries and their licences are listed in [ATTRIBUTIONS.md](ATTRIBUTIONS.md). AI assistants used: Antigravity IDE with a Gemini model for code generation, and Claude (Anthropic) for planning, prompt drafting, document drafting and review. All code was reviewed and tested by the team. The repository was created for this hackathon and no earlier project code was reused.

## Links

- Repository: https://github.com/kcfl/erct
- Demo video and documents: https://drive.google.com/drive/folders/16fkHBRo68dgbkpLJ9nCEFnkXA8WBh0Yr
