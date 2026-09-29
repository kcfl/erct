# Architecture & Implementation Decisions
**Project:** Exam Resilience Control Tower (ERCT)  
**Hackathon:** MPOnline Idea & Innovation Hackathon 2026 (Technical Track, PS6)  
**Target Path:** `D:\mp online hackathon\erect`

---

## 1. Environment & Operational Baseline
- **Primary OS:** Windows 10/11 is primary. Scripts and commands must quote all paths containing spaces.
- **Process Orchestration (`run.bat`):** Must start the FastAPI backend and the simulator as **two separate processes** with independent PIDs to enable the live API kill/restart zero-duplicate replay demo.
- **Process Isolation & DB Ownership:** The audit chain and SQLite database are written **ONLY by the API process**. The simulator NEVER opens SQLite or connects to the database; it interacts exclusively over the HTTP API (`POST /v1/events`).
- **Console Output:** Clean ASCII output only. No Unicode emoji or special characters that can break Windows `cmd.exe` or PowerShell codepages.
- **Server:** `uvicorn` run without `--reload` for stability and predictable single-process state.
- **Docker:** Low-priority secondary fallback; single-command native execution via `run.bat` is primary.
- **Repository:** Fresh, standalone git repository initialized at the project root (`git init`). Zero reuse of prior project code.

---

## 2. Identifiers, Namespaces & Seed Data
- **ID Conventions (strict PRD compliance):**
  - Exams: `EX-2026-PS6-01`
  - Centres: `C-BPL-01`, `C-BPL-02`, `C-BPL-03`, `C-BPL-04`, `C-BPL-05`
  - Candidates: `CAND-000417` (strictly pseudonymous, no PII)
  - Sessions: `SES-000417-1`
- **Seeding:** Pre-seeded deterministically from `config.yaml` on setup (5 centres x 40 candidates = 200 sessions).
- **API Versioning:** All endpoints use the `/v1/...` namespace (e.g., `POST /v1/events`, `GET /v1/incidents`).

---

## 3. Readiness Gate
- **Required Version:** Defined in `config.yaml` (default `"4.2.1"`).
- **Simulated Centre State:** Centre `C-BPL-03` reports software version `"4.2.0"`.
- **Readiness Scoring:** PRD weights applied:
  - Software Version: 40 points
  - Power Backup: 30 points
  - Capacity: 30 points
  - Minimum Pass Score: 70 points
- **Gate Enforcement:** Version mismatch is an absolute **hard block** (`passed: false`, gate blocks centre from commencing exam), regardless of score. No NTP or ping checks.

---

## 4. Power Loss Signature & Detection Engine
- **Telemetry Signature & Edge Agent UPS Assumption:** Centre edge agent emits `POWER_LOSS` (`source`: grid/ups/generator, `backup_minutes`), under the operational assumption that the centre edge node possesses a localized micro-UPS providing 15-30 seconds of auxiliary power to broadcast a last-gasp outage alert. Immediately following this, all workstations and sessions at that centre go silent (machines powered down; zero telemetry generated or buffered).
- **Detection Criteria:**
  - Explicit `POWER_LOSS` event received, OR
  - $\ge 60\%$ (configurable via `detection.centre_loss_fraction`) of sessions at a centre go silent within a 10s window (`detection.centre_loss_window_s`).
- **Heartbeat Rhythm & Time Decision:**
  - Event `ts` is **ALWAYS real UTC** (`datetime.now(timezone.utc).isoformat()`).
  - Cadence intervals (2s) and fault durations (e.g. 240s) are in **real wall-clock seconds**. No artificial clock warping or timestamp compression is permitted.
  - Setting `exam_clock_speed` (default 1.0) scales strictly the rate at which `remaining_s` decrements within active sessions.
  - Tests shorten intervals purely via configuration overrides (e.g. `heartbeat_interval_s: 0.5`), never by faking timestamps.
- **Payload Extension (`local_seq`):** Candidate `HEARTBEAT` payloads support an optional `local_seq` field representing local client-side answer submissions before server-side persistence (`saved_seq`). This allows deterministic modelling of 0-4 unsaved answers at outage inception. After a power outage, unsaved answers are wiped and `local_seq` resets to `last_saved_seq`.
- **Incident Classification:**
  - Classify as `power` if a `POWER_LOSS` event exists within the detection window.
  - Classify as `network` if a `NETWORK_DOWN` event exists within the detection window.
  - Otherwise, classify as `network` with `evidence_quality = partial` (low confidence flag).

---

## 5. Integrity Flag Rules
`integrity_flag` is **NOT** set by ordinary outages. It is explicitly set when anomalous telemetry indicates potential tampering or systemic breach:
1. **Sequence Regression / Gaps:** Monotonic `seq` counter regressions or unexplained gaps not accounted for by buffered replay.
2. **Timestamp Skew:** Skew between event source timestamp `ts` and server ingest time `ingested_at` exceeding `integrity.max_timestamp_skew_s`.
3. **Session Collision:** The same candidate session active concurrently across two different centres.
4. **Login Anomaly:** `LOGIN_FAIL` event burst exceeding `integrity.login_fail_burst_threshold`.
5. **Simulator Fault Injection:** Simulator supports an optional `integrity_anomaly` fault injection to trigger `integrity_flag` and exercise Rule R4 during the live demo.

---

## 6. Remedy Engine Precedence & Rules
First match wins, strictly evaluated in order **R3 -> R4 -> R1 -> R2**:
- **Output Enum:** `resume` | `extra_time` | `retest_recommended` | `manual_review`
- **Rule Definitions:**
  1. **R3 (Guard - Weak Evidence):**  
     `if evidence_quality != 'strong' -> manual_review`  
     *Always evaluated first. Prevents unverified automatic remedies.*
  2. **R4 (Centre-level Anomaly Advisory):**  
     `if centre_affected_fraction >= 0.5 and integrity_flag -> retest_recommended`  
     *Controller-facing recommendation for centre-wide re-evaluation; never automatically applied to individual candidates.*
  3. **R1 (Minor Interruption):**  
     `if lost_s <= 300 and unsaved_answers <= 2 -> resume`  
     *Restores remaining exam time to candidate session.*
  4. **R2 (Moderate/Major Interruption or Lost Data):**  
     `if lost_s > 300 or unsaved_answers > 2 -> extra_time = lost_s + 120`  
     *Compensates exact lost time plus a configurable buffer (default +120s).*

- **Traceability & Governance:**
  - Every decision record stores `rule_id`, exact evidence rows consulted, and a plain-language explainability rationale.
  - **Fairness Check:** Compares average extra time granted across centres, flagging anomalies or disparities > threshold to the exam controller.
  - **Decisions API (`POST /v1/decisions`):** Allows controllers to approve or override recommendations with required `decided_by` and `reason` audit logging.

---

## 7. Audit Hash Chain Specification
- **Hash Formula:**
  $$\text{entry\_hash} = \text{SHA256}(\text{prev\_hash} \mid \text{seq} \mid \text{ts\_iso} \mid \text{entry\_type} \mid \text{ref\_id} \mid \text{canonical\_json}(\text{payload}))$$
- **Separator:** Pipe character (`|`).
- **Genesis Block:** `prev_hash = "0" * 64` (64 zeros).
- **Canonical JSON:** Strict UTF-8, keys sorted, zero whitespace separators (`separators=(',', ':')`, `ensure_ascii=False`).
- **Timestamp Integrity:** `ts_iso` is stored as the exact string that was hashed; it is never re-parsed or reformatted.
- **Concurrency:** Appends are serialized strictly by an in-process thread-safe writer lock (`threading.Lock`).
- **Verification (`verify()`):** Sequentially iterates rows by `seq`, recomputes each `entry_hash`, validates each `prev_hash` against the predecessor's `entry_hash`, and returns `{"ok": True}` or `{"ok": False, "failing_seq": <seq>, "error": "<reason>"}` at the **first** discrepancy.

---

## 8. Tamper Demonstration
- **Mechanism:** Both a CLI command (`python -m app.core.audit_chain --tamper <seq>`) and an Ops Dashboard button (`[DEMO ONLY] Simulate DB Tamper`).
- **Action:** Executes a raw SQL `UPDATE` directly on the database to bypass application validation and alter an audit payload.
- **Restoration:** Includes a restore command/endpoint to reset the tampered row so the verification pass/fail sequence can be repeated smoothly.

---

## 9. Extra Scope & Offline Handling
- **Reporting:** Exportable standalone HTML audit and incident report (`GET /v1/audit/report/{exam_id}`).
- **Communications:** Candidate notices feed and candidate-facing transparency query page (`GET /v1/status/{candidate_id}`).
- **Anomaly Detection:** Pure Python rolling z-score / EWMA latency detector (zero heavy ML dependencies, no River required).
- **Optional Modules:** Preflight TLS/headers, weather risk, IP reputation, and systemic-risk graph.
  - **Display Rule:** The UI must explicitly mark non-live data as `"cached/seeded"` to ensure total integrity before judges.

---

## 10. Delivery Phases & Schedule
- **Hour 0–3:** Phase 1: DB Schema, WAL configuration, thread-safe writer lock, Audit Hash Chain, and tests.
- **Hour 4–6:** Phase 2: Ingest API with idempotency + Store-and-Forward retry buffer simulator.
- **Hour 7–10:** Phase 3:
  - **3a:** Incident detection engine (<10s power outage, network classifier, stream aggregator).
  - **3b:** Remedy engine (R3->R4->R1->R2), fairness checks, review queue, decision approvals.
  - **3c:** Readiness gate (version 4.2.1 vs 4.2.0, scoring weights).
- **Hour 11–13:** Phase 4: Ops Control Tower dashboard (SSE) + Candidate status page + Report export.
- **Hour 13:** **CODE FREEZE** — End-to-end demo hardening, video recording, single-command validation.
- **Hour 14–18:** Phase 5 & 6 polish, optional modules with offline labels, slides, and submission packaging.

---

## 11. Phase 3a-i Data Layer Hardening Decisions
- **Dual-Clock Architecture:** Outage detection measures silence using server receive time (`ingested_at`), while impact duration is calculated from event source time (`ts`). When buffered backlogs arrive late with `ts` falling into an outage window, incidents update retroactively.
- **Session Monotonicity:** `sessions` table tracks `last_ingested_at` (server UTC receive time). Fields `last_heartbeat_at`, `remaining_s`, and `last_saved_seq` strictly monotonically update only when incoming event `ts >= last_heartbeat_at`. A delayed backlog from a restored network cannot regress or overwrite newer terminal state.
- **Answer Counter Idempotency:** `answers_saved` increments strictly on newly inserted answer rows (`INSERT OR IGNORE`), ensuring network retransmissions and replays never double count.
- **Server-Side Centre Liveness:** `centre_liveness(centre_id PRIMARY KEY, last_ingested_at, last_event_ts, events_total)` is maintained atomically in the same database transaction per ingested batch, providing the detector with immediate heartbeat and silence visibility across all centres.
- **Staggered Reboot Model:** Following `POWER_RESTORED`, the edge agent resumes immediately, while individual candidate workstations stagger reboot over `simulation.restore_boot_delay_s` (range [2, 20] seconds). Delays are deterministically seeded with `Random(f"{seed}:{candidate_id}:{fault_id}")`. Workstations emit zero events while booting, but the exam countdown continues uninterrupted; the first heartbeat post-restore carries the decremented `remaining_s`.
- **Simulator Ground Truth Isolation:** `data/ground_truth.jsonl` records ground truth upon fault completion (including true downtime `lost_s_true`, unsaved answers at outage inception, and resume timestamps). To maintain absolute evaluative integrity, all modules under `app/` are strictly barred from referencing or importing `ground_truth`, guaranteed by automated CI scanning (`tests/test_ground_truth.py`).

---

## 12. Phase 3a-ii Detection Engine & Incident Manager Decisions
- **Public API Authorization:** Endpoints (`GET /v1/centres`, `GET /v1/incidents`, `GET /v1/incidents/{id}`, and `GET /v1/health`) are read-only and unauthenticated in Phase 3a-ii; **roles come later**.
- **Deterministic Detection Engine:** `DetectionEngine.tick(now)` takes `now` as a parameter to ensure fully deterministic, mock-clock testing with zero `time.sleep` in unit test suites. A background daemon thread in FastAPI lifespan invokes `tick(utcnow)` every `tick_s`.
- **Active Incident Singularity:** A unique partial SQLite index `idx_incidents_active_centre` enforces at most one active (`open` or `recovering`) incident per `(exam_id, centre_id)`. New incidents for a centre can only be created after resolution ($n+1$ sequence naming).
- **Rule A vs Rule B:** Explicit hardware signals (`POWER_LOSS`, `NETWORK_DOWN`) immediately trigger high-confidence incidents independent of startup grace or ingest stalls. Inferred silence incidents (`CENTRE_LOSS_FRACTION`) require $D \ge 5$, `silent_fraction >= 0.6`, non-grace, and non-stalled state.
- **Audit Verification & Notice Delivery:** Incident state transitions (`opened`, `reclassified`, `escalated`, `recovering`, `resolved`) append an audit log entry with entry_type `"incident"`. Background ticks without status transitions produce zero audit spam. Multi-audience notices are delivered to admin and centre targets upon transition.

---

## 13. Phase 3b-i Schema Extensions & Impact Engine Decisions
- **`incident_impacts` Table Extensions:** Added columns `evidence TEXT` (canonical JSON storing exact event IDs used and baseline quality metrics), `computed_at TEXT` (ISO timestamp of computation), and `impact_version INTEGER DEFAULT 1`. Primary key remains `(incident_id, candidate_id)`.
- **`incidents` Table Extensions:** Added column `impact_computed_at TEXT` recording the timestamp when the post-resolution impact assessment was executed.
- **`review_queue` Table Specification:** Defined table with primary key `review_id` and unique constraint `(incident_id, candidate_id)` enforced via `idx_review_queue_inc_cand`. Valid status values: `pending`, `superseded`, `resolved`. Added index `idx_review_queue_status`.
- **Simulator Ground Truth Metrics:** Added per-candidate fields `lost_s_from_fault_start` (difference between candidate `resumed_ts` and fault `start_ts` for power_loss, 0 for network_drop) and `expected_lost_answers` (`unsaved_answers_at_start` for power_loss, 0 for network_drop). These remain strictly confined to the simulator process and test fixtures; never imported or read by `app/`.
- **Exposed Set Isolation:** Exposed candidate sessions are derived deterministically from the events and session records of the incident centre with `started_at <= window_start` and not submitted before `window_start`. Unaffected centres are strictly excluded.
- **Pure-Function Remedy Evaluation:** Pure typed Python logic with zero `eval()`/`exec()`. Evaluates strictly in order R3 -> R4 -> R1 -> R2 using configurable thresholds from `config.yaml`.

---

## 14. Phase 3b-ii Decisions, Fairness Gate & Candidate Communications Decisions
- **`decisions` Table Extensions:** Added columns `extra_seconds INTEGER`, `rule_id TEXT`, `recommended_remedy TEXT`, `impact_version INTEGER`, `supersedes INTEGER`, `acknowledged_fairness INTEGER DEFAULT 0`, `fairness_snapshot TEXT` (JSON), and `decided_at TEXT`. Index on `(incident_id, candidate_id, decision_id)`.
- **Deliberate Design Choice — No 'auto' Mode:** Mode is strictly `'approved'` or `'overridden'`. Mode `'auto'` is deliberately excluded: every remedy requires an explicit human decision by an exam controller or proctor, ensuring complete institutional accountability.
- **`notices` Table Extensions & Idempotency:** Added columns `kind TEXT` and `ref_key TEXT` with a unique index on `ref_key` (`idx_notices_ref_key`). Notice creation is guaranteed idempotent; duplicate notifications cannot be generated under retries or multiple ticks.
- **Fairness Gate:** Evaluates cross-centre fairness across all resolved incidents for an exam. Flags anomalies when `ratio_spread > 1.5` or `manual_review_gap > 0.3` for centres meeting `min_rows_per_centre`. Bulk approvals are gated: if the exam is flagged, `POST /v1/incidents/{incident_id}/decisions/approve-all` is rejected with HTTP 409 unless `acknowledge_fairness: true` is supplied.
- **Controller Authorization & Privacy:** Controller mutations require header `X-Controller-Key`. GET endpoints remain open (roles come later). Candidate status endpoint (`GET /v1/status/{candidate_id}`) enforces strict data isolation: only the specific candidate's confirmed status is returned; unconfirmed remedy recommendations and decision reasons are strictly hidden.

---

## 15. Phase 3b-ii Hardening, Stale Telemetry & Simulator Blind Window Decisions
- **`impact.stale_hb_factor` Tightening & Hidden Clamp Removal:** Removed hardcoded `effective_stale_factor = min(stale_factor, 1.8)` in `app/core/impact.py`. The real value needed is `1.8` and is now explicitly configured in `config.yaml` (`impact.stale_hb_factor: 1.8`) and read directly by the engine and rationale text.
  - *Measurement on CAND-000074:* Under `stale_hb_factor: 2.5` (threshold $2.5 \times 0.5\text{ s} = 1.25\text{ s}$), CAND-000074 had `stale_seconds = 0.91 s <= 1.25 s`, classifying it as `strong`. Its last heartbeat recorded $L=13$, and last save was $S=12$, computing $L - S = 1$ unsaved answer. However, ground truth had 2 lost answers ($L=14$ in memory wiped on power cut). A stale heartbeat does *not* recalculate answer counts; rather, delayed heartbeat telemetry means the server's view of $L$ freezes in the past while the candidate answers an unconfirmed question before the outage. Tightening to `1.8` ($1.8 \times 0.5\text{ s} = 0.90\text{ s} < 0.91\text{ s}$) correctly demotes CAND-000074 to `partial` (manual review), eliminating the calibration violation on `strong` rows.
- **Simulator Step Ordering & Blind Window (`simulation.answer_before_heartbeat`):** Added configurable boolean switch in `config.yaml` (`simulation.answer_before_heartbeat: true`, default `true`).
  - *The Intra-Step Blind Window:* If answers execute *after* the heartbeat (`answer_before_heartbeat: false`), a candidate answering in step $k$ increments memory `local_seq`, but that step's heartbeat already transmitted the prior sequence. If an outage occurs before step $k+1$, that answer is destroyed in memory but was never observed by the server, causing $L$ to lag by 1. Empirical measurement: `answer_before_heartbeat: false` produced 23 calibration violations and 5 oracle violations (out of 36 strong rows), whereas `true` produced 0 calibration and 0 oracle violations.
- **Principled Baseline Slot Regularity Alignment:**
  - *Baseline Regularity Definition:* Baseline slots are defined as the up to `baseline_slots: 10` intervals immediately preceding `window_start`, clipped to the session's actual `started_at`.
  - *Session Lifecycle:* Seeded sessions initialize with `started_at = NULL` and `state = 'registered'`. The `started_at` timestamp is set strictly from the `SESSION_STARTED.ts` event upon candidate login.
  - *Integer Allowance:* Compares `missing_slots > floor(baseline_missing_max * len(available_slots))` rather than an unfloored ratio, ensuring candidates missing $\le 20\%$ of available slots remain `strong`.
  - *Validation:* Passes `test_13` under both override (0.5 s) and real (2.0 s) intervals with manual review share $\le 7.5\% \le 20\%$, eliminating false-positive fairness anomalies.

---

## 16. Evidence Rules As Built

### 16.1 Designated Pitch Sentence
> "We don't guess lost time from network silence; we prove it from monotonic sequence gaps and signed local saves, while preserving human-in-the-loop controller sovereignty."

### 16.2 Stale Factor 1.8 Rationale
At heartbeat interval $\Delta$, candidate edge telemetry is expected every $\Delta$ wall-clock seconds. If an outage occurs, the last observed heartbeat timestamp `last_good_ts` may precede `window_start`.
- If `stale_seconds = (window_start - last_good_ts) <= 1.8 * interval`, the telemetry at outage inception is verified fresh within one heartbeat interval plus $0.8 \times \Delta$ network jitter tolerance.
- If `stale_seconds > 1.8 * interval`, telemetry was unobserved during the critical pre-outage window. In this unobserved window, a candidate could have submitted an unconfirmed answer ($L$ incremented in memory) that is wiped upon power cut. Because the server's view of $L$ is frozen in the past, calculating compensation automatically risks under-compensating the candidate.
- Tightening `stale_hb_factor` from `2.5` to `1.8` correctly classifies unobserved sessions (e.g. CAND-000074 at 0.5s interval with 0.91s staleness) as `partial`, routing them safely to manual review (Rule R3) and guaranteeing 100% calibration and oracle accuracy on all `strong` rows.

### 16.3 Telemetry Sequencing: `answer_before_heartbeat`
Empirical validation of simulator step sequencing under power loss at C-BPL-02 (seed 36):

| Configuration | Evaluated Strong | Calibration Accuracy | Oracle Accuracy | Rationale & Failure Mode |
|---|---|---|---|---|
| `answer_before_heartbeat: false` | 36 / 40 | 13 / 36 (36.1%) | 31 / 36 (86.1%) | **Intra-Step Blind Window:** Heartbeat transmitted before answer submission in step $k$. When power cuts before step $k+1$, memory answers are wiped without ever being broadcast to server; server $L$ lags memory by 1. |
| `answer_before_heartbeat: true` (As Built) | 36 / 40 | 36 / 36 (100.0%) | 36 / 36 (100.0%) | **Committed Telemetry:** Answer submission increments local sequence $L$ before heartbeat dispatch; each heartbeat truthfully advertises latest $L$, ensuring exact server-side recovery calculation. |

### 16.4 Session `started_at` Lifecycle & Regularity Slot Grid
1. **Seeding:** All candidate sessions are initialized in SQLite with `started_at = NULL`, `last_heartbeat_at = NULL`, and `state = 'registered'`.
2. **Ingest Binding:** When the candidate's `SESSION_STARTED` event arrives at ingest, `started_at` is set via `CASE WHEN started_at IS NULL OR ? < started_at THEN ? ELSE started_at END` to the exact event source timestamp `ts`.
3. **Detection Domain $D$ & Exposed Set:** Sessions with `started_at IS NULL` are unstarted: they are excluded from the active centre detection domain $D$ and excluded from incident exposed sets.
4. **Regularity Slot Grid:** The baseline slot window evaluates the $K = 10$ slots immediately preceding `window_start` ($[T_{\text{ws}} - K\Delta, T_{\text{ws}}]$). Slots are clipped to `slot_t0 >= started_at`. Because `started_at` is strictly bounded to the candidate's actual start time rather than a synthetic midnight placeholder (`00:00:00`), pre-start intervals are not counted as missing heartbeats.



