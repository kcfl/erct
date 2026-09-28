# Architecture & Implementation Decisions
**Project:** Exam Resilience Control Tower (ERCT)  
**Hackathon:** MPOnline Idea & Innovation Hackathon 2026 (Technical Track, PS6)  
**Target Path:** `D:\mp online hackathon\erect`

---

## 1. Environment & Operational Baseline
- **Primary OS:** Windows 10/11 is primary. Scripts and commands must quote all paths containing spaces.
- **Process Orchestration (`run.bat`):** Must start the FastAPI backend and the simulator as **two separate processes** with independent PIDs to enable the live API kill/restart zero-duplicate replay demo.
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
- **Telemetry Signature:** Centre edge agent emits `POWER_LOSS` (`source`: grid/ups/generator, `backup_minutes`), immediately followed by heartbeat silence from all candidate sessions at that centre.
- **Detection Criteria:**
  - Explicit `POWER_LOSS` event received, OR
  - $\ge 60\%$ (configurable via `detection.centre_loss_fraction`) of sessions at a centre go silent within a 10s window (`detection.centre_loss_window_s`).
- **Heartbeat Rhythm:** Normal heartbeat every 2s (`simulation.heartbeat_interval_s`); silence declared after 6s gap (`detection.heartbeat_gap_s`). Incident opens within 10 seconds.
- **Incident Classification:**
  - Classify as `power` if a `POWER_LOSS` event exists within the detection window.
  - Classify as `network` if a `NETWORK_DOWN` event exists within the detection window.
  - Otherwise, classify as `network` with `evidence_quality = partial` (low confidence flag).
- **Time Compression:** Support a configurable `demo_time_scale` in `config.yaml` to accelerate demo timelines if needed.

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
