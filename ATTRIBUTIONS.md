# Third-Party Attributions & Open-Source Disclosures
**Project:** Exam Resilience Control Tower (ERCT)  
**Hackathon:** MPOnline Idea & Innovation Hackathon 2026 (Problem Statement 6)

In compliance with the hackathon rules and ethical development practices, this document discloses all third-party libraries, specifications, and AI assistance utilized in this repository.

---

## 1. Libraries and Frameworks

| Library / Tool | Version | License | Purpose / Component |
| :--- | :--- | :--- | :--- |
| **FastAPI** | `>=0.110.0` | MIT License | High-performance asynchronous REST API and SSE streaming backend. |
| **Uvicorn** | `>=0.28.0` | BSD 3-Clause | ASGI web server for hosting the FastAPI application. |
| **Pydantic** | `>=2.6.0` | MIT License | Strict data validation, schema enforcement, and type safety for models. |
| **Pydantic Settings** | `>=2.2.0` | MIT License | Configuration loading and validation. |
| **PyYAML** | `>=6.0.1` | MIT License | Parsing configuration from `config.yaml`. |
| **SQLite3** | Standard Lib | Public Domain | High-concurrency local embedded database configured in WAL mode. |
| **HTTPX** | `>=0.27.0` | BSD 3-Clause | Client for testing and simulator store-and-forward queue dispatch. |
| **Pytest** | `>=8.0.0` | MIT License | Automated test runner for unit, integration, and E2E verification. |
| **Pytest-asyncio** | `>=0.23.0` | Apache 2.0 | Async test fixtures for FastAPI endpoints. |
| **Jinja2** | `>=3.1.3` | BSD 3-Clause | HTML template rendering for standalone audit/incident report exports. |

---

## 2. Cryptographic Specifications & Standards

| Standard | Description | Application in ERCT |
| :--- | :--- | :--- |
| **SHA-256 (FIPS 180-4)** | Cryptographic hash algorithm | Generates deterministic tamper-evident hash links for the audit chain. |
| **RFC 8785** | JSON Canonicalization Scheme (JCS) | Normalizes audit JSON payloads (sorted keys, no whitespace, UTF-8) before hashing. |
| **ISO 8601** | Date and time representation | Universal UTC timestamp representation across all telemetry and audit blocks. |

---

## 3. AI Assistance Disclosure

| Assistant | Provider | Role in Project |
| :--- | :--- | :--- |
| **Gemini / Antigravity Agent** | Google DeepMind | Architecture design consultation, SQL schema translation, test generation, and implementation support. |

---

## 4. Proprietary & Hackathon IP Notice
- All business logic, resilience algorithms, readiness gating logic, and remedy rules (R1–R4) were authored specifically during the MPOnline Idea & Innovation Hackathon 2026 competition window. Zero prior project code was reused.
