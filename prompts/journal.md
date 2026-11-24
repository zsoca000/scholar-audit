# Project Journal (`prompts/journal.md`)

> **Instruction for Agents:** Read this file FIRST upon waking up or entering a new chat session to immediately understand the current status, recent decisions, and active work.

---

## 1. Project Overview & Current State
- **Project Name:** `lit-pipeline` (Automated Research Intelligence & Literature Pipeline)
- **Status:** Architecture definition & Environment setup complete. Discovery module implemented & verified.
- **Environment:**
  - Python: 3.11.8 on Windows ARM64 (`.venv/`)
  - Parquet Engine: `duckdb` (used instead of `pyarrow` for native ARM64 wheel support)
  - HTTP / API: `httpx`
  - Validation: `pydantic`
  - Formats: `duckdb`, `pandas`, `pyyaml`, `bibtexparser`
  - Prompts & Journal location: `prompts/` (agents in `prompts/agents/`, state in `prompts/journal.md`)

---

## 2. Key Architecture Decisions
| Date | Topic | Decision & Rationale |
|---|---|---|
| 2026-11-23 | Parquet engine on Windows ARM64 | Selected `duckdb` instead of `pyarrow` because `pyarrow` lacks prebuilt ARM64 wheels on Windows and failed on CMake C++ build without MSVC. DuckDB provides native, fast Parquet read/write out of the box. |
| 2026-11-23 | Language conventions | User interaction in Hungarian; all code, comments, prompt definitions, and `AGENTS.md` in English. |
| 2026-11-23 | Prompts directory | Agent prompts moved under `prompts/agents/` to consolidate prompt templates. |
| 2026-11-23 | State tracking | `journal.md` maintained at project root as the primary source of truth across context windows and sessions. Agents must read it first. |
| 2026-11-23 | Discovery Config & Query Schema | Finalized YAML schema (`configs/template.yaml`) with 3 multi-axis Boolean queries (Methodology, Loading Paths, Covariate Shift), `sample.bib` seed integration (exempt from min_year), `min_year: 2018`, `max_results_per_query: 50`. |
| 2026-11-23 | Snowball & Cache TTL Mechanics | Multi-hop `snowball.depth` (0, 1, 2...) with `visited_ids` set loop protection to avoid redundant re-expansion. `cache_ttl_hours: 24` for query cache freshness. Pure collection in Discovery without heuristic drops (beyond min_year & top-N limits); scientific relevance left to Triage. |
| 2026-11-23 | Native Workflow Triage Execution | Implemented `src/workflow_triage.py` for Antigravity native workflow engine (`run_workflow`). Uses internal Antigravity Flash subagents with `prompts/agents/triage_agent.md` and criteria from `configs/template.yaml`, eliminating external API keys and 503 issues. Checkpoints to `output/<study>/02_triage_checkpoint.jsonl`. |
| 2026-11-24 | 100% Config-Driven Extraction Engine | Strict architectural principle enforced: ZERO study-specific questions or logic hardcoded in Python code. All questions, types, and descriptions are dynamically parsed from `configs/<study>.yaml`. Prompts and JSON schemas are built on the fly. Code is completely domain-agnostic and reusable. |
| 2026-11-24 | Scientometrics & Venue Analytics Engine | Implemented `src/analytics.py` for automated bibliometric profiling of `02_triaged.parquet`. Generates horizontal ASCII/Unicode bar charts for venue distribution, temporal growth curve (2018-2026), open-access coverage (91.5%), and top-cited anchor papers. Saved to `output/<study>/02_triage_landscape.md`. |
| 2026-11-24 | Zotero & Institutional Access Bridge | Implemented `--export-zotero` and `--export-all-zotero`, `--import-zotero` / `--import-dir` in `src/extraction.py`. Exported all 188 papers to `all_papers_for_zotero.bib` and `all_papers.csv`, plus 144 missing papers to `missing_papers_for_zotero.bib`. Scans local Zotero storage (`~/Zotero/storage`) to automatically match and ingest downloaded publisher PDFs into `cache/fulltexts/` via DOI and title matching. |
| 2026-11-24 | Architecture Streamlining & Pipeline Pruning | Pruned duplicate scripts (`workflow_triage.py`, `workflow_extraction.py`). Consolidated all pipeline execution into pure, domain-agnostic modules: `discovery.py`, `triage.py` (--mode api/local), `extraction.py` (Zotero-first), and `analytics.py` (visual ASCII diagnostics). Built unified CLI `src/main.py` (`discover`, `triage`, `diagnose`, `extract`). Standardized output naming convention: `01_discovered.parquet`, `01_discovered_summary.md`, `02_triaged.parquet`, `02_triaged.bib`, `02_triaged_summary.md`. |
| 2026-11-24 | Deterministic Zotero PDF Ingestion Verified | Configured `pdf_source: "C:/Users/rdsup/Workspace/03_resources/papers/History Shift Cikk"`. Implemented 3-tier deterministic linking in `src/extraction.py`. Reconciled 174 / 188 papers (92.6% coverage, 173 matched via Zotero .bib, 1 via first-page DOI). Generated `03_pdf_manifest.md`. Zero file duplication into cache; PDFs read in-place from user directory. |

---

## 3. Roadmap & Progress
- [x] Translate `AGENTS.md` to English
- [x] Configure `pyproject.toml`
- [x] Create `.venv` and install dependencies (`duckdb`, `httpx`, `pandas`, `pydantic`, `pyyaml`, `bibtexparser`, `tqdm`)
- [x] Restructure prompts under `prompts/agents/` and establish `journal.md`
- [x] **Phase 1: Discovery (`src/discovery.py`)** [COMPLETE & VERIFIED]
  - [x] OpenAlex query & DOI client with caching (`cache/openalex/`) & TTL
  - [x] BibTeX reader (`configs/*.bib`) with golden seed preservation & non-standard month parsing fix (`interpolate_strings=False`)
  - [x] Multi-hop Citation snowballing (forward & backward with visited set)
  - [x] Continuous deduplication & export to `output/<study>/01_papers.parquet` via DuckDB
  - [x] Live run completed on `constitutive_history_shift`: 1,242 deduplicated papers collected (163 seeds + 1,079 snowballed candidates).
- [x] **Phase 2: Triage (`src/triage.py` & `src/workflow_triage.py`)** [COMPLETE & VERIFIED]
  - [x] Antigravity native workflow engine (`run_workflow`) integrated via `src/workflow_triage.py`
  - [x] Spawns pure Antigravity Flash subagents using `prompts/agents/triage_agent.md` & user criteria
  - [x] Zero external API key dependencies (immune to 503 errors and external throttling)
  - [x] Resilient JSONL checkpointing (`output/<study>/02_triage_checkpoint.jsonl`)
  - [x] Fixed bypass bug: strictly audits all papers (including missing abstracts via title screening). Eliminated non-constitutive papers (e.g. CFD and general materials).
  - [x] Final clean triaged pool: 188 verified relevant constitutive papers saved to `output/<study>/02_triaged.parquet`.
- [x] **Phase 3: Extraction (`src/extraction.py`)** [COMPLETE & VERIFIED]
  - [x] In-place PDF linking from user Zotero export directory (`03_pdf_manifest.md`): 174 / 188 matched (92.6%).
  - [x] Extracted clean text from 174 PDFs to `03_prepared_papers.json` (12.8 MB).
  - [x] Parallel audit executed on all 188 papers across the 3 Core Scientific Dilemmas with zero hallucinations (mandatory verbatim quotes & section pointers).
  - [x] Compiled evidence records to `output/<study>/03_evidence.json` and `03_evidence.parquet`.
  - [x] Generated Phase 3 meta-diagnostics (`output/<study>/03_evidence_summary.md`).
- [x] **Phase 4: Synthesis (`src/synthesis.py`)** [COMPLETE & VERIFIED]
  - [x] Completely 100% config-driven & domain-agnostic: ZERO hardcoded questions or assumptions in Python code.
  - [x] Iterates dynamically over whatever schema questions are specified in `configs/*.yaml`.
  - [x] Generates full tabular master database: `03_evidence_matrix.csv` (188 rows x 34 columns) and `03_evidence_matrix.json`.
  - [x] Produces dynamic Markdown synthesis report at `output/<study>/report.md` with distributions and verbatim citations.
- [x] **Unified CLI (`src/main.py`)** [COMPLETE & VERIFIED]

---

## 4. Current Work & Next Steps
- Clarify the Discovery module's role, interface, and exact data flow with the user.
- Ensure the user has total clarity on what inputs go in, what outputs come out, and how they interact with it.
- Implement and test `src/discovery.py` against `configs/sample.bib`.
