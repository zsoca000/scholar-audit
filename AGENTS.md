# Research Intelligence Pipeline (`lit-pipeline`)

> **MANDATORY STARTUP RULE FOR AGENTS:**
> ALWAYS read `prompts/journal.md` FIRST at the start of any conversation or task. `prompts/journal.md` contains the active project state, recent decisions, environment details, and current roadmap.

---

## 1. Goal and Core Philosophy
An ultra-clean, modular research intelligence engine designed to extract structured evidence from academic literature and generate comparative synthesis reports.

The architecture strictly separates **Inputs**, **Agent Roles**, **Execution Code**, **Temporary Cache**, and **Outputs**.

---

## 2. Minimalist Directory Structure

```
lit-pipeline/
│
├── pyproject.toml            # Project dependencies & package metadata
│
├── configs/                  # [FIXED INPUTS] Research definitions and bibliographies (version-controlled)
│   ├── template.yaml         # Configuration template
│   └── sample.bib            # Readable Zotero / Mendeley .bib bibliography
│
├── prompts/                  # [AGENT DEFINITIONS & STATE] Prompts, role instructions and active journal
│   ├── journal.md            # [STATE & DECISIONS] Single source of truth across sessions (READ FIRST!)
│   └── agents/
│       ├── triage_agent.md       # Triage subagent: abstract screening, zero-hallucination rules
│       ├── extraction_agent.md   # Extraction subagent: full-text auditing, mandatory verbatim citations
│       └── synthesis_agent.md    # Synthesis subagent: landscape analysis, research gap identification
│
├── src/                      # [APPLICATION LOGIC] Core engine - clean Python modules
│   ├── discovery.py          # Phase 1: OpenAlex API search + citation snowballing graph
│   ├── triage.py             # Phase 2: Abstract screening (invokes prompts/agents/triage_agent)
│   ├── extraction.py         # Phase 3: Full-text extraction (invokes prompts/agents/extraction_agent)
│   ├── synthesis.py          # Phase 4: Report generation (invokes prompts/agents/synthesis_agent)
│   └── main.py               # Unified CLI entrypoint
│
├── cache/                    # [TRANSIENT / SAFE TO DELETE] (Gitignored)
│   ├── openalex/             # Raw API JSON responses (avoid duplicate network requests)
│   └── fulltexts/            # Downloaded raw papers (PDF/HTML)
│
└── output/                   # [RESULTS] Generated artifacts and reports per study
    └── <study_name>/
        ├── 01_papers.parquet # Discovered and deduplicated paper metadata
        ├── 02_triaged.parquet# Abstract-screened papers
        ├── 03_evidence.json  # Extracted structured facts and verbatim quotes
        └── report.md         # Final human-readable synthesis report
```

---

## 3. Agent Hierarchy and Delegation Strategy

### A. Main Agent (Lead Architect / Coordinator - Main Chat)
- Interacts directly with the user in chat (in Hungarian if the user speaks Hungarian).
- Develops, tests, and maintains the `src/` modules.
- Consults and updates `journal.md` continuously.
- When high-volume, parallelizable reading tasks arise (e.g., screening 200 abstracts or deeply auditing 30 papers), avoids context bloat by:
  - **Spawning subagents (`invoke_subagent`)** following dedicated instructions in `prompts/agents/*.md`,
  - OR delegating through background Python batch scripts in `src/`.

### B. Specialized Subagents (`prompts/agents/`)
1. **`triage_agent.md`:**
   - **Role:** Rapid abstract screening.
   - **Rule:** High recall (when uncertain, retain rather than exclude), mandatory single-sentence factual rationale, zero hallucination.
2. **`extraction_agent.md`:**
   - **Role:** Deep methodological auditor.
   - **Rule:** Every extracted claim must include a verbatim quotation (`verbatim_quote`) and section pointer. If a paper omits an item, explicitly state `NOT_REPORTED`.
3. **`synthesis_agent.md`:**
   - **Role:** Synthesis and research gap identification.
   - **Rule:** Generate structured comparative tables, highlighting blind spots and under-explored areas in the field.

---

## 4. Development Guidelines & Workflow
- **Language & Dependencies:** Python 3.10+ (ARM64 Windows compatible), `httpx`, `pandas`, `duckdb` (for Parquet I/O), `bibtexparser`, `pydantic`.
- **Step-by-Step Implementation:** Implement `src/discovery.py` first, validating against `configs/sample.bib`.
- **Collaborative Review:** Confirm design choices and external API touchpoints with the user before major code changes.
