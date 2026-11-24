# 🔬 Scholar Audit (`lit-pipeline`)

> **Turn hundreds of academic papers into structured evidence, verified quotes, and publication-ready synthesis reports — without the hallucination headache.**

---

### 🤔 What is this, and why is it not "just another paper scraper"?

Most academic scripts just search a keyword, dump 300 PDFs into a messy folder, and leave you to read them manually. 

**Scholar Audit is a research intelligence engine.** It doesn't just collect papers — it **audits** them like a rigorous peer-reviewer:

- 🕸️ **Citation Graph Snowballing:** Discovers papers not only via boolean keywords, but by traversing the forward and backward citation network of your golden seed papers.
- 🎯 **Agentic Triage:** Screens abstracts against strict, user-defined inclusion/exclusion rules with zero hallucination, filtering out 85%+ of off-topic noise before you download anything.
- 📑 **1-Click Zotero Integration:** Automatically generates a clean, scoped `.bib` collection ready for bulk full-text retrieval on your university or institutional access.
- 🔍 **In-Depth Methodological Audit:** Reads the actual full-text PDFs (method sections, equations, setup protocols) and extracts structured answers to your core scientific dilemmas — requiring **mandatory section pointers and exact verbatim quotes** as proof.
- 📊 **Research Gap Discovery:** Quantifies community blind spots (e.g., *"68% of papers ignore synthetic generator bias"*), generates interactive Excel-ready matrices, and compiles an executive literature review report.

**100% Config-driven. Zero hardcoded logic. Reusable for any scientific field.**

---

## 🚀 Quickstart in 4 Steps

### 1. Setup Environment
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r pyproject.toml  # or: pip install duckdb pandas pydantic pyyaml pypdf httpx tqdm
```

### 2. Configure Your Study
Duplicate `configs/template.yaml` to `configs/my_study.yaml` and define:
- `study_name`: folder identifier (e.g., `constitutive_history_shift`)
- `discovery`: queries, seed `.bib`, min_year, snowball depth
- `triage`: inclusion / exclusion criteria and threshold
- `extraction`: path to your downloaded PDFs (`pdf_source`) and extraction `schema`

---

## 🛠️ The 4-Phase Workflow

### Phase 1: Discovery (100% Deterministic Python, 0% AI)
Searches OpenAlex using boolean queries, loads your golden seed `.bib` file, and performs multi-hop forward and backward citation snowballing.
```bash
python src/main.py discover -c configs/my_study.yaml
```
- **Output:** `output/<study>/01_discovered.parquet` (pure metadata, no full text)
- **Check it:** `python src/main.py diagnose -c configs/my_study.yaml --phase discovered`

---

### Phase 2: Triage (Agentic Abstract Screening)
Evaluates abstracts against your YAML criteria using zero-hallucination agent rules (`prompts/agents/triage_agent.md`). Automatically exports a Zotero-ready `.bib` file for 1-click import.
```bash
# Using external API (Gemini/OpenAI) or local workflow:
python src/main.py triage -c configs/my_study.yaml --mode api
```
- **Outputs:**
  - `output/<study>/02_triaged.parquet` (accepted relevant papers)
  - `output/<study>/02_triaged.bib` (ready for 1-click Zotero import!)
- **Check it:** `python src/main.py diagnose -c configs/my_study.yaml --phase triaged`

---

### Phase 3: PDF Acquisition & In-Place Audit (Human-in-the-Loop)
1. **Zotero Download (Manual 2-minute step):**
   - Import `02_triaged.bib` into Zotero as a collection.
   - Select all (`Ctrl+A`) -> right-click -> **"Find Available Full Text"** on your institutional network.
   - Right-click collection -> **"Export Collection..."** (BibTeX, check **[X] Export Files**).
   - Point `extraction.pdf_source` in your YAML to this export folder.
2. **Reconciliation & Linking Check:**
   ```bash
   python src/main.py extract -c configs/my_study.yaml --check
   ```
   *No files are copied or duplicated into cache; PDFs are read in-place directly from your directory.*
3. **Execute Extraction Audit:**
   Performs deep methodological extraction on each paper according to your YAML schema with mandatory verbatim quotes and section pointers.
   - **Outputs:** `output/<study>/03_evidence.parquet` and `03_evidence.json`

---

### Phase 4: Synthesis & Meta-Analysis
Dynamically aggregates evidence across whatever variables you defined in your schema and generates publication-grade reports and filterable master matrices.
```bash
python src/main.py synthesize -c configs/my_study.yaml
```
- **Outputs:**
  - `output/<study>/report.md`: Complete Markdown research synthesis report.
  - `output/<study>/03_evidence_matrix.csv`: Flat table (all papers $\times$ all questions, quotes, sections) — open directly in Excel or Sheets!
  - `output/<study>/03_evidence_matrix.json`: Machine-readable JSON matrix.

---

## 📊 Live Monitoring & Diagnostics
Check the status, charts, and funnel metrics of any phase anytime:
```bash
python src/main.py diagnose -c configs/my_study.yaml --phase all
```

---

## 📁 Clean Output Architecture
Every study run produces an identical, predictable artifact folder:
```text
output/<study_name>/
├── 01_discovered.parquet       # Raw discovered metadata
├── 01_discovered_summary.md    # Discovery metrics & sources chart
├── 02_triaged.parquet          # Screened articles & rationales
├── 02_triaged.bib              # 1-click Zotero import bibliography
├── 02_triaged_summary.md       # Acceptance rate & venue concentration
├── 03_pdf_manifest.md          # Full-text linking reconciliation log
├── 03_evidence.parquet         # Structured extracted facts & quotes
├── 03_evidence_matrix.csv      # Flat Excel-compatible paper matrix
├── 03_evidence_matrix.json     # Complete structured evidence JSON
└── report.md                   # Final synthesis & research gap report
```
