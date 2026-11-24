"""
Extraction Module for lit-pipeline.

Phase 3 of Research Intelligence Pipeline:
- Downloads full-text open-access PDFs to cache/fulltexts/.
- Extracts and cleans text from PDF documents using pypdf.
- Audits full text with extraction agents according to prompts/agents/extraction_agent.md.
- Extracts structured answers matching study schema with mandatory verbatim quotes.
- Enforces strict NOT_REPORTED handling for omitted methodology aspects.
- Resilient checkpointing (output/<study>/03_extraction_checkpoint.jsonl).
- Exports structured evidence to output/<study>/03_evidence.json and 03_evidence.parquet.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import httpx
import pandas as pd
from pypdf import PdfReader
import yaml
from pydantic import BaseModel, Field
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("extraction")


# ============================================================================
# 1. Pydantic Models for Evidence Extraction
# ============================================================================

class ClaimEvidence(BaseModel):
    value: Any = Field(description="Extracted value: string, boolean, or number. Or 'NOT_REPORTED'.")
    verbatim_quote: str = Field(default="", description="Exact sentence quoted from article proving the claim.")
    section: str = Field(default="", description="Section pointer, e.g. Section 2.3 / Page 4.")
    confidence: str = Field(default="HIGH", description="Confidence level: HIGH, MEDIUM, LOW.")


class PaperEvidence(BaseModel):
    paper_id: str
    title: str
    doi: Optional[str] = None
    has_fulltext: bool = False
    fulltext_source: str = "none"  # "pdf", "abstract_only", "manual_pdf"
    answers: Dict[str, ClaimEvidence] = Field(default_factory=dict)


# ============================================================================
# 2. PDF Download & Text Extraction Manager
# ============================================================================

class FullTextManager:
    """Manages downloading, caching, and text extraction for full-text PDFs."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir / "fulltexts"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/pdf,*/*",
        }
        self.client = httpx.Client(timeout=35.0, follow_redirects=True, headers=self.headers)

    def close(self):
        self.client.close()

    def get_safe_filename(self, paper_id: str) -> str:
        clean_id = paper_id.replace("https://openalex.org/", "").strip()
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", clean_id)
        return f"{safe_name}.pdf"

    def download_pdf(self, paper_id: str, pdf_url: Optional[str]) -> Optional[Path]:
        """Downloads PDF if not cached. Returns Path if valid PDF on disk."""
        filename = self.get_safe_filename(paper_id)
        pdf_path = self.cache_dir / filename

        # 1. Check if already cached
        if pdf_path.exists() and pdf_path.stat().st_size > 1024:
            return pdf_path

        # 2. Check if a valid URL exists
        if not pdf_url or not pdf_url.startswith("http"):
            return None

        # 3. Download
        try:
            resp = self.client.get(pdf_url)
            if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
                pdf_path.write_bytes(resp.content)
                logger.debug(f"Downloaded PDF ({len(resp.content)/1024:.1f} KB) for {paper_id}")
                return pdf_path
            else:
                logger.debug(f"URL did not return valid PDF bytes (status {resp.status_code}) for {paper_id}")
                return None
        except Exception as e:
            logger.debug(f"Failed to download PDF from {pdf_url}: {e}")
            return None

    def extract_clean_text(self, pdf_path: Path, max_chars: int = 150000) -> str:
        """Extracts text from PDF, normalizes formatting, and strips trailing references."""
        import hashlib

        text_cache_dir = self.cache_dir.parent / "texts"
        text_cache_dir.mkdir(parents=True, exist_ok=True)
        safe_hash = hashlib.md5(str(pdf_path).encode("utf-8")).hexdigest()[:16]
        cached_txt = text_cache_dir / f"{safe_hash}.txt"

        if cached_txt.exists() and cached_txt.stat().st_size > 100:
            return cached_txt.read_text(encoding="utf-8")

        try:
            reader = PdfReader(str(pdf_path))
            pages_text = []
            for p in reader.pages:
                txt = p.extract_text()
                if txt:
                    pages_text.append(txt)

            full_text = "\n\n".join(pages_text)

            # Fix hyphenation across line breaks (e.g., plas-\nticity -> plasticity)
            full_text = re.sub(r"(\w+)-\n(\w+)", r"\1\2", full_text)

            # Strip trailing References / Bibliography from the last 35% of text
            text_len = len(full_text)
            tail_start = int(text_len * 0.65)
            tail = full_text[tail_start:]

            ref_match = re.search(
                r"\n(?:[0-9]+\.?\s*)?(?:References|REFERENCES|Bibliography|BIBLIOGRAPHY)\s*\n",
                tail,
            )
            if ref_match:
                full_text = full_text[: tail_start + ref_match.start()]

            # Cap length if exceeding maximum limit
            if len(full_text) > max_chars:
                full_text = full_text[:max_chars]

            res = full_text.strip()
            if len(res) > 500:
                try:
                    cached_txt.write_text(res, encoding="utf-8")
                except Exception:
                    pass
            return res
        except Exception as e:
            logger.warning(f"Error extracting text from {pdf_path.name}: {e}")
            return ""


# ============================================================================
# 3. Extraction Runner & Prompt Formatting
# ============================================================================

def format_schema_for_prompt(schema_list: List[Dict[str, Any]]) -> str:
    """Formats extraction questions into a clear prompt block."""
    lines = []
    for item in schema_list:
        qid = item.get("id")
        desc = item.get("description", "")
        qtype = item.get("type", "string")
        lines.append(f"- Question ID: '{qid}' (Type: {qtype}) -> {desc}")
    return "\n".join(lines)


def build_agent_json_schema(schema_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generates strict JSON schema for the extraction agent."""
    properties = {}
    required = []
    for item in schema_list:
        qid = item["id"]
        required.append(qid)
        properties[qid] = {
            "type": "object",
            "properties": {
                "value": {"type": ["string", "boolean", "number"]},
                "verbatim_quote": {"type": "string"},
                "section": {"type": "string"},
                "confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
            },
            "required": ["value", "verbatim_quote", "section", "confidence"],
        }

    return {
        "type": "object",
        "properties": {
            "paper_id": {"type": "string"},
            "answers": {
                "type": "object",
                "properties": properties,
                "required": required,
            },
        },
        "required": ["paper_id", "answers"],
    }


class ExtractionRunner:
    """Orchestrates PDF acquisition, text extraction, checkpointing, and output."""

    def __init__(self, config_path: str | Path, base_dir: Path):
        self.base_dir = base_dir
        self.config_path = base_dir / config_path if not Path(config_path).is_absolute() else Path(config_path)

        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_yaml = yaml.safe_load(f)

        self.study_name = self.raw_yaml.get("study_name", "study")
        self.study_dir = base_dir / "output" / self.study_name
        self.input_parquet = self.study_dir / "02_triaged.parquet"
        self.checkpoint_file = self.study_dir / "03_extraction_checkpoint.jsonl"
        self.output_json = self.study_dir / "03_evidence.json"
        self.output_parquet = self.study_dir / "03_evidence.parquet"

        self.extraction_cfg = self.raw_yaml.get("extraction", {})
        self.pdf_source_str = self.extraction_cfg.get("pdf_source", "")
        self.pdf_source = Path(self.pdf_source_str) if self.pdf_source_str else None

        self.schema_list = self.extraction_cfg.get("schema", [])
        self.ft_manager = FullTextManager(base_dir / "cache")

        prompt_path = base_dir / "prompts" / "agents" / "extraction_agent.md"
        with open(prompt_path, "r", encoding="utf-8") as f:
            self.system_prompt = f.read()

    def reconcile_and_link_pdfs(self) -> Dict[str, Dict[str, Any]]:
        """
        Reconciles triaged papers against external PDF source (e.g. Zotero export directory).
        Uses 3-tiered linking:
          1. Exact match via Zotero collection .bib file
          2. First-page DOI regex matching on PDFs
          3. Normalized title fuzzy containment
        Prints transparent statistics and writes output/<study>/03_pdf_manifest.md.
        """
        if not self.input_parquet.exists():
            raise FileNotFoundError(f"Input triaged papers not found: {self.input_parquet}")

        df_triaged = duckdb.read_parquet(str(self.input_parquet)).df()
        total_study_papers = len(df_triaged)

        if not self.pdf_source or not self.pdf_source.exists():
            logger.warning(f"PDF source path does not exist or not configured: {self.pdf_source}")
            return {}

        logger.info(f"Scanning PDF source: {self.pdf_source}")

        # Build lookup tables from triaged papers
        doi_to_pid = {}
        title_to_pid = {}
        for _, row in df_triaged.iterrows():
            pid = str(row["id"])
            if pd.notna(row["doi"]) and row["doi"]:
                clean_doi = str(row["doi"]).lower().strip().replace("https://doi.org/", "").replace("http://dx.doi.org/", "").strip("/")
                doi_to_pid[clean_doi] = pid
            if pd.notna(row["title"]) and row["title"]:
                clean_t = re.sub(r"[^a-zA-Z0-9]", "", str(row["title"]).lower())
                if len(clean_t) > 15:
                    title_to_pid[clean_t] = pid

        paper_to_pdf: Dict[str, Dict[str, Any]] = {}
        matched_method_counts = {"zotero_bib": 0, "page_doi": 0, "title_match": 0}

        # 1. Tier 1: Check for .bib files in pdf_source (e.g. Zotero export)
        bib_files = list(self.pdf_source.glob("*.bib"))
        if bib_files:
            logger.info(f"Found Zotero export bibliography: {bib_files[0].name}")
            content = bib_files[0].read_text(encoding="utf-8")
            entries = re.findall(r"@article\{([^,]+),(.*?)\n\}", content, re.DOTALL)
            for cite_key, body in entries:
                doi_match = re.search(r"doi\s*=\s*\{([^}]+)\}", body)
                file_match = re.search(r"file\s*=\s*\{([^}]+)\}", body)
                title_match = re.search(r"title\s*=\s*\{([^}]+)\}", body)

                target_pdf: Optional[Path] = None
                if file_match:
                    path_match = re.search(r"(files/[^:]+\.pdf)", file_match.group(1))
                    if path_match:
                        cand = self.pdf_source / path_match.group(1)
                        if cand.exists():
                            target_pdf = cand

                matched_pid = None
                if doi_match:
                    clean_doi = doi_match.group(1).lower().strip().replace("https://doi.org/", "").strip("/")
                    if clean_doi in doi_to_pid:
                        matched_pid = doi_to_pid[clean_doi]

                if not matched_pid and title_match:
                    clean_t = re.sub(r"[^a-zA-Z0-9]", "", title_match.group(1).lower())
                    for ct, pid in title_to_pid.items():
                        if ct in clean_t or clean_t in ct:
                            matched_pid = pid
                            break

                if matched_pid and target_pdf and matched_pid not in paper_to_pdf:
                    paper_to_pdf[matched_pid] = {
                        "pdf_path": target_pdf,
                        "method": "zotero_bib",
                    }
                    matched_method_counts["zotero_bib"] += 1

        # 2. Tier 2 & 3: Check remaining PDF files in directory
        all_pdfs = list(self.pdf_source.rglob("*.pdf"))
        already_used_paths = {v["pdf_path"] for v in paper_to_pdf.values()}
        unused_pdfs = [p for p in all_pdfs if p not in already_used_paths]

        doi_regex = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+")
        for pdf_path in unused_pdfs:
            try:
                reader = PdfReader(str(pdf_path))
                first_text = ""
                for p in reader.pages[:2]:
                    first_text += p.extract_text() or ""

                found_dois = doi_regex.findall(first_text)
                matched_pid = None
                for fd in found_dois:
                    clean_fd = fd.strip().lower().rstrip(".,;)")
                    if clean_fd in doi_to_pid and doi_to_pid[clean_fd] not in paper_to_pdf:
                        matched_pid = doi_to_pid[clean_fd]
                        matched_method_counts["page_doi"] += 1
                        paper_to_pdf[matched_pid] = {"pdf_path": pdf_path, "method": "page_doi"}
                        break

                if not matched_pid:
                    norm_text = re.sub(r"[^a-zA-Z0-9]", "", first_text.lower())
                    for clean_t, pid in title_to_pid.items():
                        if clean_t in norm_text and pid not in paper_to_pdf:
                            paper_to_pdf[pid] = {"pdf_path": pdf_path, "method": "title_match"}
                            matched_method_counts["title_match"] += 1
                            break
            except Exception:
                continue

        # Compile Manifest & Print Console Report
        matched_count = len(paper_to_pdf)
        missing_count = total_study_papers - matched_count
        match_rate = (matched_count / total_study_papers) * 100 if total_study_papers > 0 else 0

        manifest_lines = []
        manifest_lines.append(f"# Phase 3 PDF Reconciliation Manifest: `{self.study_name}`")
        manifest_lines.append(f"**PDF Source Directory:** `{self.pdf_source}`  ")
        manifest_lines.append(f"**Total Study Papers (02_triaged):** **{total_study_papers}**  ")
        manifest_lines.append(f"**Total PDF Files Found in Source:** **{len(all_pdfs)}**  ")
        manifest_lines.append(f"**Successfully Matched Full-Text PDFs:** **{matched_count} ({match_rate:.1f}%)**  ")
        manifest_lines.append(f"**Missing Full-Text PDFs:** **{missing_count}**\n")

        manifest_lines.append("## Linking Method Breakdown")
        manifest_lines.append(f"- Matched via Zotero Collection .bib: **{matched_method_counts['zotero_bib']}**")
        manifest_lines.append(f"- Matched via First-Page DOI extraction: **{matched_method_counts['page_doi']}**")
        manifest_lines.append(f"- Matched via Title keyword scan: **{matched_method_counts['title_match']}**\n")

        manifest_lines.append("## Missing Papers (Abstract-only fallback)")
        manifest_lines.append("| Year | Venue | Title | DOI |")
        manifest_lines.append("|---|---|---|---|")
        missing_rows = df_triaged[~df_triaged["id"].isin(paper_to_pdf.keys())]
        for _, r in missing_rows.iterrows():
            yr = r.get("publication_year", "")
            ven = str(r.get("venue", ""))[:28]
            ttl = str(r.get("title", ""))[:55]
            doi = str(r.get("doi", ""))
            doi_link = f"[{doi}](https://doi.org/{doi})" if doi and doi != "None" else "N/A"
            manifest_lines.append(f"| {yr} | {ven} | {ttl}... | {doi_link} |")

        manifest_content = "\n".join(manifest_lines)
        manifest_path = self.study_dir / "03_pdf_manifest.md"
        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write(manifest_content)

        print("\n" + "=" * 80)
        print("PHASE 3: PDF RECONCILIATION & LINKING REPORT")
        print("=" * 80)
        print(f"Total Triaged Study Papers: {total_study_papers}")
        print(f"Total PDFs in Source:       {len(all_pdfs)}")
        print(f"Successfully Matched:       {matched_count} / {total_study_papers} ({match_rate:.1f}%)")
        print(f"  -> via Zotero .bib:       {matched_method_counts['zotero_bib']}")
        print(f"  -> via Page DOI:          {matched_method_counts['page_doi']}")
        print(f"  -> via Title match:       {matched_method_counts['title_match']}")
        print(f"Missing Full-Text PDFs:     {missing_count}")
        print(f"Manifest written to:        {manifest_path.name}")
        print("=" * 80 + "\n")

        return paper_to_pdf

    def load_checkpoint(self) -> Dict[str, Dict[str, Any]]:
        """Loads previously extracted evidence from checkpoint."""
        checkpoint = {}
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        try:
                            obj = json.loads(line)
                            checkpoint[obj["paper_id"]] = obj
                        except Exception:
                            pass
        return checkpoint

    def save_checkpoint_item(self, evidence: Dict[str, Any]):
        """Appends a newly extracted paper evidence to checkpoint."""
        with open(self.checkpoint_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(evidence, ensure_ascii=False) + "\n")

    def prepare_fulltexts(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Loads triaged papers, links them to PDFs on disk from pdf_source, extracts text,
        and returns list of papers ready for agent audit.
        """
        logger.info(f"Loading triaged papers from {self.input_parquet.name}...")
        df_triaged = duckdb.read_parquet(str(self.input_parquet)).df()

        # Reconcile PDFs from configured source
        pdf_map = self.reconcile_and_link_pdfs()

        if limit:
            df_triaged = df_triaged.head(limit)
            logger.info(f"Limit applied: processing first {len(df_triaged)} papers.")

        prepared_papers = []
        pdf_success = 0

        for _, row in tqdm(df_triaged.iterrows(), total=len(df_triaged), desc="Extracting Texts"):
            paper_id = str(row["id"])
            title = str(row["title"])
            doi = str(row["doi"]) if pd.notna(row["doi"]) else None
            abstract = str(row["abstract"]) if pd.notna(row["abstract"]) else ""

            text = ""
            source = "abstract_only"

            if paper_id in pdf_map:
                pdf_path = pdf_map[paper_id]["pdf_path"]
                text = self.ft_manager.extract_clean_text(pdf_path)
                if len(text) > 500:
                    source = "pdf"
                    pdf_success += 1

            if not text:
                text = f"Title: {title}\nAbstract: {abstract}"

            prepared_papers.append({
                "paper_id": paper_id,
                "title": title,
                "doi": doi,
                "has_fulltext": source == "pdf",
                "fulltext_source": source,
                "text": text,
            })

        manifest_path = self.study_dir / "03_prepared_papers.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(prepared_papers, f, ensure_ascii=False, indent=2)

        logger.info(
            f"Full-text preparation complete: {pdf_success}/{len(df_triaged)} "
            f"({(pdf_success/max(1, len(df_triaged)))*100:.1f}%) papers have parsed full-text. "
            f"Saved to {manifest_path.name}."
        )
        return prepared_papers

    def export_results(self, all_evidences: List[Dict[str, Any]]):
        """Exports evidence records to JSON and Parquet."""
        self.study_dir.mkdir(parents=True, exist_ok=True)

        with open(self.output_json, "w", encoding="utf-8") as f:
            json.dump(all_evidences, f, ensure_ascii=False, indent=2)

        flattened = []
        for ev in all_evidences:
            row = {
                "paper_id": ev["paper_id"],
                "title": ev.get("title", ""),
                "doi": ev.get("doi"),
                "has_fulltext": ev.get("has_fulltext", False),
                "fulltext_source": ev.get("fulltext_source", "none"),
            }
            for qid, ans in ev.get("answers", {}).items():
                row[f"{qid}_value"] = str(ans.get("value", ""))
                row[f"{qid}_quote"] = ans.get("verbatim_quote", "")
                row[f"{qid}_section"] = ans.get("section", "")
                row[f"{qid}_confidence"] = ans.get("confidence", "")
            flattened.append(row)

        df = pd.DataFrame(flattened)
        con = duckdb.connect()
        con.register("df_ev", df)
        con.execute(f"COPY df_ev TO '{self.output_parquet.as_posix()}' (FORMAT PARQUET)")
        con.close()

        logger.info(f"Saved {len(all_evidences)} evidence records to {self.output_json.name} and {self.output_parquet.name}")

    def export_zotero_manifest(
        self,
        missing_only: bool = True,
        output_bib: Optional[Path] = None,
        output_csv: Optional[Path] = None,
    ) -> Dict[str, Path]:
        """
        Exports triaged papers (or only papers missing full-text) to BibTeX and CSV
        for 1-click import into Zotero / Mendeley or batch downloading via institutional access.
        """
        df_triaged = duckdb.read_parquet(str(self.input_parquet)).df()

        missing_ids = set()
        prepared_manifest = self.study_dir / "03_prepared_papers.json"
        if missing_only and prepared_manifest.exists():
            with open(prepared_manifest, "r", encoding="utf-8") as f:
                prepared = json.load(f)
            missing_ids = {p["paper_id"] for p in prepared if not p.get("has_fulltext", False)}

        target_papers = []
        for _, row in df_triaged.iterrows():
            pid = str(row["id"])
            if missing_only and missing_ids and pid not in missing_ids:
                continue
            target_papers.append(row.to_dict())

        suffix = "missing" if missing_only else "all"
        if not output_bib:
            output_bib = self.study_dir / f"{suffix}_papers_for_zotero.bib"
        if not output_csv:
            output_csv = self.study_dir / f"{suffix}_papers.csv"

        # Write BibTeX
        with open(output_bib, "w", encoding="utf-8") as f:
            for p in target_papers:
                cid = str(p["id"]).split("/")[-1]
                raw_authors = str(p.get("authors") or "")
                author_list = [a.strip() for a in raw_authors.split(",") if a.strip()]
                bib_authors = " and ".join(author_list) if author_list else "Unknown"

                title = str(p.get("title") or "").replace("{", "").replace("}", "")
                venue = str(p.get("venue") or "").replace("{", "").replace("}", "")
                year = str(p.get("publication_year") or "")
                doi = str(p.get("doi") or "") if pd.notna(p.get("doi")) else ""

                f.write(f"@article{{{cid},\n")
                f.write(f"  title = {{{{{title}}}}},\n")
                f.write(f"  author = {{{bib_authors}}},\n")
                f.write(f"  journal = {{{{{venue}}}}},\n")
                f.write(f"  year = {{{year}}},\n")
                if doi:
                    clean_doi = doi.replace("https://doi.org/", "").replace("http://dx.doi.org/", "")
                    f.write(f"  doi = {{{clean_doi}}},\n")
                    f.write(f"  url = {{https://doi.org/{clean_doi}}},\n")
                f.write("}\n\n")

        # Write CSV
        import csv
        with open(output_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title", "authors", "year", "venue", "doi", "url", "cited_by_count", "open_access_pdf"])
            for p in target_papers:
                doi = str(p.get("doi") or "") if pd.notna(p.get("doi")) else ""
                clean_doi = doi.replace("https://doi.org/", "").replace("http://dx.doi.org/", "")
                url = f"https://doi.org/{clean_doi}" if clean_doi else ""
                writer.writerow([
                    p.get("id"),
                    p.get("title"),
                    p.get("authors"),
                    p.get("publication_year"),
                    p.get("venue"),
                    clean_doi,
                    url,
                    p.get("cited_by_count"),
                    p.get("open_access_pdf") if pd.notna(p.get("open_access_pdf")) else "",
                ])

        logger.info(f"Exported {len(target_papers)} papers to {output_bib.name} and {output_csv.name}")
        return {"bib": output_bib, "csv": output_csv}

    def import_external_pdfs(self, source_dir: Path) -> int:
        """
        Scans an external directory (e.g. Zotero storage or user folder) for PDF files,
        matches them against triaged papers via DOI or normalized title, and imports
        them into cache/fulltexts/ as <safe_paper_id>.pdf.
        """
        import shutil

        if not source_dir.exists():
            logger.error(f"Source directory does not exist: {source_dir}")
            return 0

        df_triaged = duckdb.read_parquet(str(self.input_parquet)).df()

        # Build lookup tables
        doi_map = {}
        title_map = {}
        for _, row in df_triaged.iterrows():
            pid = str(row["id"])
            if pd.notna(row["doi"]) and row["doi"]:
                clean_doi = str(row["doi"]).strip().lower().replace("https://doi.org/", "").replace("http://dx.doi.org/", "")
                doi_map[clean_doi] = pid
            if pd.notna(row["title"]) and row["title"]:
                clean_t = re.sub(r"[^a-zA-Z0-9]", "", str(row["title"]).lower())
                if len(clean_t) > 15:
                    title_map[clean_t] = pid

        pdf_files = list(source_dir.rglob("*.pdf"))
        logger.info(f"Found {len(pdf_files)} PDF files in {source_dir}. Matching against {len(df_triaged)} study papers...")

        doi_regex = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+")
        imported_count = 0

        for pdf_file in tqdm(pdf_files, desc="Importing PDFs"):
            matched_pid = None
            try:
                reader = PdfReader(str(pdf_file))
                first_pages_text = ""
                for page in reader.pages[:2]:
                    first_pages_text += page.extract_text() or ""

                found_dois = doi_regex.findall(first_pages_text)
                for fd in found_dois:
                    clean_fd = fd.strip().lower().rstrip(".,;)")
                    if clean_fd in doi_map:
                        matched_pid = doi_map[clean_fd]
                        break

                if not matched_pid:
                    norm_text = re.sub(r"[^a-zA-Z0-9]", "", first_pages_text.lower())
                    for clean_t, pid in title_map.items():
                        if clean_t in norm_text:
                            matched_pid = pid
                            break
            except Exception:
                continue

            if matched_pid:
                safe_name = self.ft_manager.get_safe_filename(matched_pid)
                target_pdf = self.ft_manager.cache_dir / safe_name
                shutil.copy2(pdf_file, target_pdf)
                imported_count += 1
                logger.info(f"Matched & imported: '{pdf_file.name}' -> {safe_name}")

        logger.info(f"Import complete: {imported_count} PDFs matched and imported to {self.ft_manager.cache_dir}.")

        # Re-run prepare_fulltexts to refresh 03_prepared_papers.json
        prepared = self.prepare_fulltexts()
        manifest_path = self.study_dir / "03_prepared_papers.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(prepared, f, ensure_ascii=False, indent=2)
        logger.info(f"Refreshed {len(prepared)} papers in {manifest_path.name}")
        return imported_count


# ============================================================================
# 4. CLI Entrypoint
# ============================================================================

def main():
    import os

    parser = argparse.ArgumentParser(description="Phase 3: Extraction Module for lit-pipeline")
    parser.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")
    parser.add_argument("--limit", "-l", type=int, default=None, help="Limit number of papers (for testing)")
    parser.add_argument("--check", action="store_true", help="Only check and reconcile PDFs without extracting texts")
    parser.add_argument("--prepare-only", action="store_true", help="Extract and stage texts without running agent audit")

    args = parser.parse_args()
    base_dir = Path.cwd()

    runner = ExtractionRunner(args.config, base_dir)

    if args.check:
        runner.reconcile_and_link_pdfs()
        return

    prepared = runner.prepare_fulltexts(limit=args.limit)

    manifest_path = runner.study_dir / "03_prepared_papers.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(prepared, f, ensure_ascii=False, indent=2)
    logger.info(f"Staged {len(prepared)} full texts in {manifest_path.name}")

    if args.prepare_only:
        print(f"Preparation complete: {len(prepared)} papers staged for extraction.")
        return


if __name__ == "__main__":
    main()
