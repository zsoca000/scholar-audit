"""
Triage Module for lit-pipeline.

Phase 2 of Research Intelligence Pipeline:
- Loads discovered papers from output/<study>/01_papers.parquet.
- Evaluates abstracts in micro-batches using Gemini API (e.g. gemini-3.5-flash).
- Enforces strict criteria and zero-hallucination rules from prompts/agents/triage_agent.md.
- Micro-batching and resilient checkpointing (resumes automatically if interrupted).
- Rate limit awareness (polite pace for free tier).
- Exports screened results to output/<study>/02_triaged.parquet via DuckDB.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import duckdb
import httpx
import pandas as pd
import yaml
from pydantic import BaseModel, Field
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("triage")


# ============================================================================
# 1. Config & Data Models
# ============================================================================

class TriageEvaluation(BaseModel):
    paper_id: str
    is_relevant: bool
    confidence: float
    reason: str
    matched_topics: List[str] = Field(default_factory=list)


def load_env_api_key(base_dir: Path) -> Optional[str]:
    """Loads GEMINI_API_KEY from environment or .env file."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if api_key:
        return api_key

    env_path = base_dir / ".env"
    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("GEMINI_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


# ============================================================================
# 2. Gemini API Client for Batch Evaluation
# ============================================================================

class GeminiTriageClient:
    """Client for evaluating batches of abstracts via Gemini REST API."""

    PRIMARY_MODEL = "gemini-3.5-flash"
    FALLBACK_MODELS = ["gemini-3.8-flash", "gemini-3.1-flash-lite"]
    BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"

    def __init__(self, api_key: str, model_name: Optional[str] = None):
        self.api_key = api_key
        self.model_name = model_name or self.PRIMARY_MODEL
        self.client = httpx.Client(timeout=45.0)

    def close(self):
        self.client.close()

    def evaluate_batch(
        self,
        system_instructions: str,
        criteria: str,
        papers_batch: List[Dict[str, Any]],
        max_retries: int = 4,
    ) -> List[TriageEvaluation]:
        """
        Sends a batch of papers to Gemini for structured JSON triage.
        """
        prompt_content = {
            "evaluation_criteria": criteria,
            "instructions": (
                "Evaluate every paper in the 'papers_to_evaluate' list strictly against the evaluation_criteria. "
                "Return a JSON array containing an evaluation object for EVERY single paper in the exact order received. "
                "Be factual, concise, and apply high recall: if borderline or likely relevant, set is_relevant=true."
            ),
            "papers_to_evaluate": papers_batch,
        }

        user_message = json.dumps(prompt_content, ensure_ascii=False)

        headers = {
            "x-goog-api-key": self.api_key,
            "Content-Type": "application/json",
        }

        payload = {
            "system_instruction": {
                "parts": [{"text": system_instructions}]
            },
            "contents": [
                {"role": "user", "parts": [{"text": user_message}]}
            ],
            "generationConfig": {
                "response_mime_type": "application/json",
                "temperature": 0.1,
            },
        }

        models_to_try = [self.model_name] + [m for m in self.FALLBACK_MODELS if m != self.model_name]

        for model in models_to_try:
            url = f"{self.BASE_URL}/{model}:generateContent"

            for attempt in range(max_retries):
                try:
                    resp = self.client.post(url, headers=headers, json=payload)

                    if resp.status_code == 200:
                        data = resp.json()
                        text = data["candidates"][0]["content"]["parts"][0]["text"]
                        parsed = json.loads(text)
                        if isinstance(parsed, dict) and "evaluations" in parsed:
                            parsed = parsed["evaluations"]
                        if not isinstance(parsed, list):
                            parsed = [parsed]

                        results: List[TriageEvaluation] = []
                        for item in parsed:
                            results.append(TriageEvaluation(**item))
                        return results

                    elif resp.status_code in (429, 503):
                        # Rate limit or temporary model unavailability - back off
                        wait_time = 4.0 * (attempt + 1)
                        logger.warning(f"HTTP {resp.status_code} on {model}. Retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        logger.warning(f"Gemini API error ({model}): HTTP {resp.status_code} - {resp.text[:150]}")
                        break

                except Exception as e:
                    logger.warning(f"Request exception on {model} (attempt {attempt+1}): {e}")
                    time.sleep(3.0)

        # Fallback if API completely fails for this batch
        logger.error(f"Failed to evaluate batch of {len(papers_batch)} papers after trying all models.")
        fallback_results = []
        for p in papers_batch:
            fallback_results.append(
                TriageEvaluation(
                    paper_id=p["paper_id"],
                    is_relevant=True,  # Default to retain on failure (high recall)
                    confidence=0.5,
                    reason="Evaluation failed due to API unavailability; retained by high-recall fallback.",
                    matched_topics=["fallback"],
                )
            )
        return fallback_results


# ============================================================================
# 3. Triage Runner & Checkpointing
# ============================================================================

class TriageRunner:
    """Manages reading papers, batching, checkpointing, and saving results."""

    def __init__(self, config_path: str | Path, base_dir: Path):
        self.base_dir = base_dir
        self.config_path = base_dir / config_path if not Path(config_path).is_absolute() else Path(config_path)

        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_yaml = yaml.safe_load(f)

        self.study_name = self.raw_yaml.get("study_name", "study")
        self.triage_cfg = self.raw_yaml.get("triage", {})
        self.criteria = self.triage_cfg.get("criteria", "Keep relevant scientific papers.")
        self.relevance_threshold = float(self.triage_cfg.get("relevance_threshold", 0.7))
        self.batch_size = int(self.triage_cfg.get("batch_size", 10))

        # Output paths
        self.study_output_dir = base_dir / "output" / self.study_name
        discovered_file = self.study_output_dir / "01_discovered.parquet"
        fallback_file = self.study_output_dir / "01_papers.parquet"
        self.input_parquet = discovered_file if discovered_file.exists() else fallback_file

        self.checkpoint_file = self.study_output_dir / "02_triage_checkpoint.jsonl"
        self.output_parquet = self.study_output_dir / "02_triaged.parquet"
        self.output_bib = self.study_output_dir / "02_triaged.bib"

        # Prompts
        self.prompt_path = base_dir / "prompts" / "agents" / "triage_agent.md"
        with open(self.prompt_path, "r", encoding="utf-8") as f:
            self.system_prompt = f.read()

        # Mode configuration (api vs local)
        self.mode = self.raw_yaml.get("triage", {}).get("mode", "api")

    def load_checkpoint(self) -> Dict[str, TriageEvaluation]:
        """Loads previously saved evaluations to resume without re-running."""
        evals = {}
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            data = json.loads(line)
                            eval_obj = TriageEvaluation(**data)
                            evals[eval_obj.paper_id] = eval_obj
                        except Exception:
                            pass
        return evals

    def save_checkpoint_batch(self, evals: List[TriageEvaluation]):
        """Appends new evaluations to the checkpoint JSONL file."""
        with open(self.checkpoint_file, "a", encoding="utf-8") as f:
            for ev in evals:
                f.write(json.dumps(ev.model_dump(), ensure_ascii=False) + "\n")

    def run(self, limit: Optional[int] = None) -> pd.DataFrame:
        logger.info(f"=== Starting Triage Phase for Study: '{self.study_name}' ===")
        if not self.input_parquet.exists():
            raise FileNotFoundError(f"Input papers file not found: {self.input_parquet}. Run discovery first!")

        # Load papers from Parquet
        con = duckdb.connect()
        df_all = con.execute(f"SELECT * FROM read_parquet('{self.input_parquet.as_posix()}')").fetchdf()
        con.close()

        total_papers = len(df_all)
        logger.info(f"Loaded {total_papers} papers from {self.input_parquet.name}")

        if limit:
            df_all = df_all.head(limit)
            logger.info(f"Limit applied: processing first {len(df_all)} papers.")

        # Load checkpoint
        checkpoint = self.load_checkpoint()
        logger.info(f"Found {len(checkpoint)} already evaluated papers in checkpoint.")

        # Prepare papers to screen
        papers_to_screen = []
        for _, row in df_all.iterrows():
            paper_id = str(row["id"])
            if paper_id in checkpoint:
                continue

            abstract = str(row["abstract"]) if pd.notna(row["abstract"]) else ""
            title = str(row["title"]) if pd.notna(row["title"]) else ""
            is_seed = bool(row["is_seed"]) if "is_seed" in row and pd.notna(row["is_seed"]) else False

            # If paper has no abstract
            if len(abstract.strip()) < 30:
                if is_seed:
                    # Always retain golden seeds
                    eval_obj = TriageEvaluation(
                        paper_id=paper_id,
                        is_relevant=True,
                        confidence=1.0,
                        reason="Golden seed retained automatically without abstract.",
                        matched_topics=["seed"],
                    )
                else:
                    eval_obj = TriageEvaluation(
                        paper_id=paper_id,
                        is_relevant=False,
                        confidence=0.9,
                        reason="No abstract available for non-seed paper.",
                        matched_topics=[],
                    )
                checkpoint[paper_id] = eval_obj
                self.save_checkpoint_batch([eval_obj])
                continue

            papers_to_screen.append({
                "paper_id": paper_id,
                "title": title,
                "abstract": abstract,
            })

        logger.info(f"Remaining papers to evaluate with LLM: {len(papers_to_screen)}")

        # Process in micro-batches
        batch_size = self.batch_size
        batches = [papers_to_screen[i : i + batch_size] for i in range(0, len(papers_to_screen), batch_size)]

        pbar = tqdm(total=len(papers_to_screen), desc="Triage Screening", unit="paper")
        for batch in batches:
            eval_results = self.client.evaluate_batch(
                system_instructions=self.system_prompt,
                criteria=self.criteria,
                papers_batch=batch,
            )

            # Store in checkpoint
            for ev in eval_results:
                checkpoint[ev.paper_id] = ev
            self.save_checkpoint_batch(eval_results)
            pbar.update(len(batch))

            # Polite rate limiting for Free Tier (15 RPM -> ~4.0s pause between batches)
            time.sleep(3.8)

        pbar.close()
        self.client.close()

        # Combine evaluations with original metadata
        logger.info("Combining evaluation results with paper metadata...")
        eval_records = []
        for pid, ev in checkpoint.items():
            eval_records.append({
                "id": pid,
                "is_relevant": ev.is_relevant,
                "confidence": ev.confidence,
                "triage_reason": ev.reason,
                "matched_topics": ", ".join(ev.matched_topics),
            })
        df_evals = pd.DataFrame(eval_records)

        # Merge with df_all
        df_merged = df_all.merge(df_evals, on="id", how="inner")

        # Filter: retain papers that are relevant and meet the threshold (seeds always kept)
        condition = (df_merged["is_seed"] == True) | (
            (df_merged["is_relevant"] == True) & (df_merged["confidence"] >= self.relevance_threshold)
        )
        df_triaged = df_merged[condition].copy()

        # Sort by is_seed, confidence descending, cited_by_count descending
        df_triaged = df_triaged.sort_values(
            by=["is_seed", "confidence", "cited_by_count"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

        # Save to Parquet via DuckDB
        con = duckdb.connect()
        con.register("df_triaged", df_triaged)
        con.execute(f"COPY df_triaged TO '{self.output_parquet.as_posix()}' (FORMAT PARQUET)")
        con.close()

        # Automatically export BibTeX for 1-click Zotero import
        self._export_to_bib(df_triaged, self.output_bib)

        logger.info(f"=== Triage Completed ===")
        logger.info(f"Total screened: {len(df_merged)}")
        logger.info(f"Retained papers: {len(df_triaged)} ({(len(df_triaged)/max(1, len(df_merged)))*100:.1f}%)")
        logger.info(f"Saved Parquet: {self.output_parquet}")
        logger.info(f"Saved Zotero BibTeX: {self.output_bib}")

        return df_triaged

    def _export_to_bib(self, df_triaged: pd.DataFrame, bib_path: Path):
        """Exports triaged papers directly to BibTeX format for Zotero."""
        with open(bib_path, "w", encoding="utf-8") as f:
            for _, row in df_triaged.iterrows():
                cid = str(row["id"]).split("/")[-1]
                raw_authors = str(row.get("authors") or "")
                author_list = [a.strip() for a in raw_authors.split(",") if a.strip()]
                bib_authors = " and ".join(author_list) if author_list else "Unknown"

                title = str(row.get("title") or "").replace("{", "").replace("}", "")
                venue = str(row.get("venue") or "").replace("{", "").replace("}", "")
                year = str(row.get("publication_year") or "")
                doi = str(row.get("doi") or "") if pd.notna(row.get("doi")) else ""

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


# ============================================================================
# 4. CLI Entrypoint
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Phase 2: Triage Module for lit-pipeline")
    parser.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")
    parser.add_argument("--limit", "-l", type=int, default=None, help="Limit number of papers to screen (for quick testing)")
    parser.add_argument("--batch-size", "-b", type=int, default=None, help="Micro-batch size for LLM evaluation")
    parser.add_argument("--mode", "-m", choices=["api", "local"], default="api", help="Execution mode: api (Gemini/OpenAI) or local (Ollama/native)")

    args = parser.parse_args()
    base_dir = Path.cwd()

    runner = TriageRunner(args.config, base_dir)
    if args.batch_size:
        runner.batch_size = args.batch_size
    if args.mode:
        runner.mode = args.mode

    runner.run(limit=args.limit)


if __name__ == "__main__":
    main()
