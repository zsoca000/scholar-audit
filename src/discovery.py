"""
Discovery Module for lit-pipeline.

Phase 1 of Research Intelligence Pipeline:
- OpenAlex API querying with native Boolean expressions.
- Local disk caching with TTL (Time-To-Live) support.
- BibTeX seed integration (golden seeds exempt from age filtering).
- Multi-hop Citation Snowballing (forward citations & backward references).
- Continuous deduplication and Parquet export via DuckDB.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import bibtexparser
from bibtexparser.bparser import BibTexParser
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
logger = logging.getLogger("discovery")


# ============================================================================
# 1. Pydantic Models & Configurations
# ============================================================================

class SnowballConfig(BaseModel):
    depth: int = Field(default=1, description="Snowball depth: 0=no snowball, 1=1 hop, 2=2 hops, etc.")
    max_citations_per_seed: int = Field(default=10, description="Max citing (forward) works per seed paper")
    max_references_per_seed: int = Field(default=10, description="Max referenced (backward) works per seed paper")


class DiscoveryConfig(BaseModel):
    study_name: str = "study"
    queries: List[str] = Field(default_factory=list)
    seed_bib: Optional[str] = None
    seed_dois: List[str] = Field(default_factory=list)
    min_year: int = 2018
    max_results_per_query: int = 50
    cache_ttl_hours: int = 24
    snowball: SnowballConfig = Field(default_factory=SnowballConfig)


class PaperMetadata(BaseModel):
    id: str  # OpenAlex ID (e.g. "https://openalex.org/W2741809807" or custom fallback)
    doi: Optional[str] = None  # Normalized clean DOI (e.g. "10.1016/j.ifacol.2018.07.326")
    title: str
    authors: List[str] = Field(default_factory=list)
    publication_year: Optional[int] = None
    venue: Optional[str] = None  # Journal or Conference name
    abstract: str = ""
    cited_by_count: int = 0
    referenced_works_count: int = 0
    discovery_source: str = "unknown"
    open_access_pdf: Optional[str] = None
    referenced_works: List[str] = Field(default_factory=list)
    is_seed: bool = False

    def normalized_doi(self) -> Optional[str]:
        if not self.doi:
            return None
        doi = self.doi.lower().strip()
        doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)
        return doi.strip("/")


# ============================================================================
# 2. Local Disk Cache with TTL
# ============================================================================

class OpenAlexCache:
    """Manages raw OpenAlex API responses on local disk with TTL."""

    def __init__(self, cache_dir: Path, ttl_hours: int = 24):
        self.cache_dir = cache_dir
        self.ttl_seconds = ttl_hours * 3600
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _hash_key(self, prefix: str, key: str) -> Path:
        safe_name = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
        # Clean prefix for readability
        clean_prefix = re.sub(r"[^a-zA-Z0-9_-]", "_", prefix)[:30]
        return self.cache_dir / f"{clean_prefix}_{safe_name}.json"

    def get(self, prefix: str, key: str) -> Optional[Dict[str, Any]]:
        path = self._hash_key(prefix, key)
        if not path.exists():
            return None
        try:
            mtime = os.path.getmtime(path)
            age = time.time() - mtime
            if age > self.ttl_seconds:
                logger.debug(f"Cache expired for {prefix}:{key[:40]} (age: {age/3600:.1f}h)")
                return None
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error reading cache file {path}: {e}")
            return None

    def set(self, prefix: str, key: str, data: Dict[str, Any]) -> None:
        path = self._hash_key(prefix, key)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Error writing cache file {path}: {e}")


# ============================================================================
# 3. OpenAlex API Client
# ============================================================================

def reconstruct_abstract(inverted_index: Optional[Dict[str, List[int]]]) -> str:
    """Reconstructs continuous text from OpenAlex inverted index."""
    if not inverted_index:
        return ""
    word_pos: List[tuple[int, str]] = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_pos.append((pos, word))
    word_pos.sort(key=lambda x: x[0])
    return " ".join(w for _, w in word_pos)


def parse_openalex_work(work: Dict[str, Any], source: str = "openalex", is_seed: bool = False) -> PaperMetadata:
    """Parses a single OpenAlex Work JSON into PaperMetadata."""
    # Extract authors
    authors: List[str] = []
    for authorship in work.get("authorships", []):
        author_name = authorship.get("author", {}).get("display_name")
        if author_name:
            authors.append(author_name)

    # Extract venue
    venue = None
    primary_loc = work.get("primary_location") or {}
    source_obj = primary_loc.get("source") or {}
    if source_obj.get("display_name"):
        venue = source_obj["display_name"]

    # Extract PDF URL if available
    pdf_url = None
    if primary_loc.get("pdf_url"):
        pdf_url = primary_loc["pdf_url"]
    elif work.get("open_access", {}).get("oa_url"):
        pdf_url = work["open_access"]["oa_url"]

    # Normalize DOI
    raw_doi = work.get("doi")
    clean_doi = None
    if raw_doi:
        clean_doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", raw_doi.strip()).strip("/")

    # Reconstruct abstract
    abstract = reconstruct_abstract(work.get("abstract_inverted_index"))

    return PaperMetadata(
        id=work.get("id", ""),
        doi=clean_doi,
        title=work.get("title") or "Untitled",
        authors=authors,
        publication_year=work.get("publication_year"),
        venue=venue,
        abstract=abstract,
        cited_by_count=work.get("cited_by_count", 0),
        referenced_works_count=len(work.get("referenced_works", [])),
        discovery_source=source,
        open_access_pdf=pdf_url,
        referenced_works=work.get("referenced_works", []),
        is_seed=is_seed,
    )


class OpenAlexClient:
    """HTTP client for OpenAlex API with disk caching and rate limit politeness."""

    BASE_URL = "https://api.openalex.org"

    def __init__(self, cache: OpenAlexCache, mailto: str = "lit-pipeline@research.local"):
        self.cache = cache
        self.headers = {
            "User-Agent": f"lit-pipeline/0.1.0 (mailto:{mailto})",
            "Accept": "application/json",
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)

    def close(self):
        self.client.close()

    def get_work_by_doi(self, doi: str, is_seed: bool = True) -> Optional[PaperMetadata]:
        clean_doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi.strip()).strip("/")
        cache_key = f"doi:{clean_doi}"
        cached = self.cache.get("work", cache_key)
        if cached:
            return parse_openalex_work(cached, source="doi_seed", is_seed=is_seed)

        url = f"{self.BASE_URL}/works/https://doi.org/{clean_doi}"
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                self.cache.set("work", cache_key, data)
                return parse_openalex_work(data, source="doi_seed", is_seed=is_seed)
            elif resp.status_code == 404:
                logger.debug(f"DOI not found on OpenAlex: {clean_doi}")
                return None
            else:
                logger.warning(f"OpenAlex error for DOI {clean_doi}: HTTP {resp.status_code}")
                return None
        except Exception as e:
            logger.warning(f"Network error querying DOI {clean_doi}: {e}")
            return None

    def get_work_by_id(self, openalex_id: str, is_seed: bool = False, source: str = "openalex") -> Optional[PaperMetadata]:
        clean_id = openalex_id.replace("https://openalex.org/", "").strip()
        cache_key = f"id:{clean_id}"
        cached = self.cache.get("work", cache_key)
        if cached:
            return parse_openalex_work(cached, source=source, is_seed=is_seed)

        url = f"{self.BASE_URL}/works/{clean_id}"
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                self.cache.set("work", cache_key, data)
                return parse_openalex_work(data, source=source, is_seed=is_seed)
            return None
        except Exception as e:
            logger.warning(f"Network error querying work {clean_id}: {e}")
            return None

    def search_works(
        self,
        query: str,
        min_year: Optional[int] = None,
        max_results: int = 50,
        source: str = "query",
    ) -> List[PaperMetadata]:
        """Performs a Boolean search on OpenAlex works."""
        cache_key = f"{query}|min_year:{min_year}|max:{max_results}"
        cached = self.cache.get("search", cache_key)
        if cached and "results" in cached:
            logger.info(f"Loaded {len(cached['results'])} search results from cache for: {query[:60]}...")
            return [parse_openalex_work(w, source=source, is_seed=True) for w in cached["results"]]

        results: List[Dict[str, Any]] = []
        page = 1
        per_page = min(max_results, 50)

        params: Dict[str, Any] = {
            "search": query,
            "per-page": per_page,
            "page": page,
        }
        if min_year:
            params["filter"] = f"publication_year:>{min_year - 1}"

        pbar = tqdm(total=max_results, desc=f"Query: {query[:35]}...", leave=False)
        while len(results) < max_results:
            params["page"] = page
            try:
                resp = self.client.get(f"{self.BASE_URL}/works", params=params)
                if resp.status_code != 200:
                    logger.warning(f"OpenAlex search error: HTTP {resp.status_code}")
                    break
                data = resp.json()
                page_results = data.get("results", [])
                if not page_results:
                    break

                results.extend(page_results)
                pbar.update(len(page_results))

                # Check if there are more results
                meta = data.get("meta", {})
                if len(results) >= meta.get("count", 0):
                    break
                page += 1
            except Exception as e:
                logger.warning(f"Network error during search query '{query}': {e}")
                break

        pbar.close()
        results = results[:max_results]
        self.cache.set("search", cache_key, {"results": results})
        logger.info(f"Retrieved {len(results)} works for query: {query[:60]}...")
        return [parse_openalex_work(w, source=source, is_seed=True) for w in results]

    def get_citations(self, work_id: str, max_count: int = 10, min_year: Optional[int] = None) -> List[PaperMetadata]:
        """Forward citations: papers that cite this work, ordered by cited_by_count."""
        clean_id = work_id.replace("https://openalex.org/", "").strip()
        cache_key = f"citations:{clean_id}|max:{max_count}|min_year:{min_year}"
        cached = self.cache.get("snowball", cache_key)
        if cached and "results" in cached:
            return [parse_openalex_work(w, source="snowball_citation") for w in cached["results"]]

        filter_expr = f"cites:{clean_id}"
        if min_year:
            filter_expr += f",publication_year:>{min_year - 1}"

        params: Dict[str, Any] = {
            "filter": filter_expr,
            "sort": "cited_by_count:desc",
            "per-page": min(max_count, 50),
        }

        try:
            resp = self.client.get(f"{self.BASE_URL}/works", params=params)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("results", [])[:max_count]
                self.cache.set("snowball", cache_key, {"results": items})
                return [parse_openalex_work(w, source="snowball_citation") for w in items]
            return []
        except Exception as e:
            logger.warning(f"Failed to fetch citations for {clean_id}: {e}")
            return []

    def get_references_batch(self, work_ids: List[str], max_count: int = 10, min_year: Optional[int] = None) -> List[PaperMetadata]:
        """Backward citations: papers referenced by this work."""
        if not work_ids:
            return []

        clean_ids = [w.replace("https://openalex.org/", "").strip() for w in work_ids][:max_count * 2]
        # OpenAlex pipe filter allows up to 50 IDs: openalex:W1|W2|W3...
        filter_expr = "openalex:" + "|".join(clean_ids[:50])
        if min_year:
            filter_expr += f",publication_year:>{min_year - 1}"

        cache_key = f"refs:{hashlib.sha256(filter_expr.encode()).hexdigest()[:16]}"
        cached = self.cache.get("snowball", cache_key)
        if cached and "results" in cached:
            return [parse_openalex_work(w, source="snowball_reference") for w in cached["results"]]

        params = {
            "filter": filter_expr,
            "sort": "cited_by_count:desc",
            "per-page": min(max_count, 50),
        }
        try:
            resp = self.client.get(f"{self.BASE_URL}/works", params=params)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("results", [])[:max_count]
                self.cache.set("snowball", cache_key, {"results": items})
                return [parse_openalex_work(w, source="snowball_reference") for w in items]
            return []
        except Exception as e:
            logger.warning(f"Failed to fetch batch references: {e}")
            return []


# ============================================================================
# 4. BibTeX Ingestion
# ============================================================================

def load_bibtex_seeds(bib_path: Path, client: OpenAlexClient) -> List[PaperMetadata]:
    """
    Parses BibTeX file and resolves OpenAlex metadata.
    Exempt from min_year filtering (Golden Seeds).
    """
    if not bib_path.exists():
        logger.warning(f"BibTeX file does not exist: {bib_path}")
        return []

    logger.info(f"Loading Golden Seeds from BibTeX: {bib_path}")
    parser = BibTexParser(interpolate_strings=False)
    with open(bib_path, "r", encoding="utf-8") as f:
        bib_database = bibtexparser.load(f, parser=parser)

    entries = bib_database.entries
    logger.info(f"Found {len(entries)} entries in {bib_path.name}")

    results: List[PaperMetadata] = []
    for entry in tqdm(entries, desc="Resolving .bib entries", leave=False):
        doi = entry.get("doi") or entry.get("DOI")
        title = entry.get("title", "").replace("{", "").replace("}", "")

        paper: Optional[PaperMetadata] = None
        if doi:
            paper = client.get_work_by_doi(doi, is_seed=True)

        # Fallback to title search if DOI not resolved
        if not paper and title:
            # Clean title
            clean_title = re.sub(r"[^\w\s]", " ", title).strip()
            search_res = client.search_works(f'"{clean_title}"', max_results=1, source="bib_fallback")
            if search_res:
                paper = search_res[0]

        if paper:
            paper.discovery_source = f"seed_bib:{bib_path.name}"
            paper.is_seed = True
            results.append(paper)
        else:
            # Standalone fallback if OpenAlex doesn't know it
            year_val = None
            if "year" in entry and entry["year"].isdigit():
                year_val = int(entry["year"])
            authors = [a.strip() for a in entry.get("author", "").split(" and ") if a.strip()]
            results.append(
                PaperMetadata(
                    id=f"bib:{entry.get('ID', title[:20])}",
                    doi=doi,
                    title=title or "Untitled",
                    authors=authors,
                    publication_year=year_val,
                    venue=entry.get("journal") or entry.get("booktitle"),
                    abstract=entry.get("abstract", ""),
                    discovery_source=f"seed_bib:{bib_path.name}_offline",
                    is_seed=True,
                )
            )

    logger.info(f"Resolved {len(results)} seeds from BibTeX.")
    return results


# ============================================================================
# 5. Core Discovery Engine & Snowballing
# ============================================================================

class DiscoveryRunner:
    """Orchestrates query execution, BibTeX integration, snowballing, deduplication, and export."""

    def __init__(self, config: DiscoveryConfig, base_dir: Path):
        self.config = config
        self.base_dir = base_dir
        self.cache_dir = base_dir / "cache" / "openalex"
        self.output_dir = base_dir / "output" / config.study_name
        self.cache = OpenAlexCache(self.cache_dir, ttl_hours=config.cache_ttl_hours)
        self.client = OpenAlexClient(self.cache)

        self.pool: Dict[str, PaperMetadata] = {}  # Key: normalized_doi or OpenAlex ID
        self.visited_ids: Set[str] = set()

    def _normalize_key(self, paper: PaperMetadata) -> str:
        doi = paper.normalized_doi()
        if doi:
            return f"doi:{doi}"
        return f"id:{paper.id.replace('https://openalex.org/', '').strip()}"

    def _add_paper(self, paper: PaperMetadata) -> bool:
        """Adds a paper to pool if not present. Returns True if newly added."""
        key = self._normalize_key(paper)
        if key in self.pool:
            # Merge sources or keep seed status
            existing = self.pool[key]
            if paper.is_seed:
                existing.is_seed = True
            if paper.discovery_source not in existing.discovery_source:
                existing.discovery_source += f"; {paper.discovery_source}"
            if not existing.abstract and paper.abstract:
                existing.abstract = paper.abstract
            return False

        self.pool[key] = paper
        return True

    def run(self) -> pd.DataFrame:
        logger.info(f"=== Starting Discovery Phase for Study: '{self.config.study_name}' ===")

        # Step 1: Ingest BibTeX Golden Seeds (no min_year filter)
        if self.config.seed_bib:
            bib_path = self.base_dir / self.config.seed_bib
            seeds = load_bibtex_seeds(bib_path, self.client)
            for s in seeds:
                self._add_paper(s)

        # Step 2: Direct seed DOIs
        for doi in self.config.seed_dois:
            p = self.client.get_work_by_doi(doi, is_seed=True)
            if p:
                self._add_paper(p)

        # Step 3: Run Boolean Search Queries
        for i, q in enumerate(self.config.queries, 1):
            logger.info(f"Running Query [{i}/{len(self.config.queries)}]: {q.strip()[:70]}...")
            works = self.client.search_works(
                query=q.strip(),
                min_year=self.config.min_year,
                max_results=self.config.max_results_per_query,
                source=f"query_axis_{i}",
            )
            added_count = 0
            for w in works:
                if self._add_paper(w):
                    added_count += 1
            logger.info(f"Query {i} produced {len(works)} hits ({added_count} new unique papers added).")

        initial_seed_count = len(self.pool)
        logger.info(f"Initial seed pool contains {initial_seed_count} unique papers.")

        # Step 4: Multi-Hop Citation Snowballing
        snowball_depth = self.config.snowball.depth
        if snowball_depth > 0:
            logger.info(f"Initiating Citation Snowballing (depth={snowball_depth})...")

            current_frontier: List[PaperMetadata] = list(self.pool.values())

            for hop in range(1, snowball_depth + 1):
                logger.info(f"--- Snowball Hop {hop}/{snowball_depth}: Processing {len(current_frontier)} candidates ---")
                next_frontier: List[PaperMetadata] = []

                for paper in tqdm(current_frontier, desc=f"Snowball Hop {hop}"):
                    paper_key = self._normalize_key(paper)
                    if paper_key in self.visited_ids:
                        continue
                    self.visited_ids.add(paper_key)

                    # Only OpenAlex IDs can be queried for citations/references
                    if not paper.id.startswith("http") and not paper.id.startswith("W"):
                        continue

                    # Forward citations (who cited this paper)
                    if self.config.snowball.max_citations_per_seed > 0:
                        cit_papers = self.client.get_citations(
                            paper.id,
                            max_count=self.config.snowball.max_citations_per_seed,
                            min_year=self.config.min_year,
                        )
                        for cp in cit_papers:
                            if self._add_paper(cp):
                                next_frontier.append(cp)

                    # Backward references (works cited in this paper's bibliography)
                    if self.config.snowball.max_references_per_seed > 0 and paper.referenced_works:
                        ref_papers = self.client.get_references_batch(
                            paper.referenced_works,
                            max_count=self.config.snowball.max_references_per_seed,
                            min_year=self.config.min_year,
                        )
                        for rp in ref_papers:
                            if self._add_paper(rp):
                                next_frontier.append(rp)

                logger.info(f"Hop {hop} complete. Added {len(next_frontier)} new papers. Pool size: {len(self.pool)}")
                current_frontier = next_frontier
                if not current_frontier:
                    logger.info("Snowball frontier is empty; stopping early.")
                    break

        # Step 5: Export to Parquet via DuckDB
        df = self._export_to_parquet()
        self.client.close()
        return df

    def _export_to_parquet(self) -> pd.DataFrame:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = self.output_dir / "01_discovered.parquet"

        records = []
        for paper in self.pool.values():
            records.append({
                "id": paper.id,
                "doi": paper.normalized_doi(),
                "title": paper.title,
                "authors": ", ".join(paper.authors),
                "publication_year": paper.publication_year,
                "venue": paper.venue,
                "abstract": paper.abstract,
                "cited_by_count": paper.cited_by_count,
                "referenced_works_count": paper.referenced_works_count,
                "discovery_source": paper.discovery_source,
                "open_access_pdf": paper.open_access_pdf,
                "is_seed": paper.is_seed,
            })

        df = pd.DataFrame(records)

        # Sort: Seeds first, then by cited_by_count descending
        if not df.empty:
            df = df.sort_values(by=["is_seed", "cited_by_count"], ascending=[False, False]).reset_index(drop=True)

        # Save to Parquet using DuckDB
        con = duckdb.connect()
        con.register("df_papers", df)
        con.execute(f"COPY df_papers TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")
        con.close()

        logger.info(f"Exported {len(df)} deduplicated papers to {parquet_path}")
        return df


# ============================================================================
# 6. CLI Entrypoint
# ============================================================================

def run_discovery(config_path: str | Path, base_dir: Optional[Path] = None) -> pd.DataFrame:
    """Programmatic entrypoint using a YAML config file."""
    if base_dir is None:
        base_dir = Path.cwd()
    cfg_file = Path(config_path)
    if not cfg_file.is_absolute():
        cfg_file = base_dir / cfg_file

    with open(cfg_file, "r", encoding="utf-8") as f:
        raw_yaml = yaml.safe_load(f)

    disc_data = raw_yaml.get("discovery", {})
    disc_data["study_name"] = raw_yaml.get("study_name", "study")
    config = DiscoveryConfig(**disc_data)

    runner = DiscoveryRunner(config, base_dir)
    return runner.run()


def main():
    parser = argparse.ArgumentParser(description="Phase 1: Discovery Module for lit-pipeline")
    parser.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")
    parser.add_argument("--query", "-q", type=str, help="Quick one-off search query")
    parser.add_argument("--bib", type=str, help="Quick one-off BibTeX seed file")
    parser.add_argument("--study", type=str, default="cli_study", help="Study name")
    parser.add_argument("--depth", type=int, default=1, help="Snowball depth")
    parser.add_argument("--min-year", type=int, default=2018, help="Minimum publication year")
    parser.add_argument("--max-results", type=int, default=50, help="Max results per query")

    args = parser.parse_args()
    base_dir = Path.cwd()

    if args.query:
        # CLI direct mode
        config = DiscoveryConfig(
            study_name=args.study,
            queries=[args.query],
            seed_bib=args.bib,
            min_year=args.min_year,
            max_results_per_query=args.max_results,
            snowball=SnowballConfig(depth=args.depth),
        )
        runner = DiscoveryRunner(config, base_dir)
        runner.run()
    else:
        # Config file mode
        run_discovery(args.config, base_dir)


if __name__ == "__main__":
    main()
