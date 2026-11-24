"""
Diagnostics & Scientometrics Module for lit-pipeline.

Generates visual and quantitative meta-analysis of pipeline phase outputs:
- Phase 1 Diagnostics: output/<study>/01_discovered_summary.md (from 01_discovered.parquet)
- Phase 2 Diagnostics: output/<study>/02_triaged_summary.md (from 02_triaged.parquet)

100% Config-driven & Reusable:
- Visual horizontal ASCII/Unicode bar charts for easy terminal & Markdown readability.
- Source breakdown: Seeds vs. Query axes vs. Snowball expansion.
- Retention & triage funnel metrics.
- Publication venue concentrations and temporal trajectories.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("diagnostics")


def render_horizontal_bar(count: int, max_count: int, bar_width: int = 25) -> str:
    """Renders a clean unicode horizontal bar scaled to bar_width."""
    if max_count == 0:
        return ""
    filled_len = int(round(bar_width * (count / max_count)))
    filled_len = max(1 if count > 0 else 0, filled_len)
    return "█" * filled_len + "░" * (bar_width - filled_len)


class PipelineDiagnostics:
    """Computes transparent diagnostics and visual summaries for pipeline stages."""

    def __init__(self, config_path: str | Path, base_dir: Path):
        self.base_dir = base_dir
        self.config_path = base_dir / config_path if not Path(config_path).is_absolute() else Path(config_path)

        with open(self.config_path, "r", encoding="utf-8") as f:
            self.raw_yaml = yaml.safe_load(f)

        self.study_name = self.raw_yaml.get("study_name", "study")
        self.study_dir = base_dir / "output" / self.study_name
        self.discovered_parquet = self.study_dir / "01_discovered.parquet"
        self.fallback_discovered = self.study_dir / "01_papers.parquet"
        self.triaged_parquet = self.study_dir / "02_triaged.parquet"
        self.evidence_parquet = self.study_dir / "03_evidence.parquet"

    def _get_discovered_file(self) -> Optional[Path]:
        if self.discovered_parquet.exists():
            return self.discovered_parquet
        if self.fallback_discovered.exists():
            return self.fallback_discovered
        return None

    def diagnose_discovery(self, top_n_venues: int = 15) -> str:
        """Generates diagnostics for Phase 1: Discovered Papers."""
        disc_file = self._get_discovered_file()
        if not disc_file:
            raise FileNotFoundError(f"Discovered papers file not found in {self.study_dir}. Run discovery first!")

        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{disc_file.as_posix()}')").fetchdf()
        con.close()

        total = len(df)
        seeds_count = df["is_seed"].sum() if "is_seed" in df.columns else 0
        snowball_count = total - seeds_count

        # Source breakdown
        sources = {}
        for src_str in df.get("discovery_source", pd.Series(dtype=object)).dropna():
            parts = [p.strip() for p in str(src_str).split(";")]
            for p in parts:
                cat = "Other"
                if "seed_bib" in p:
                    cat = "Golden Seeds (.bib)"
                elif "query_axis" in p:
                    cat = f"Search Query ({p})"
                elif "snowball_citation" in p:
                    cat = "Snowball (Forward Citations)"
                elif "snowball_reference" in p:
                    cat = "Snowball (Backward References)"
                sources[cat] = sources.get(cat, 0) + 1

        df_sources = pd.DataFrame(list(sources.items()), columns=["source", "count"]).sort_values(by="count", ascending=False)
        max_src = df_sources["count"].max() if not df_sources.empty else 1

        # Venues
        df_venues = df["venue"].fillna("Unknown / Direct Submission").value_counts().reset_index()
        df_venues.columns = ["venue", "count"]
        top_venues = df_venues.head(top_n_venues)
        max_venue = top_venues["count"].max() if not top_venues.empty else 1

        # Years
        df_years = df["publication_year"].dropna().astype(int).value_counts().sort_index().reset_index()
        df_years.columns = ["year", "count"]
        max_year = df_years["count"].max() if not df_years.empty else 1

        out_lines = []
        out_lines.append(f"# Phase 1 Discovery Diagnostics: `{self.study_name}`")
        out_lines.append(f"**Total Discovered Papers:** **{total:,}** | **Seeds:** {seeds_count} | **Snowballed / Query Hits:** {snowball_count}\n")

        out_lines.append("## 1. Discovery Source Breakdown")
        out_lines.append("```text")
        out_lines.append(f"{'SOURCE / CHANNEL':<38} | {'COUNT':<6} | {'SHARE':<6} | DISTRIBUTION")
        out_lines.append("-" * 80)
        for _, r in df_sources.iterrows():
            sname = str(r["source"])[:36]
            cnt = int(r["count"])
            pct = (cnt / total) * 100
            bar = render_horizontal_bar(cnt, max_src, bar_width=22)
            out_lines.append(f"{sname:<38} | {cnt:<6} | {pct:>5.1f}% | {bar}")
        out_lines.append("```\n")

        out_lines.append(f"## 2. Top {top_n_venues} Publication Venues")
        out_lines.append("```text")
        out_lines.append(f"{'VENUE / JOURNAL':<50} | {'COUNT':<5} | {'SHARE':<6} | DISTRIBUTION")
        out_lines.append("-" * 92)
        for _, r in top_venues.iterrows():
            vname = str(r["venue"])[:48]
            cnt = int(r["count"])
            pct = (cnt / total) * 100
            bar = render_horizontal_bar(cnt, max_venue, bar_width=22)
            out_lines.append(f"{vname:<50} | {cnt:<5} | {pct:>5.1f}% | {bar}")
        out_lines.append("```\n")

        out_lines.append("## 3. Publication Timeline")
        out_lines.append("```text")
        out_lines.append(f"{'YEAR':<6} | {'COUNT':<5} | {'SHARE':<6} | GROWTH")
        out_lines.append("-" * 50)
        for _, r in df_years.iterrows():
            yr = int(r["year"])
            cnt = int(r["count"])
            pct = (cnt / total) * 100
            bar = render_horizontal_bar(cnt, max_year, bar_width=22)
            out_lines.append(f"{yr:<6} | {cnt:<5} | {pct:>5.1f}% | {bar}")
        out_lines.append("```\n")

        out_md = self.study_dir / "01_discovered_summary.md"
        content = "\n".join(out_lines)
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Saved Phase 1 diagnostics to {out_md.name}")
        return content

    def diagnose_triage(self, top_n_venues: int = 15) -> str:
        """Generates diagnostics for Phase 2: Triaged Papers."""
        if not self.triaged_parquet.exists():
            raise FileNotFoundError(f"Triaged file not found: {self.triaged_parquet}. Run triage first!")

        con = duckdb.connect()
        df_triaged = con.execute(f"SELECT * FROM read_parquet('{self.triaged_parquet.as_posix()}')").fetchdf()

        disc_file = self._get_discovered_file()
        total_discovered = None
        if disc_file:
            df_disc = con.execute(f"SELECT COUNT(*) FROM read_parquet('{disc_file.as_posix()}')").fetchone()
            total_discovered = df_disc[0] if df_disc else None
        con.close()

        retained = len(df_triaged)
        retention_pct = (retained / total_discovered) * 100 if total_discovered else 0.0

        # Venues
        df_venues = df_triaged["venue"].fillna("Unknown / Direct Submission").value_counts().reset_index()
        df_venues.columns = ["venue", "count"]
        top_venues = df_venues.head(top_n_venues)
        max_venue = top_venues["count"].max() if not top_venues.empty else 1
        unique_venues = len(df_venues)

        # Years
        df_years = df_triaged["publication_year"].dropna().astype(int).value_counts().sort_index().reset_index()
        df_years.columns = ["year", "count"]
        max_year = df_years["count"].max() if not df_years.empty else 1

        out_lines = []
        out_lines.append(f"# Phase 2 Triage Diagnostics: `{self.study_name}`")
        if total_discovered:
            out_lines.append(f"**Funnel:** **{retained:,}** retained out of **{total_discovered:,}** screened (**{retention_pct:.1f}% acceptance rate**)")
        else:
            out_lines.append(f"**Total Retained Papers:** **{retained:,}**")
        out_lines.append(f"**Distinct Publication Venues:** **{unique_venues}**\n")

        out_lines.append(f"## 1. Top {top_n_venues} Publication Venues (Filtered Literature)")
        out_lines.append("```text")
        out_lines.append(f"{'VENUE / JOURNAL':<50} | {'COUNT':<5} | {'SHARE':<6} | DISTRIBUTION")
        out_lines.append("-" * 92)
        for _, r in top_venues.iterrows():
            vname = str(r["venue"])[:48]
            cnt = int(r["count"])
            pct = (cnt / retained) * 100
            bar = render_horizontal_bar(cnt, max_venue, bar_width=22)
            out_lines.append(f"{vname:<50} | {cnt:<5} | {pct:>5.1f}% | {bar}")
        out_lines.append("```\n")

        out_lines.append("## 2. Retained Papers Publication Timeline")
        out_lines.append("```text")
        out_lines.append(f"{'YEAR':<6} | {'COUNT':<5} | {'SHARE':<6} | GROWTH")
        out_lines.append("-" * 50)
        for _, r in df_years.iterrows():
            yr = int(r["year"])
            cnt = int(r["count"])
            pct = (cnt / retained) * 100
            bar = render_horizontal_bar(cnt, max_year, bar_width=22)
            out_lines.append(f"{yr:<6} | {cnt:<5} | {pct:>5.1f}% | {bar}")
        out_lines.append("```\n")

        out_lines.append("## 3. Top 10 Influential / Anchor Papers")
        out_lines.append("| Year | Citations | Venue | Title | DOI |")
        out_lines.append("|---|---|---|---|---|")
        top_cited = df_triaged.sort_values(by="cited_by_count", ascending=False).head(10)
        for _, r in top_cited.iterrows():
            yr = r.get("publication_year", "N/A")
            cites = r.get("cited_by_count", 0)
            ven = str(r.get("venue", "N/A"))[:28]
            ttl = str(r.get("title", ""))[:60]
            doi = str(r.get("doi", ""))
            doi_link = f"[{doi}](https://doi.org/{doi})" if doi and doi != "None" else "N/A"
            out_lines.append(f"| {yr} | **{cites}** | {ven} | {ttl}... | {doi_link} |")

        out_md = self.study_dir / "02_triaged_summary.md"
        content = "\n".join(out_lines)
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Saved Phase 2 diagnostics to {out_md.name}")
        return content

    def diagnose_evidence(self) -> str:
        """Generates diagnostics for Phase 3: Audited Evidence & The 3 Dilemmas."""
        if not self.evidence_parquet.exists():
            raise FileNotFoundError(f"Evidence file not found: {self.evidence_parquet}. Run extraction audit first!")

        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{self.evidence_parquet.as_posix()}')").fetchdf()
        con.close()

        total = len(df)
        has_fulltext_count = df["has_fulltext"].sum() if "has_fulltext" in df.columns else 0

        out_lines = []
        out_lines.append(f"# Phase 3 Evidence & Dilemma Diagnostics: `{self.study_name}`")
        out_lines.append(f"**Total Audited Papers:** **{total}** | **Full-Text PDFs:** **{has_fulltext_count} ({has_fulltext_count/total*100:.1f}%)**\n")

        # DILEMMA 1: Generator Bias
        if "generator_bias_acknowledged_value" in df.columns:
            bias_counts = df["generator_bias_acknowledged_value"].value_counts().reset_index()
            bias_counts.columns = ["status", "count"]
            max_cnt = bias_counts["count"].max() if not bias_counts.empty else 1

            out_lines.append("## Dilemma 1: Generator Bias & Covariate Shift Awareness")
            out_lines.append("```text")
            out_lines.append(f"{'STATUS':<25} | {'COUNT':<5} | {'SHARE':<6} | DISTRIBUTION")
            out_lines.append("-" * 65)
            for _, r in bias_counts.iterrows():
                st = str(r["status"])[:23]
                cnt = int(r["count"])
                pct = (cnt / total) * 100
                bar = render_horizontal_bar(cnt, max_cnt, bar_width=20)
                out_lines.append(f"{st:<25} | {cnt:<5} | {pct:>5.1f}% | {bar}")
            out_lines.append("```\n")

        # DILEMMA 2: Recurrent / GRU Standing
        if "recurrent_gru_role_value" in df.columns:
            def simplify_role(val):
                v = str(val).upper()
                if "PRIMARY_PROPOSED_MODEL" in v:
                    return "PRIMARY_PROPOSED_MODEL"
                elif "COMPETITIVE_BASELINE" in v:
                    return "COMPETITIVE_BASELINE"
                elif "DISMISSED" in v or "OBSOLETE" in v:
                    return "DISMISSED_AS_OBSOLETE"
                elif "NOT_MENTIONED" in v:
                    return "NOT_MENTIONED"
                else:
                    return "LITERATURE_ONLY / OTHER"

            df["gru_simplified"] = df["recurrent_gru_role_value"].apply(simplify_role)
            gru_counts = df["gru_simplified"].value_counts().reset_index()
            gru_counts.columns = ["role", "count"]
            max_cnt = gru_counts["count"].max() if not gru_counts.empty else 1

            out_lines.append("## Dilemma 2: Standing of Recurrent Neural Networks (GRU/LSTM)")
            out_lines.append("```text")
            out_lines.append(f"{'ROLE OF GRU / RNN':<26} | {'COUNT':<5} | {'SHARE':<6} | DISTRIBUTION")
            out_lines.append("-" * 65)
            for _, r in gru_counts.iterrows():
                rl = str(r["role"])[:24]
                cnt = int(r["count"])
                pct = (cnt / total) * 100
                bar = render_horizontal_bar(cnt, max_cnt, bar_width=20)
                out_lines.append(f"{rl:<26} | {cnt:<5} | {pct:>5.1f}% | {bar}")
            out_lines.append("```\n")

        # DILEMMA 3: Generalization Illusion
        if "is_genuine_ood_value" in df.columns:
            ood_counts = df["is_genuine_ood_value"].value_counts().reset_index()
            ood_counts.columns = ["verdict", "count"]
            max_cnt = ood_counts["count"].max() if not ood_counts.empty else 1

            out_lines.append("## Dilemma 3: Generalization Illusion (Echo-Chamber vs Genuine OOD)")
            out_lines.append("```text")
            out_lines.append(f"{'IS GENUINE OOD?':<25} | {'COUNT':<5} | {'SHARE':<6} | DISTRIBUTION")
            out_lines.append("-" * 65)
            for _, r in ood_counts.iterrows():
                vd = str(r["verdict"])[:23]
                cnt = int(r["count"])
                pct = (cnt / total) * 100
                bar = render_horizontal_bar(cnt, max_cnt, bar_width=20)
                out_lines.append(f"{vd:<25} | {cnt:<5} | {pct:>5.1f}% | {bar}")
            out_lines.append("```\n")

        out_md = self.study_dir / "03_evidence_summary.md"
        content = "\n".join(out_lines)
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"Saved Phase 3 diagnostics to {out_md.name}")
        return content


def main():
    parser = argparse.ArgumentParser(description="Diagnostics & Scientometrics Module for lit-pipeline")
    parser.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")
    parser.add_argument("--phase", "-p", choices=["discovered", "triaged", "evidence", "all"], default="all", help="Phase to diagnose")
    parser.add_argument("--top", "-t", type=int, default=15, help="Number of top venues to chart")

    args = parser.parse_args()
    base_dir = Path.cwd()

    diag = PipelineDiagnostics(args.config, base_dir)

    if args.phase in ("discovered", "all"):
        try:
            res1 = diag.diagnose_discovery(top_n_venues=args.top)
            print("\n" + res1 + "\n")
        except Exception as e:
            logger.warning(f"Could not run discovery diagnostics: {e}")

    if args.phase in ("triaged", "all"):
        try:
            res2 = diag.diagnose_triage(top_n_venues=args.top)
            print("\n" + res2 + "\n")
        except Exception as e:
            logger.warning(f"Could not run triage diagnostics: {e}")

    if args.phase in ("evidence", "all"):
        try:
            res3 = diag.diagnose_evidence()
            print("\n" + res3 + "\n")
        except Exception as e:
            logger.warning(f"Could not run evidence diagnostics: {e}")


if __name__ == "__main__":
    main()
