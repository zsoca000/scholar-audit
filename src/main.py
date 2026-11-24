"""
Main Entrypoint for Research Intelligence Pipeline (lit-pipeline).

Provides a single, streamlined CLI interface for all pipeline phases:
  discover   - Phase 1: Pure Python, AI-free OpenAlex search & citation snowballing.
  triage     - Phase 2: Agentic abstract screening (--mode api | local) + auto Zotero .bib export.
  diagnose   - Diagnostics: ASCII/Unicode charts & summary reports for discovered/triaged datasets.
  extract    - Phase 3: Zotero-first PDF ingestion & methodological extraction audit.
  synthesize - Phase 4: Comparative synthesis & research gap report.

Usage:
  python src/main.py discover --config configs/template.yaml
  python src/main.py triage --config configs/template.yaml --mode api
  python src/main.py diagnose --config configs/template.yaml --phase all
  python src/main.py extract --config configs/template.yaml --import-zotero
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main():
    parser = argparse.ArgumentParser(
        prog="lit-pipeline",
        description="Modular Research Intelligence Pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Pipeline command to run")

    # Command: discover
    p_disc = subparsers.add_parser("discover", help="Phase 1: Deterministic OpenAlex search & citation snowballing")
    p_disc.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")
    p_disc.add_argument("--seed-limit", type=int, default=None, help="Limit number of seed papers")

    # Command: triage
    p_triage = subparsers.add_parser("triage", help="Phase 2: Abstract screening with agent + Zotero .bib export")
    p_triage.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")
    p_triage.add_argument("--mode", "-m", choices=["api", "local"], default="api", help="Execution mode: api or local")
    p_triage.add_argument("--limit", "-l", type=int, default=None, help="Limit number of papers to screen")
    p_triage.add_argument("--batch-size", "-b", type=int, default=None, help="Micro-batch size for LLM evaluation")

    # Command: diagnose
    p_diag = subparsers.add_parser("diagnose", help="Diagnostics & Meta-Analysis for pipeline stages")
    p_diag.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")
    p_diag.add_argument("--phase", "-p", choices=["discovered", "triaged", "all"], default="all", help="Phase to diagnose")
    p_diag.add_argument("--top", "-t", type=int, default=15, help="Number of top venues to chart")

    # Command: extract
    p_ext = subparsers.add_parser("extract", help="Phase 3: Zotero-first PDF ingestion & methodological audit")
    p_ext.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")
    p_ext.add_argument("--check", action="store_true", help="Only check and reconcile PDFs without running extraction")
    p_ext.add_argument("--prepare-only", action="store_true", help="Only prepare texts without running agent audit")

    # Command: synthesize
    p_synth = subparsers.add_parser("synthesize", help="Phase 4: Synthesis report and research gap matrix")
    p_synth.add_argument("--config", "-c", type=str, default="configs/template.yaml", help="Path to study YAML config")

    args = parser.parse_args()
    base_dir = Path.cwd()

    if args.command == "discover":
        from src.discovery import DiscoveryRunner
        runner = DiscoveryRunner(args.config, base_dir=base_dir)
        runner.run(seed_limit=args.seed_limit)

    elif args.command == "triage":
        from src.triage import TriageRunner
        runner = TriageRunner(args.config, base_dir=base_dir)
        if args.batch_size:
            runner.batch_size = args.batch_size
        if args.mode:
            runner.mode = args.mode
        runner.run(limit=args.limit)

    elif args.command == "diagnose":
        from src.analytics import PipelineDiagnostics
        diag = PipelineDiagnostics(args.config, base_dir=base_dir)
        if args.phase in ("discovered", "all"):
            try:
                res1 = diag.diagnose_discovery(top_n_venues=args.top)
                print("\n" + res1 + "\n")
            except Exception as e:
                print(f"[Warning] Discovery diagnostics: {e}")
        if args.phase in ("triaged", "all"):
            try:
                res2 = diag.diagnose_triage(top_n_venues=args.top)
                print("\n" + res2 + "\n")
            except Exception as e:
                print(f"[Warning] Triage diagnostics: {e}")

    elif args.command == "extract":
        from src.extraction import ExtractionRunner
        runner = ExtractionRunner(args.config, base_dir=base_dir)
        if args.check:
            runner.reconcile_and_link_pdfs()
        else:
            runner.prepare_fulltexts()

    elif args.command == "synthesize":
        from src.synthesis import SynthesisRunner
        runner = SynthesisRunner(args.config, base_dir=base_dir)
        runner.run()


if __name__ == "__main__":
    main()
