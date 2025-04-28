#!/usr/bin/env python
"""
Project Enricher (Offline Phase)

This class performs Phase 2 of the pipeline:
- Reads `projects.json` (output of the online phase)
- For each repo:
    • extracts README.md text and imported libraries
    • marks enrichment_status (success, skipped, missing_path, or error)
- Saves the result to `enriched_projects.json`

Tool: RepoMetadataExtractor
"""
import os
import json

from project_utils.starter_class import setup_logger, get_logger, build_context
from project_utils.readme_parser import RepoMetadataExtractor


class ProjectEnricher:
    def __init__(self):
        # Initialize logging & get a module-level logger
        setup_logger()
        self.logger = get_logger(self.__class__.__name__)

        # Load paths from config
        ctx = build_context(__name__)
        cfg = ctx.get_required_keys({"output_json", "enriched_json"})
        self.input_path = cfg["output_json"]
        self.output_path = cfg["enriched_json"]

        # Use the unified extractor
        self.extractor = RepoMetadataExtractor()

        self.logger.info(f"[Enricher Initialized] In: {self.input_path} → Out: {self.output_path}")

    def load_projects(self) -> list[dict]:
        if not os.path.exists(self.input_path):
            self.logger.error(f"Input file not found: {self.input_path}")
            return []
        with open(self.input_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.logger.info(f"Loaded {len(data)} projects from {self.input_path}")
            return data

    def enrich(self):
        projects = self.load_projects()
        enriched = []

        for proj in projects:
            owner = proj.get("owner")
            repo = proj.get("repo")

            # Skip if clone failed
            if proj.get("clone_status") != "success":
                proj["enrichment_status"] = "skipped"
                enriched.append(proj)
                continue

            clone_path = proj.get("clone_path")
            if not clone_path or not os.path.isdir(clone_path):
                proj["enrichment_status"] = "missing_path"
                enriched.append(proj)
                continue

            try:
                # Delegate to RepoMetadataExtractor
                enriched_proj = self.extractor._process_repo(proj)
                enriched_proj["enrichment_status"] = "success"
                enriched.append(enriched_proj)
                self.logger.debug(f"Enriched {owner}/{repo}")
            except Exception as e:
                proj["enrichment_status"] = "error"
                proj["enrichment_error"] = str(e)
                self.logger.error(f"Error enriching {owner}/{repo}: {e}")
                enriched.append(proj)

        self.save_output(enriched)

    def save_output(self, enriched_data: list[dict]):
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(enriched_data, f, indent=2)
        self.logger.info(f"Saved enriched metadata ({len(enriched_data)}) to {self.output_path}")

    def run(self):
        self.logger.info("Starting enrichment process…")
        self.enrich()
        self.logger.info("Enrichment process complete.")


if __name__ == "__main__":
    ProjectEnricher().run()
