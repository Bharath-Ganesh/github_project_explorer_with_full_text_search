# === project_utils/readme_extractor.py ===

import os
import json
import re
import ast
import fnmatch
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Protocol

import pandas as pd
import nbformat
from rapidfuzz import fuzz, process

from project_utils.starter_class import get_logger, setup_logger, build_context


# ── Importer Interface ─────────────────────────────────────────────────────────

class ImportExtractor(Protocol):
    """
    Protocol for extracting imports from a source file.
    Subclasses implement supports(path) and extract(path).
    """
    def supports(self, path: Path) -> bool:
        ...

    def extract(self, path: Path) -> List[str]:
        ...


# ── Python Import Extractor ─────────────────────────────────────────────────────

class PythonImportExtractor:
    """Extracts top‐level imports from .py files via the AST."""
    def supports(self, path: Path) -> bool:
        return path.suffix.lower() == ".py"

    def extract(self, path: Path) -> List[str]:
        imports = set()
        logger = get_logger(__name__)
        try:
            src = path.read_text(encoding="utf-8", errors="ignore")
            tree = ast.parse(src)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports |= {n.name.split(".")[0] for n in node.names}
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.add(node.module.split(".")[0])
        except SyntaxError as e:
            # broken Python file—skip it
            logger.debug("Skipping broken Python file %s: %s", path, e)
        except Exception as e:
            logger.warning("Error parsing Python file %s: %s", path, e, exc_info=True)
        return sorted(imports)


# ── Notebook Import Extractor ────────────────────────────────────────────────────

class NotebookImportExtractor:
    """Extracts imports from Jupyter notebooks by parsing code cells."""
    def supports(self, path: Path) -> bool:
        return path.suffix.lower() == ".ipynb"

    def _load_notebook(self, path: Path) -> Optional[nbformat.NotebookNode]:
        logger = get_logger(__name__)
        try:
            return nbformat.read(str(path), as_version=4, validate=False)
        except Exception as e:
            logger.debug("Skipping notebook %s: %s", path, e)
            return None

    def extract(self, path: Path) -> List[str]:
        imports = set()
        logger = get_logger(__name__)
        nb = self._load_notebook(path)
        if nb is None:
            return []

        for cell in nb.cells:
            if cell.cell_type != "code":
                continue
            lines = [l for l in cell.source.splitlines() if not l.strip().startswith(("%", "!"))]
            code  = "\n".join(lines)
            try:
                tree = ast.parse(code)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        imports |= {n.name.split(".")[0] for n in node.names}
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        imports.add(node.module.split(".")[0])
            except SyntaxError as e:
                logger.debug("Skipping broken cell in %s: %s", path, e)
            except Exception as e:
                logger.warning("Error parsing code cell in %s: %s", path, e, exc_info=True)

        return sorted(imports)


# ── Main RepoMetadataExtractor ─────────────────────────────────────────────────

class RepoMetadataExtractor:
    """
    Offline‐phase processor that:
      1. Reads config for how many README lines to scan, section rules,
         file patterns to consider, and concurrency.
      2. Loads the 'online' JSON of fork metadata.
      3. Parses each repo:
         - Extracts fuzzy‐matched sections from README.
         - Captures raw README snippet.
         - Walks allowed files, dispatching to ImportExtractor implementations.
      4. Writes out final_projects.json and optionally ingests to Postgres.
    """

    HEADER_RE = re.compile(r"^(#{1,6})\s*(.+?)\s*$", re.I)
    SPLIT_RE  = re.compile(r",|/| and ", re.I)

    def __init__(self, metadata_json: str = "data/project_data.json"):
        setup_logger()
        self.logger = get_logger(__name__)

        ctx = build_context(__name__)
        cfg = ctx.get_required_keys({
            "readme_lines_to_scan",
            "extract_sections",
            "max_threads",
            "sparse_clone_paths",
        })

        self.max_lines     = cfg["readme_lines_to_scan"]
        self.rules         = cfg["extract_sections"]
        self.fields        = list(self.rules.keys())
        self.max_threads   = cfg["max_threads"]
        self.patterns      = cfg["sparse_clone_paths"]
        self.metadata_json = metadata_json

        # Register import extractors
        self.importers: List[ImportExtractor] = [
            PythonImportExtractor(),
            NotebookImportExtractor(),
            # TODO:
            # future: JavaImportExtractor(), etc.
        ]

    def _normalize(self, text: str) -> str:
        return re.sub(r"\W+", " ", text.lower()).strip()

    def _truncate(self, text: str, limit: int = 100) -> str:
        return text if len(text) <= limit else text[:limit].rstrip() + "…"

    def _variants(self, alias: str) -> List[str]:
        alias = alias.lower()
        return [alias, alias[:-1]] if alias.endswith("s") else [alias, alias + "s"]

    def _detect_header(self, line: str, alias_map: Dict[str, str]) -> Optional[str]:
        m = self.HEADER_RE.match(line)
        text = m.group(2) if m else line
        if ":" in text:
            lhs, _ = text.split(":", 1)
            best, score, _ = process.extractOne(self._normalize(lhs), alias_map.keys(), scorer=fuzz.WRatio)
            if score >= 80:
                return alias_map[best]
        elif m:
            best, score, _ = process.extractOne(self._normalize(m.group(2)), alias_map.keys(), scorer=fuzz.WRatio)
            if score >= 80:
                return alias_map[best]
        return None

    def _parse_lines(self, lines: List[str]) -> Dict[str, List[str]]:
        # Build alias_map
        alias_map = {
            var: key
            for key, meta in self.rules.items()
            for alias in meta["aliases"]
            for var in self._variants(alias)
        }

        result: Dict[str, List[str]] = {}
        current: Optional[str]  = None
        first_h1: Optional[str] = None

        for line in lines[: self.max_lines * 2]:
            if first_h1 is None:
                m = self.HEADER_RE.match(line)
                if m and len(m.group(1)) == 1:
                    first_h1 = m.group(2).strip()

            sec = self._detect_header(line, alias_map)
            if sec:
                current = sec
                result.setdefault(sec, [])
                continue

            if current:
                if not line.strip() or len(result[current]) >= self.max_lines:
                    current = None
                else:
                    result[current].append(line)

        # Fallbacks
        for key, meta in self.rules.items():
            fb = meta.get("fallback")
            if fb == "title_from_heading" and not result.get(key) and first_h1:
                result[key] = [first_h1]
            if fb == "split_lines" and result.get(key):
                tokens: List[str] = []
                for ln in result[key]:
                    cleaned = ln.split("#", 1)[0]
                    for token in self.SPLIT_RE.split(cleaned):
                        t = token.strip()
                        if t and t not in tokens:
                            tokens.append(t)
                result[key] = tokens

        return result

    def _parse_readme(self, path: Path) -> Dict[str, List[str]]:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return {}
        return self._parse_lines(text.splitlines())

    def parse_readme_path(self, readme_path: Path) -> Dict[str, List[str]]:
        """Public: parse README.md on disk"""
        return self._parse_readme(readme_path)

    def parse_readme_text(self, content: str) -> Dict[str, List[str]]:
        """Public: parse a raw README snippet string"""
        return self._parse_lines(content.splitlines())

    def _extract_imports(self, path: Path) -> List[str]:
        for importer in self.importers:
            if importer.supports(path):
                return importer.extract(path)
        return []

    def _process_repo(self, repo: Dict) -> Dict:
        repo_path = repo.get("clone_path")
        if not repo_path:
            return repo

        repo = repo.copy()
        rp   = Path(repo_path)

        # README
        readme_file = rp / "README.md"
        if readme_file.exists():
            sections = self._parse_readme(readme_file)
            for key in self.fields:
                vals = sections.get(key, [])
                if key == "team_members":
                    repo[key] = [self._truncate(x, 30) for x in vals]
                else:
                    repo[key] = self._truncate("\n".join(vals), 30) if vals else None
            raw_lines = readme_file.read_text(encoding="utf-8", errors="ignore").splitlines()
            repo["readme_text"] = "\n".join(raw_lines[: self.max_lines])
        else:
            repo.setdefault("errors", []).append("README.md missing")

        # Libraries
        libs = set()
        for root, _, files in os.walk(rp):
            for fn in files:
                full = Path(root) / fn
                if any(fnmatch.fnmatch(str(full), pat) for pat in self.patterns):
                    libs.update(self._extract_imports(full))
        repo["libraries"] = sorted(libs)

        return repo

    def run(self) -> List[Dict]:
        # 1) Load online JSON
        df    = pd.read_json(self.metadata_json)
        forks = df.to_dict(orient="records")

        # 2) Parallel parse
        enriched: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as exe:
            futures = [exe.submit(self._process_repo, f) for f in forks]
            for fut in as_completed(futures):
                enriched.append(fut.result())

        # 3) Write final JSON
        out = Path("data/final_projects.json")
        out.write_text(json.dumps(enriched, indent=2))

        # 4) Optional ingest
        from src.service import ProjectService
        from src.dao     import ProjectsDAO
        svc = ProjectService(ProjectsDAO(), build_context(__name__)._config)
        svc.ingest_projects(enriched)

        return enriched


if __name__ == "__main__":
    RepoMetadataExtractor().run()
