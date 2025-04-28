# === project_utils/readme_extractor.py ===
import os
import json
import re
import ast
import fnmatch
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import pandas as pd
import nbformat
from rapidfuzz import fuzz, process

from project_utils.logger_setup import get_logger, setup_logger
from project_utils.starter_class import build_context


class RepoMetadataExtractor:
    """
    Offline-phase processor that:
      1. Reads config for README lines, section extraction rules, allowed file patterns,
         and concurrency.
      2. Loads the 'online' JSON of forks.
      3. Parses each repo:
         - Extracts configured sections from README via fuzzy headers + fallbacks.
         - Captures up to N lines of raw README.
         - Walks allowed .py/.ipynb files to extract import libraries.
      4. Writes out final JSON and pushes to Postgres via your DAO.
    """

    HEADER_RE = re.compile(r"^(#{1,6})\s*(.+?)\s*$", re.I)
    SPLIT_RE  = re.compile(r",|/| and ", re.I)

    def __init__(self, metadata_json: str = "data/enriched_projects.json"):
        setup_logger()
        self.logger = get_logger(__name__)

        # Load exactly the keys that exist in config.yaml
        ctx = build_context(__name__)
        cfg = ctx.get_required_keys({
            "readme_lines_to_scan",
            "extract_sections",
            "max_threads",
            "sparse_clone_paths",
        })

        self.max_lines    = cfg["readme_lines_to_scan"]
        self.rules        = cfg["extract_sections"]    # section extraction rules
        self.fields       = list(self.rules.keys())    # infer field names
        self.max_threads  = cfg["max_threads"]
        self.patterns     = cfg["sparse_clone_paths"]  # allowed file glob patterns
        self.metadata_json = metadata_json

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
            best, score, _ = process.extractOne(self._normalize(lhs), alias_map.keys(),
                                                 scorer=fuzz.WRatio)
            if score >= 80:
                return alias_map[best]
        elif m:
            best, score, _ = process.extractOne(self._normalize(m.group(2)), alias_map.keys(),
                                                 scorer=fuzz.WRatio)
            if score >= 80:
                return alias_map[best]
        return None

    def _parse_lines(self, lines: List[str]) -> Dict[str, List[str]]:
        # Build alias_map: normalized variant → field key
        alias_map = {
            var: key
            for key, meta in self.rules.items()
            for alias in meta["aliases"]
            for var in self._variants(alias)
        }

        result: Dict[str, List[str]] = {}
        current: Optional[str] = None
        first_h1: Optional[str] = None

        # Scan up to twice max_lines for headers & content
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

        # Apply fallbacks (title from heading, or splitting lines)
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
        """
        Public: parse README.md on disk
        """
        return self._parse_readme(readme_path)

    def parse_readme_text(self, content: str) -> Dict[str, List[str]]:
        """
        Public: parse a raw README snippet string
        """
        return self._parse_lines(content.splitlines())

    def _extract_imports(self, path: Path) -> List[str]:
        imports = set()
        try:
            if path.suffix == ".py":
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        imports |= {n.name.split(".")[0] for n in node.names}
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        imports.add(node.module.split(".")[0])

            elif path.suffix == ".ipynb":
                nb = nbformat.read(path, as_version=4)
                for cell in nb.cells:
                    if cell.cell_type != "code":
                        continue
                    code = "\n".join(
                        l for l in cell.source.splitlines()
                        if not l.strip().startswith(("%", "!"))
                    )
                    try:
                        tree = ast.parse(code)
                        for node in ast.walk(tree):
                            if isinstance(node, ast.Import):
                                imports |= {n.name.split(".")[0] for n in node.names}
                            elif isinstance(node, ast.ImportFrom) and node.module:
                                imports.add(node.module.split(".")[0])
                    except SyntaxError:
                        continue
        except Exception as e:
            self.logger.warning(f"Failed parsing {path}: {e}")
        return sorted(imports)

    def _process_repo(self, repo: Dict) -> Dict:
        repo_path = repo.get("clone_path")
        if not repo_path:
            return repo
        repo = repo.copy()
        rp = Path(repo_path)

        # README sections
        readme_file = rp / "README.md"
        if readme_file.exists():
            sections = self._parse_readme(readme_file)
            for key in self.fields:
                vals = sections.get(key, [])
                if key == "team_members":
                    repo[key] = [self._truncate(x, 30) for x in vals]
                else:
                    repo[key] = self._truncate("\n".join(vals), 30) if vals else None
            raw = readme_file.read_text(encoding="utf-8", errors="ignore").splitlines()
            repo["readme_text"] = "\n".join(raw[: self.max_lines])
        else:
            repo.setdefault("errors", []).append("README.md missing")

        # Library imports
        libs = set()
        for root, _, files in os.walk(rp):
            for fn in files:
                full = Path(root) / fn
                if any(fnmatch.fnmatch(str(full), pat) for pat in self.patterns):
                    libs.update(self._extract_imports(full))
        repo["libraries"] = sorted(libs)

        return repo

    def run(self) -> List[Dict]:
        # 1) Load
        df = pd.read_json(self.metadata_json)
        forks = df.to_dict(orient="records")

        # 2) Parallel parsing
        enriched: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as exe:
            for fut in as_completed([exe.submit(self._process_repo, f) for f in forks]):
                enriched.append(fut.result())

        # 3) Output JSON
        out = Path("data/final_projects.json")
        out.write_text(json.dumps(enriched, indent=2))

        # 4) Push to Postgres via your unified Service/DAO
        from src.service import ProjectService
        from src.dao     import ProjectsDAO
        svc = ProjectService(ProjectsDAO(), build_context(__name__)._config)
        svc.ingest_projects(enriched)

        return enriched


if __name__ == "__main__":
    RepoMetadataExtractor().run()
