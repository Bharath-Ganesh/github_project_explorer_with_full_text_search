#!/usr/bin/env python
"""
GitHub Cloner & Metadata Extractor (Online Phase)

This module implements the “online” half of our pipeline.  It:

1. Loads configuration for sparse‐checkout paths and thread count.
2. Reads `data/project_data.json` if present to pick up prior `pushed_at` timestamps.
3. Fetches all forks for each base repo in `data/semesters.csv`, capturing:
     - semester, year, owner, repo, html_url, created_at, pushed_at, contributors
4. For each fork, shallow‐clones (sparse + depth=1) only the configured file paths,
   skipping any clone whose `pushed_at` hasn’t changed since last run.
5. Retries with authentication if the unauthenticated clone fails.
6. Emits a fresh `data/project_data.json` for the next run.
7. Logs every step for easy debugging and incremental operation.
"""
import os
import time
import json
import shutil
import subprocess
import requests
import pandas as pd

from urllib.parse import urlparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from project_utils.starter_class import setup_logger, get_logger, HEADERS, build_context


class GitHubCloner:
    """
    Performs the 'Online Phase':
      - Reads semesters.csv for base repo URLs.
      - Calls GitHub API to list all forks.
      - Sparse‐clones each fork in parallel, skipping unchanged repos.
      - Tracks clone status, paths, and errors.
    """

    def __init__(self):
        """
        Initialize logger, config, and incremental metadata.
        """
        setup_logger()
        self.logger = get_logger(self.__class__.__name__)

        # Load sparse‐checkout file paths and max threads from config
        cfg = build_context(self.__class__.__name__).get_required_keys({
            "sparse_clone_paths",
            "max_threads"
        })
        self.paths       = cfg["sparse_clone_paths"]
        self.max_threads = cfg["max_threads"]

        # Load previous `pushed_at` timestamps for incremental skipping
        self.old_meta: dict[str, str] = {}
        meta_path = Path("../data") / "project_data.json"
        if meta_path.is_file():
            try:
                prev = json.loads(meta_path.read_text(encoding="utf-8"))
                for fork in prev:
                    key = f"{fork['owner']}/{fork['repo']}"
                    self.old_meta[key] = fork.get("pushed_at", "")
                self.logger.info("Loaded %d existing forks for incremental run", len(self.old_meta))
            except Exception as e:
                self.logger.warning("Could not load old metadata: %s", e)
        else:
            self.logger.info(f"{meta_path} not found" )

    def parse_github_repo_url(self, repo_url: str) -> tuple[str, str]:
        """
        Extract owner and repo name from a GitHub URL.
        """
        parts = urlparse(repo_url).path.strip("/").split("/")
        if "network" in parts:
            parts = parts[:parts.index("network")]
        if len(parts) >= 2:
            return parts[0], parts[1]
        raise ValueError(f"Invalid GitHub URL: {repo_url}")

    def fetch_contributors(self, owner: str, repo: str) -> list[dict]:
        """
        List contributors for a given repo via GitHub API.
        """
        url = f"https://api.github.com/repos/{owner}/{repo}/contributors"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code == 200:
            return [{"login": c["login"], "contributions": c["contributions"]} for c in resp.json()]
        return []

    def get_fork_metadata_from_api(self, base_repo_url: str, semester: str) -> list[dict]:
        """
        Retrieve all forks of `base_repo_url`, capturing metadata including pushed_at.
        """
        owner, repo = self.parse_github_repo_url(base_repo_url)
        api_url = f"https://api.github.com/repos/{owner}/{repo}/forks"
        session = requests.Session()
        session.headers.update(HEADERS)

        forks: list[dict] = []
        page = 1
        year = semester.split()[-1] if " " in semester else semester

        while True:
            self.logger.info("Fetching forks page %d for %s/%s (%s)", page, owner, repo, semester)
            resp = session.get(api_url, params={"per_page": 100, "page": page})
            if resp.status_code != 200:
                self.logger.warning("Failed to list forks for %s/%s: %d", owner, repo, resp.status_code)
                break

            data = resp.json()
            if not data:
                break

            for f in data:
                forks.append({
                    "semester":     semester,
                    "year":         year,
                    "owner":        f["owner"]["login"],
                    "repo":         f["name"],
                    "html_url":     f["html_url"],
                    "created_at":   f.get("created_at"),
                    "pushed_at":    f.get("pushed_at"),
                    "contributors": self.fetch_contributors(f["owner"]["login"], f["name"]),
                    "clone_status": "pending",
                    "clone_path":   None,
                    "errors":       []
                })

            page += 1
            time.sleep(0.5)

        return forks

    def shallow_clone_repo(self, fork: dict, destination_root: str = "cloned_repos") -> str | None:
        """
        Actually does the sparse‐checkout + depth=1 clone into the target folder.
        Returns dest_dir on success or None on any failure.
        """
        owner    = fork["owner"]
        repo     = fork["repo"]
        semester = fork["semester"]
        pushed   = fork.get("pushed_at", "")
        folder   = semester.replace(" ", "").lower()
        dest_dir = os.path.join(destination_root, folder, f"{owner}_{repo}")
        key      = f"{owner}/{repo}"

        # 1) Incremental skip if unchanged
        if os.path.isdir(dest_dir):
            old_p = self.old_meta.get(key, "")
            if old_p == pushed:
                # will be recorded as 'skipped' by clone_and_track_status
                return dest_dir
            # repo updated, delete stale clone
            shutil.rmtree(dest_dir, ignore_errors=True)

        # 2) Prepare fresh clone directory
        os.makedirs(dest_dir, exist_ok=True)
        public_url = f"https://github.com/{owner}/{repo}.git"

        def _run_clone(git_url: str) -> bool:
            """Initialize repo, set sparse‐checkout, and pull both master/main."""
            try:
                subprocess.run(["git", "init"], cwd=dest_dir, check=True)
                subprocess.run(["git", "remote", "add", "origin", git_url], cwd=dest_dir, check=True)
                subprocess.run(["git", "config", "core.sparseCheckout", "true"], cwd=dest_dir, check=True)

                info = os.path.join(dest_dir, ".git", "info", "sparse-checkout")
                with open(info, "w") as f:
                    for p in self.paths:
                        f.write(p + "\n")

                for branch in ("master", "main"):
                    r = subprocess.run(
                        ["git", "pull", "--depth=1", "origin", branch],
                        cwd=dest_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    )
                    if r.returncode == 0:
                        return True

                self.logger.error("Both 'master' and 'main' failed for %s", key)
                return False

            except subprocess.CalledProcessError as e:
                self.logger.exception("Git error cloning %s: %s", key, e)
                return False

        # 3) Unauthenticated attempt
        if _run_clone(public_url):
            return dest_dir

        # 4) Fallback: prompt for token
        self.logger.warning("Unauthenticated clone failed for %s; prompting for token", key)
        token = input("GitHub token (blank to skip): ").strip()
        if token and _run_clone(f"https://{token}@github.com/{owner}/{repo}.git"):
            return dest_dir

        fork["errors"].append("Clone failed")
        return None

    def clone_and_track_status(self, fork: dict) -> dict:
        """
        Decide whether to skip or clone, then update fork dict:
          - clone_status='skipped' if nothing new
          - clone_status='success' if clone succeeded
          - clone_status='error' if clone failed
        """
        owner  = fork["owner"]
        repo   = fork["repo"]
        pushed = fork.get("pushed_at", "")
        key    = f"{owner}/{repo}"

        # 1) If dest exists and unchanged, mark skipped
        folder = fork["semester"].replace(" ", "").lower()
        dest   = os.path.join("cloned_repos", folder, f"{owner}_{repo}")
        old_p  = self.old_meta.get(key, "")
        if os.path.isdir(dest) and old_p == pushed:
            self.logger.info("Skipping %s (no new pushes since %s)", key, pushed)
            fork["clone_status"] = "skipped"
            fork["clone_path"]   = dest
            return fork

        # 2) Otherwise perform a fresh clone
        self.logger.info("Cloning %s (new pushed_at %s)", key, pushed)
        try:
            path = self.shallow_clone_repo(fork)
            if path:
                fork["clone_status"] = "success"
                fork["clone_path"]   = path
            else:
                fork["clone_status"] = "error"
        except Exception as e:
            fork["clone_status"] = "error"
            fork["errors"].append(str(e))
            self.logger.error("Error cloning %s/%s: %s", owner, repo, e, exc_info=True)

        return fork

    def get_forks_from_semester_csv(self) -> list[dict]:
        """
        Reads `data/semesters.csv`, fetches forks for each semester,
        and sparse‐clones them in parallel.
        """
        df = pd.read_csv("../data/semesters.csv")
        all_forks: list[dict] = []

        for _, row in df.iterrows():
            sem = row["Semester"]
            url = row["GitHub Network URL"]
            try:
                forks = self.get_fork_metadata_from_api(url, sem)
                all_forks.extend(forks)
            except Exception as e:
                self.logger.warning("Skipping semester %s due to %s", sem, e)

        enriched: list[dict] = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as exe:
            futures = [exe.submit(self.clone_and_track_status, f) for f in all_forks]
            for fut in as_completed(futures):
                enriched.append(fut.result())

        return enriched

    def run(self) -> list[dict]:
        """
        Entry point: fetch, clone, and return enriched metadata.
        """
        self.logger.info("Starting GitHubCloner (incremental)...")
        data = self.get_forks_from_semester_csv()
        self.logger.info("Cloning phase complete: %d forks processed", len(data))
        return data


if __name__ == "__main__":
    """
    CLI entry: run cloner and write out JSON for next incremental run.
    """
    cloner = GitHubCloner()
    result  = cloner.run()

    out = Path("data") / "project_data.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Wrote {len(result)} records to {out}")



