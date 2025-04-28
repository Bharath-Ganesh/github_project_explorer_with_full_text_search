"""
GitHub Cloner & Metadata Extractor (Online Phase)

This class performs the 'Online Phase' of our pipeline:

1. Load configuration:
   - Reads paths to sparsely clone (e.g., `README.md`, `*.py`, `*.ipynb`) and max threads from `config.yaml`.

2. Read input semester data:
   - Parses `semesters.csv` to load base GitHub template repo URLs and associated semester labels.

3. Fetch fork metadata (Online):
   - Uses GitHub REST API to retrieve all forks for each base repo.
   - Captures fork owner, repo name, URL, creation time, contributors, and semester context.
   - Paginates over API responses to gather full fork list.

4. Shallow clone forks with sparse checkout (Online):
   - Performs minimal `git` clone using sparse checkout and `--depth=1` for efficiency.
   - Clones only selected file types (`README.md`, Python files) into `cloned_repos/<semester>/<owner_repo>/`.
   - Handles both `master` and `main` branches; retries with authentication if unauthenticated clone fails.

5. Track status of cloning:
   - Each fork dictionary is enriched with `clone_status`, `clone_path`, and `errors` fields.
   - This step does **not** perform any README parsing or code analysis.

6. Execute steps concurrently using threads:
   - Cloning operations are performed in parallel using `ThreadPoolExecutor`.
   - Thread count is controlled via config (`max_threads`).

7. Return enriched metadata:
   - Returns a list of dictionaries containing:
     - GitHub metadata (owner, repo, url, created_at, contributors)
     - Semester info
     - Clone tracking fields (status, path, errors)
"""

import os
import time
import subprocess
import requests
import pandas as pd
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from project_utils.logger_setup import get_logger, setup_logger
from project_utils.starter_class import build_context
from project_utils.env_utils import HEADERS

class GitHubCloner:
    def __init__(self):
        setup_logger()
        self.logger = get_logger(__name__)
        values = build_context(__name__).get_required_keys({
            "sparse_clone_paths",
            "max_threads"
        })
        self.paths = values.get("sparse_clone_paths", [])
        self.max_threads = values.get("max_threads", 4)

    def parse_github_repo_url(self, repo_url):
        parts = urlparse(repo_url).path.strip("/").split("/")
        if "network" in parts:
            parts = parts[:parts.index("network")]
        if len(parts) >= 2:
            return parts[0], parts[1]
        raise ValueError(f"Invalid GitHub URL: {repo_url}")

    def fetch_contributors(self, owner, repo):
        url = f"https://api.github.com/repos/{owner}/{repo}/contributors"
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            return [
                {"login": c["login"], "contributions": c["contributions"]}
                for c in response.json()
            ]
        return []

    def get_fork_metadata_from_api(self, base_repo_url, semester):
        owner, repo = self.parse_github_repo_url(base_repo_url)
        api_url = f"https://api.github.com/repos/{owner}/{repo}/forks"

        session = requests.Session()
        session.headers.update(HEADERS)

        forks = []
        page = 1
        year = semester.split()[-1] if " " in semester else semester

        while True:
            self.logger.info(f"Fetching forks (page {page}) from {owner}/{repo} ({semester})...")
            response = session.get(api_url, params={"per_page": 100, "page": page})
            if response.status_code != 200:
                self.logger.warning(f"Request failed for {owner}/{repo}: {response.status_code}")
                break

            data = response.json()
            if not data:
                break

            for fork in data:
                forks.append({
                    "semester": semester,
                    "year": year,
                    "owner": fork["owner"]["login"],
                    "repo": fork["name"],
                    "html_url": fork["html_url"],
                    "created_at": fork.get("created_at"),
                    "contributors": self.fetch_contributors(fork["owner"]["login"], fork["name"]),
                    "clone_status": "pending",
                    "clone_path": None,
                    "errors": []
                })

            page += 1
            time.sleep(0.5)

        return forks

    def shallow_clone_repo(self, owner, repo, semester, destination_root="cloned_repos"):
        folder_name = semester.replace(" ", "").lower()
        dest_dir = os.path.join(destination_root, folder_name, f"{owner}_{repo}")

        if os.path.exists(dest_dir):
            self.logger.info(f"Already cloned: {dest_dir}")
            return dest_dir

        os.makedirs(dest_dir, exist_ok=True)
        public_url = f"https://github.com/{owner}/{repo}.git"
        self.logger.info(f"Attempting sparse clone of {public_url} into {dest_dir}...")

        def run_sparse_clone(repo_url):
            try:
                subprocess.run(["git", "init"], cwd=dest_dir, check=True)
                subprocess.run(["git", "remote", "add", "origin", repo_url], cwd=dest_dir, check=True)
                subprocess.run(["git", "config", "core.sparseCheckout", "true"], cwd=dest_dir, check=True)

                sparse_file = os.path.join(dest_dir, ".git", "info", "sparse-checkout")
                with open(sparse_file, "w") as f:
                    for path in self.paths:
                        f.write(path + "\n")

                result = subprocess.run(["git", "pull", "--depth=1", "origin", "master"], cwd=dest_dir,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if result.returncode != 0:
                    self.logger.warning("Branch 'master' failed, trying 'main'...")
                    result = subprocess.run(["git", "pull", "--depth=1", "origin", "main"], cwd=dest_dir,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    if result.returncode != 0:
                        self.logger.error("Both 'master' and 'main' failed.")
                        self.logger.error(result.stderr.decode())
                        return False
                return True

            except subprocess.CalledProcessError:
                self.logger.exception("Sparse checkout setup failed.")
                return False

        if run_sparse_clone(public_url):
            self.logger.info(f"Successfully sparse cloned into {dest_dir}")
            return dest_dir

        self.logger.warning("Unauthenticated sparse clone failed.")
        try:
            token = input("Enter your GitHub Personal Access Token (or leave blank to skip): ").strip()
            if not token:
                self.logger.info("Skipping clone due to missing token.")
                return None

            authed_url = f"https://{token}@github.com/{owner}/{repo}.git"
            self.logger.info("Retrying sparse clone with authentication...")
            if run_sparse_clone(authed_url):
                self.logger.info(f"Successfully sparse cloned (authenticated) into {dest_dir}")
                return dest_dir
            else:
                self.logger.error("Authenticated sparse clone failed.")
                return None

        except KeyboardInterrupt:
            self.logger.warning("Clone canceled by user.")
            return None

    def clone_and_track_status(self, fork):
        owner, repo, semester = fork["owner"], fork["repo"], fork["semester"]
        try:
            local_path = self.shallow_clone_repo(owner, repo, semester)
            if local_path:
                fork["clone_status"] = "success"
                fork["clone_path"] = local_path
            else:
                fork["clone_status"] = "error"
                fork["errors"].append("Clone failed.")
        except Exception as e:
            fork["clone_status"] = "error"
            fork["errors"].append(str(e))
        return fork

    def get_forks_from_semester_csv(self, csv_path="data/semesters.csv"):
        df = pd.read_csv(csv_path)
        all_fork_metadata = []

        for _, row in df.iterrows():
            semester = row["Semester"]
            base_url = row["GitHub Network URL"]
            try:
                forks = self.get_fork_metadata_from_api(base_url, semester)
                all_fork_metadata.extend(forks)
            except Exception as e:
                self.logger.warning(f"Skipped {semester} due to error: {e}")

        enriched_forks = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = [executor.submit(self.clone_and_track_status, fork) for fork in all_fork_metadata]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    enriched_forks.append(result)

        return enriched_forks

    def run(self):
        self.logger.info(f"Starting GitHubCloner with sparse paths: {self.paths}")
        enriched_data = self.get_forks_from_semester_csv()
        self.logger.info(f"Cloning completed. Total projects: {len(enriched_data)}")
        return enriched_data
