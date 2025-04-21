import time
import pandas as pd
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from project_utils.readme_library_parser import extract_from_local_repo
from project_utils.env_utils import HEADERS
import os
import subprocess
from project_utils.logger_setup import get_logger, setup_logger

logger = get_logger(__name__)

def shallow_clone_repo(owner, repo, semester, destination_root="cloned_repos"):
    folder_name = semester.replace(" ", "").lower()
    dest_dir = os.path.join(destination_root, folder_name, f"{owner}_{repo}")
    a : int = 2
        
    if os.path.exists(dest_dir):
        logger.info(f"Already cloned: {dest_dir}")
        return dest_dir

    os.makedirs(dest_dir, exist_ok=True)
    public_url = f"https://github.com/{owner}/{repo}.git"
    logger.info(f"Attempting sparse clone of {public_url} into {dest_dir}...")

    def run_sparse_clone(repo_url):
        try:
            subprocess.run(["git", "init"], cwd=dest_dir, check=True)
            subprocess.run(["git", "remote", "add", "origin", repo_url], cwd=dest_dir, check=True)
            subprocess.run(["git", "config", "core.sparseCheckout", "true"], cwd=dest_dir, check=True)

            # Set sparse checkout patterns
            sparse_file = os.path.join(dest_dir, ".git", "info", "sparse-checkout")
            with open(sparse_file, "w") as f:
                f.write("README.md\n")
                f.write("*.py\n")
                f.write("**/*.py\n")

            # Try pulling from master, fallback to main
            result = subprocess.run(["git", "pull", "--depth=1", "origin", "master"], cwd=dest_dir,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode != 0:
                logger.warning("Branch 'master' failed, trying 'main'...")
                result = subprocess.run(["git", "pull", "--depth=1", "origin", "main"], cwd=dest_dir,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if result.returncode != 0:
                    logger.error("Both 'master' and 'main' failed.")
                    logger.error(result.stderr.decode())
                    return False
            return True

        except subprocess.CalledProcessError as e:
            logger.exception("Sparse checkout setup failed.")
            return False

    if run_sparse_clone(public_url):
        logger.info(f"Successfully sparse cloned into {dest_dir}")
        return dest_dir

    logger.warning("Unauthenticated sparse clone failed.")
    try:
        token = input("Enter your GitHub Personal Access Token (or leave blank to skip): ").strip()
        if not token:
            logger.info("Skipping clone due to missing token.")
            return None

        authed_url = f"https://{token}@github.com/{owner}/{repo}.git"
        logger.info("Retrying sparse clone with authentication...")
        if run_sparse_clone(authed_url):
            logger.info(f"Successfully sparse cloned (authenticated) into {dest_dir}")
            return dest_dir
        else:
            logger.error("Authenticated sparse clone failed.")
            return None

    except KeyboardInterrupt:
        logger.warning("Clone canceled by user.")
        return None

def parse_github_repo_url(repo_url):
    parts = urlparse(repo_url).path.strip("/").split("/")
    if "network" in parts:
        parts = parts[:parts.index("network")]
    if len(parts) >= 2:
        return parts[0], parts[1]
    raise ValueError(f"Invalid GitHub URL: {repo_url}")

def get_fork_metadata_from_api(base_repo_url, semester):
    owner, repo = parse_github_repo_url(base_repo_url)
    api_url = f"https://api.github.com/repos/{owner}/{repo}/forks"

    session = requests.Session()
    session.headers.update(HEADERS)

    forks = []
    page = 1
    year = semester.split()[-1] if " " in semester else semester

    while True:
        logger.info(f"Fetching forks (page {page}) from {owner}/{repo} ({semester})...")
        response = session.get(api_url, params={"per_page": 100, "page": page})
        if response.status_code != 200:
            logger.warning(f"Request failed for {owner}/{repo}: {response.status_code}")
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
                "html_url": fork["html_url"]
            })

        page += 1
        time.sleep(0.5)

    return forks

def clone_and_enrich_fork(fork, include_libraries=True):
    semester = fork["semester"]
    owner = fork["owner"]
    repo = fork["repo"]

    local_path = shallow_clone_repo(owner, repo, semester)
    if not local_path:
        return None

    summary = extract_from_local_repo(local_path) if include_libraries else {}
    if summary is None:
        logger.warning(f"No valid README found for {owner}/{repo} ({semester})")
        summary = {
            "readme_text": "",
            "imports": []
        }

    return {**fork, **summary}

def get_forks_from_semester_csv(csv_path="data/semesters.csv", include_libraries=True, max_threads=4):
    df = pd.read_csv(csv_path)
    all_fork_metadata = []

    # Phase 1: Gather fork metadata
    for _, row in df.iterrows():
        semester = row["Semester"]
        base_url = row["GitHub Network URL"]
        try:
            forks = get_fork_metadata_from_api(base_url, semester)
            all_fork_metadata.extend(forks)
        except Exception as e:
            logger.warning(f"Skipped {semester} due to error: {e}")

    # Phase 2: Clone and parse in parallel
    enriched_forks = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(clone_and_enrich_fork, fork, include_libraries) for fork in all_fork_metadata]
        for future in as_completed(futures):
            result = future.result()
            if result:
                enriched_forks.append(result)

    return enriched_forks

if __name__ == '__main__':
    setup_logger()

