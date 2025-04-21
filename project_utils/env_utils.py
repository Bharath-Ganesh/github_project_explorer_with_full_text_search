import os

def load_env_from_file(filepath=".env"):
    """Loads environment variables from a .env file into os.environ"""
    if not os.path.exists(filepath):
        print(f"Warning: {filepath} file not found.")
        return

    with open(filepath) as f:
        for line in f:
            if line.strip() and not line.strip().startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ[key] = value


# Load token
load_env_from_file()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
LOGGER_LEVEL = os.getenv("LOG_LEVEL")

# Set headers for GitHub API requests
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}