import ast
from project_utils.logger_setup import setup_logger, get_logger
import os
import re

logger = get_logger(__name__)
SECTION_HEADERS = {
    "title": re.compile(r"#*\s*title\s*:?\s*(.*)", re.IGNORECASE),
    "team_members": re.compile(r"#*\s*team\s+member\(s\)?\s*:?", re.IGNORECASE),
    "monte_carlo": re.compile(r"#*\s*monte\s+carlo.*", re.IGNORECASE),
    "section_generic": re.compile(r"#.*")  # for detecting any new markdown header
}

def extract_readme_summary_from_text(readme_text, max_lines=20):
    lines = readme_text.splitlines()[:max_lines]
    n = len(lines)

    result = {
        "title": "",
        "team_members": [],
        "description": "",
        "monte_carlo_simulation": False
    }

    current_section = None
    buffer = []

    i = 0
    while i < n:
        line = lines[i].strip()

        # Check if we are entering a new section
        title_match = SECTION_HEADERS["title"].match(line)
        team_match = SECTION_HEADERS["team_members"].match(line)
        monte_match = SECTION_HEADERS["monte_carlo"].match(line)

        if title_match:
            if buffer and current_section:
                if current_section == "team_members":
                    result["team_members"] = buffer
                elif current_section == "description":
                    result["description"] = " ".join(buffer).strip()
                buffer = []

            title_inline = title_match.group(1).strip()
            if title_inline:
                result["title"] = title_inline
                current_section = None
                logger.debug(f"Found inline title: {result['title']}")
            else:
                current_section = "title"
                logger.debug("Started collecting title from next lines.")
            buffer = []
            i += 1
            continue

        elif team_match:
            if buffer and current_section:
                if current_section == "title":
                    result["title"] = " ".join(buffer).strip()
                elif current_section == "description":
                    result["description"] = " ".join(buffer).strip()
                buffer = []

            current_section = "team_members"
            buffer = []
            logger.debug("Started collecting team members.")
            i += 1
            continue

        elif monte_match:
            if buffer and current_section:
                if current_section == "title":
                    result["title"] = " ".join(buffer).strip()
                elif current_section == "team_members":
                    result["team_members"] = buffer
                buffer = []

            result["monte_carlo_simulation"] = True
            current_section = "description"
            buffer = []
            logger.debug("Started collecting Monte Carlo description.")
            i += 1
            continue

        elif current_section:
            if line:  # skip empty lines
                buffer.append(line)
                logger.debug(f"Added line to '{current_section}' buffer: {line}")

        i += 1

    # Final buffer flush
    if buffer and current_section:
        if current_section == "title":
            result["title"] = " ".join(buffer).strip()
        elif current_section == "team_members":
            result["team_members"] = buffer
        elif current_section == "description":
            result["description"] = " ".join(buffer).strip()

        logger.debug(f"Flushed final buffer for section '{current_section}'.")

    if not result["title"]:
        logger.warning("README skipped due to missing title.")
        return None

    logger.info(f"Extracted README summary: Title='{result['title']}', "
                f"Team Members={len(result['team_members'])}, "
                f"Monte Carlo={result['monte_carlo_simulation']}")

    return result



def extract_imports_from_file(file_path):
    """Parses a local Python file and returns a set of top-level imported libraries."""
    imports = set()
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for n in node.names:
                    imports.add(n.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
    except Exception as e:
        logger.error(f"Failed to parse {file_path}: {e}")
    return imports


def extract_from_local_repo(repo_path):
    """
    Extracts README metadata and Python imports from a local repo directory.

    Returns:
        dict: {
            title: str,
            team_members: list[str],
            monte_carlo_simulation: bool,
            libraries: list[str]
        }
        or None if README is missing or invalid.
    """
    readme_summary = None
    libraries = set()

    for root, _, files in os.walk(repo_path):
        for file in files:
            full_path = os.path.join(root, file)

            # Extract README summary
            if file.lower() == "readme.md" and readme_summary is None:
                try:
                    with open(full_path, encoding="utf-8", errors="ignore") as f:
                        readme_text = f.read()
                    readme_summary = extract_readme_summary_from_text(readme_text)
                except Exception as e:
                    logger.error(f"Error reading README in {repo_path}: {e}")

            # Extract imports from Python source files
            elif file.endswith(".py") and "test" not in file.lower():
                imports = extract_imports_from_file(full_path)
                libraries.update(imports)

    if not readme_summary:
        logger.warning(f"No valid README found in {repo_path}")
        return None

    readme_summary["libraries"] = sorted(libraries)
    logger.info(f"Parsed {repo_path} | Title: {readme_summary['title']} | Imports: {len(libraries)}")
    return readme_summary


if __name__ == '__main__':
    setup_logger()

