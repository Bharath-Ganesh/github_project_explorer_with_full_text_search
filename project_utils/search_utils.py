from project_utils.logger_setup import setup_logger, get_logger


def search_projects(projects_data, keyword):
    keyword = keyword.lower()
    return [
        proj for proj in projects_data
        if "title" in proj and keyword.lower() in proj["title"].lower()
        or keyword in " ".join(proj.get("team_members", [])).lower()
        or any(keyword in lib.lower() for lib in proj.get("libraries", []))
    ]

def extract_core_fields(readme_text):
    """
    Extracts Title, Team Members, and Simulation Purpose from a README.
    """
    lines = readme_text.splitlines()
    result = {
        "title": "",
        "team_members": [],
        "description": ""
    }

    current_section = None
    buffer = []

    for line in lines:
        line_strip = line.strip()
        line_lower = line_strip.lower()

        # Detect section headers
        if line_lower.startswith("title:"):
            result["title"] = line_strip[len("title:"):].strip()
            current_section = None  # reset section
            continue

        if line_lower.startswith("team member") or "team member(s)" in line_lower:
            current_section = "team_members"
            continue

        if line_lower.startswith("monte carlo simulation scenario"):
            current_section = "description"
            continue

        # Stop at known "end" sections
        if any(keyword in line_lower for keyword in ["hypothesis", "instruction", "source used", "variable"]):
            current_section = None
            continue

        # Capture content under current section
        if current_section == "team_members" and line_strip:
            result["team_members"].append(line_strip)

        elif current_section == "description":
            buffer.append(line_strip)

    if buffer:
        result["description"] = "\n".join(buffer).strip()

    return result

if __name__ == '__main__':
    logger = get_logger(__name__)