# GitHub Project Metadata Explorer

## Overview

We built this tool to help analyze and explore classroom project repositories hosted on GitHub. These are usually final projects from students, and we often want to understand what topics they worked on, which technologies they used, and how students collaborated. Instead of going through each repository manually, this tool gives us a way to collect all the relevant information and view it in a searchable, visual format.

This project uses PostgreSQL for storing data and searching through it using full-text search. We also show how students, libraries, and project topics are connected using an interactive graph.

---

## Goals and Features

- Store all project metadata in one place
- Extract project titles, team members, libraries used, and key phrases from READMEs
- Normalize and clean text for better searching
- Use PostgreSQL full-text search to quickly find relevant projects
- Let users search by keyword, year, library, or other fields
- Visualize connections between students and the tools they used with a network graph
- Configure everything using a single config file

---

## Project Structure

```
project-root/
│
├── data/
│   ├── semesters.csv             # GitHub URLs to fetch forks
│   └── projects.json             # Optional local backup of all parsed data
│
├── config.yaml                   # Main config file for the whole project
│
├── app.py                        # Main Streamlit app
│
├── scripts/
│   └── generate_project_metadata.py  # Creates the database snapshot
│
├── project_utils/
│   ├── github_utils.py           # For cloning GitHub repos
│   ├── readme_parser.py          # Parses titles, team names, and descriptions
│   ├── preprocess.py             # Cleans and normalizes text
│   ├── search_utils.py           # Handles database search queries
│   ├── graph_utils.py            # Creates network graphs
│   ├── embedding_utils.py        # (Optional) For semantic search with vectors
│   ├── db_setup.sql              # SQL commands to create tables and indexes
│   └── logger_setup.py           # Logging setup
│
└── README.md                     # This file
```

---

---

## How Each Part Works

### Step 1: Clone GitHub Repos (One-Time)
We perform a one-time shallow clone of each GitHub repository using sparse checkout. Only critical files like `README.md` and `.py` files are downloaded. This helps reduce clone time and avoids unnecessary content like media, tests, or data files. This behavior is driven by configuration and can be adapted to include/exclude more file types in the future.

### Step 2: Parse README and Extract Metadata
We parse the README files to extract structured fields such as:
- Project title
- Team members
- Key descriptive sections like simulation descriptions

These fields are configurable via `config.yaml`, meaning you can choose which sections to extract without modifying the core logic. For example, if you want to focus on "purpose" or "dataset" sections instead, the parser logic can be reused by adjusting config values.

We also scan Python files to detect which libraries were used in the project. This gives insights into trends in tooling, frameworks, or analysis libraries across different semesters.

### Step 3: Preprocess Text for Search
To prepare for effective full-text search, we normalize the extracted fields by:
- Lowercasing all text
- Removing common stopwords
- Stripping punctuation and excess whitespace
- Combining important fields into a single `search_blob`

This ensures consistent and accurate search performance without user errors from formatting.

### Step 4: Store Structured Data in PostgreSQL
The parsed and cleaned data is stored in a PostgreSQL database. We create a full-text index column called `search_vector` using `to_tsvector()`, and apply a GIN index to accelerate keyword matching. This makes the data fast to query using SQL-based search expressions.

### Step 5: Perform Full-Text Search
We query the indexed database using `to_tsquery()` to retrieve relevant projects. These results are matched by ranked relevance, and allow for keyword-based or boolean search expressions. Results are returned to the frontend as dictionaries or DataFrames.

### Step 6: Generate a Network Graph
For each set of search results, we build a network graph that maps out:
- Which students worked on which projects
- Which libraries were used in those projects
- How multiple projects might be related by shared contributors or libraries

This provides a visual summary of collaboration and tool usage across semesters.

### Step 7: Explore Results via Streamlit
We expose everything through a Streamlit app that lets users:
- Search for keywords and filter by semester or year
- View tabular results and metadata for each project
- Explore connections between students and projects using an interactive graph

This frontend makes the system accessible to non-technical users like instructors or reviewers who want a quick understanding of classroom projects.

---

## Configuration: `config.yaml`

All the behavior of this system, such as which fields to extract, how many lines of the README to scan, and how to connect to PostgreSQL, is controlled through the config.yaml file. This file makes it easy to customize what gets parsed, stored, and visualized without needing to touch the code.

---

## How to Run the Project

### 1. Clone and Parse Repos
```
python scripts/generate_project_metadata.py --config config.yaml
```
This will clone the repos, parse the data, and insert it into your local PostgreSQL database.

### 2. Set Up PostgreSQL
```
psql -U postgres -d projectdb -f project_utils/db_setup.sql
```
Make sure the username and password match your config file.

### 3. Launch the Streamlit App
```
streamlit run app.py
```
You’ll be able to search and view the graph through your browser.

---

## Example Searches

- **monte carlo** – shows simulation projects
- **queue** – shows models involving wait lines
- **graph** – shows projects using NetworkX or PyVis
- You can also filter by year or semester in the app

---

## Why PostgreSQL and Network Graphs?

PostgreSQL gives us a powerful, production-grade way to index and query large text fields with high performance. It’s structured, reliable, and supports flexible search logic through full-text search extensions. By using GIN indexes on `tsvector` fields, search results are instant even for large project sets.

Network graphs complement this by surfacing collaboration and technology trends. Instead of reading through every record manually, users can visually spot clusters of students, overlapping tools, and cross-semester themes.

Together, they combine the power of data processing with the clarity of visual storytelling.

---

## Ideas for the Future

- Add support for semantic search with vector embeddings
- Add filters by number of libraries or lines of code
- Export results to Excel or CSV
- Track trends in library usage across semesters

---

## Final Note

This project gives us a structured and visual way to explore classroom project data from GitHub. It’s easy to reuse and extend, and everything can be done offline after the first run. Feel free to suggest features or improvements!
