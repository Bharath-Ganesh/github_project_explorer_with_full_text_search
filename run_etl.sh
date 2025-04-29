echo "Hi"
# Fetch & sparse-clone all forks
python -m project_utils.github_cloner
python -m project_utils.postgres_uploader