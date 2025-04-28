import networkx as nx
from project_utils.starter_class import setup_logger, get_logger

logger = get_logger(__name__)

def generate_graph(projects):
    G = nx.Graph()

    for project in projects:
        proj_node = project["title"]
        G.add_node(proj_node, label=proj_node, color="orange")

        # Add team member nodes
        for member in project.get("team_members", []):
            G.add_node(member, label=member, color="lightblue")
            G.add_edge(member, proj_node)

        # Add library nodes
        for lib in project.get("libraries", []):
            G.add_node(lib, label=lib, color="pink")
            G.add_edge(proj_node, lib)

    logger.info(f"Graph created with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G

if __name__ == '__main__':
    setup_logger()
