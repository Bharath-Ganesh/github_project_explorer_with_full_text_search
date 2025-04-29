import math
import streamlit as st
from project_utils.starter_class import setup_logger, get_logger, build_context
from src.dao import ProjectsDAO
from src.service import ProjectService
from src.renderer import Renderer, UIConfig

# ────────────────────────────────────────────────────────────────
#  Global logging setup
# ────────────────────────────────────────────────────────────────
setup_logger()                                 # configure file + console logging
logger = get_logger(__name__)                  # module‐level logger

def main():
    logger.info(" Starting Final Projects Explorer app")

    # ────────────────────────────────────────────────────────────────
    # 1) Page config + header
    # ────────────────────────────────────────────────────────────────
    ctx = build_context(__name__)
    UIConfig(ctx).apply()
    logger.info("Applied UIConfig: title=%r, layout=%r",
                ctx.get_app_ui().get("title"),
                ctx.get_app_ui().get("layout"))

    # ────────────────────────────────────────────────────────────────
    # 2) Sidebar filters
    # ────────────────────────────────────────────────────────────────
    st.sidebar.header("Filters")
    filters_cfg = ctx.get_filters()
    logger.info("Available filters: %s", list(filters_cfg.keys()))

    filter_inputs = {}
    for alias, meta in filters_cfg.items():
        label = meta["label"]
        if meta["type"] == "dropdown":
            val = st.sidebar.selectbox(label, meta["options"])
        else:
            val = st.sidebar.text_input(label)
        filter_inputs[alias] = val
    logger.info("User selected filters: %s", filter_inputs)

    # ────────────────────────────────────────────────────────────────
    # 3) Fetch all matching rows
    # ────────────────────────────────────────────────────────────────
    dao     = ProjectsDAO()
    service = ProjectService(dao, ctx._config)
    all_results = service.fetch_projects(filter_inputs, ctx.get_display_columns())
    logger.info("Fetched %d projects from database", len(all_results))

    # ────────────────────────────────────────────────────────────────
    # 4) Early exit if no results
    # ────────────────────────────────────────────────────────────────
    if not all_results:
        logger.info("No projects matched filters, aborting render")
        st.warning("No projects found matching your filters. Try broadening or clearing them.")
        return

    # ────────────────────────────────────────────────────────────────
    # 5) Display total
    # ────────────────────────────────────────────────────────────────
    total = len(all_results)
    st.markdown(f"** Search results:** {total} projects")
    logger.info("Displaying total count: %d", total)

    # ────────────────────────────────────────────────────────────────
    # 6) Pagination setup
    # ────────────────────────────────────────────────────────────────
    page_size   = ctx.get("pagination.page_size", 10)
    total_pages = max(1, math.ceil(total / page_size))
    if "page" not in st.session_state:
        st.session_state.page = 1

    logger.info("Pagination config: page_size=%d, total_pages=%d",
                page_size, total_pages)

    # compute slice
    page_size = ctx.get("pagination.page_size", 10)
    start = (st.session_state.page - 1) * page_size
    end = start + page_size
    page_results = all_results[start:end]
    logger.info("Page %d selected: rows %d–%d", st.session_state.page, start+1, end)

    # ────────────────────────────────────────────────────────────────
    # 7) Render table
    # ────────────────────────────────────────────────────────────────
    renderer = Renderer(
        fields_cfg           = ctx.get_display_columns(),
        graph_options        = ctx.get("graph_options", {}),
        default_column_width = ctx.get("default_column_width", "250px")
    )
    logger.info("Renderer initialized with fields: %s",
                [f["field"] for f in ctx.get_display_columns()])
    renderer.render_table(page_results)
    logger.info("Rendered %d rows in table", len(page_results))

    # ────────────────────────────────────────────────────────────────
    # 8) Footer: pager controls
    # ────────────────────────────────────────────────────────────────
    st.markdown(f"Showing **{start+1}**–**{end}** of **{total}** projects")

    col_prev, col_mid, col_next = st.columns([1,2,1])
    with col_prev:
        disabled = st.session_state.page <= 1
        if st.button("← Prev", disabled=disabled):
            st.session_state.page -= 1
            logger.info("Prev clicked, new page=%d", st.session_state.page)
    with col_mid:
        st.markdown(f"Page **{st.session_state.page}** of **{total_pages}**")
    with col_next:
        disabled = st.session_state.page >= total_pages
        if st.button("Next →", disabled=disabled):
            st.session_state.page += 1
            logger.info("Next clicked, new page=%d", st.session_state.page)

if __name__ == "__main__":
    main()
