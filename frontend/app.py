import streamlit as st
import asyncio
from typing import Dict, Any
from frontend.query_controller import query_controller
from backend.server import get_schema
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

st.set_page_config(
    page_title="DB-gpt Database Query System",
    page_icon="🔍",
    layout="wide"
)

st.markdown("""
    <style>
    .stTextInput input {
        font-size: 16px;
    }
    .stButton button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
        font-weight: bold;
    }
    .stButton button:hover {
        background-color: #45a049;
    }
    .result-table {
        margin-top: 20px;
    }
    .schema-container {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 20px;
    }
    .error-message {
        color: #ff4b4b;
        font-weight: bold;
    }
    .page-nav {
        display: flex;
        justify-content: center;
        align-items: center;
        margin: 15px 0;
    }
    .page-info {
        margin: 0 20px;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

async def run_query(database: str, query: str) -> Dict[str, Any]:
    result = await query_controller(database, query)
    return result


async def run_get_schema(database: str, table_name: str = None) -> Dict[str, Any]:
    return await get_schema(database, table_name)


def display_paginated_results(df: pd.DataFrame, page_size=15):
    """Display paginated query results"""
    if 'page_number' not in st.session_state:
        st.session_state.page_number = 1

    total_pages = max(1, (len(df) - 1) // page_size + 1)
    current_page = min(st.session_state.page_number, total_pages)

    start_idx = (current_page - 1) * page_size
    end_idx = min(start_idx + page_size, len(df))
    st.dataframe(df.iloc[start_idx:end_idx], use_container_width=True)

    col1, col2, col3 = st.columns([1, 4, 1])

    with col1:
        if st.button("Previous", disabled=(current_page == 1)):
            st.session_state.page_number = current_page - 1
            st.rerun()

    with col2:
        st.markdown(
            f"<div style='text-align: center; margin-top: 0.5rem;'>"
            f"Page {current_page} of {total_pages} · {len(df)} records"
            f"</div>",
            unsafe_allow_html=True
        )

    with col3:
        if st.button("Next", disabled=(current_page == total_pages)):
            st.session_state.page_number = current_page + 1
            st.rerun()

    jump_col1, jump_col2 = st.columns([2, 3])
    with jump_col1:
        st.write("")  
    with jump_col2:
        page_input = st.number_input(
            "Go to page",
            min_value=1,
            max_value=total_pages,
            value=current_page,
            step=1
        )
        if st.button("Jump"):
            if 1 <= page_input <= total_pages:
                st.session_state.page_number = page_input
                st.rerun()


def main():
    st.title("DB-gpt Database Query System")

    if 'generated_sql' not in st.session_state:
        st.session_state.generated_sql = None
    if 'query_result' not in st.session_state:
        st.session_state.query_result = None
    if 'page_number' not in st.session_state:
        st.session_state.page_number = 1

    with st.sidebar:
        st.header("Database Settings")
        default_db = os.getenv('MYSQL_DATABASE', 'college')
        database = st.text_input("Database Name", value=default_db)

        st.markdown("---")
        st.header("Feature Navigation")
        show_schema = st.checkbox("Show Table Schema")
        if show_schema:
            table_name = st.text_input("Enter Table Name (Optional)", help="Leave empty to show all tables")

        st.markdown("---")
        st.header("Pagination Settings")
        page_size = st.selectbox("Rows per page", [10, 15, 20, 30, 50], index=1)

    # Main content area
    st.subheader("Natural Language Query")
    query = st.text_area(
        "Enter your query in natural language",
        placeholder="E.g., show all students in the history department",
        height=100,
        key="user_query"
    )

    col1, col2 = st.columns([1, 3])
    with col1:
        run_query_btn = st.button("Run Query", use_container_width=True, key="run_query_btn")
    with col2:
        if st.button("Clear Results", use_container_width=True, key="clear_results"):
            st.session_state.generated_sql = None
            st.session_state.query_result = None
            st.session_state.page_number = 1
            st.rerun()

    if show_schema:
        with st.spinner("Fetching table schema..."):
            schema_result = asyncio.run(run_get_schema(database, table_name if table_name else None))

        if 'error' in schema_result:
            st.error(f"Failed to get schema: {schema_result['error']}")
        else:
            st.subheader("Database Table Schema")
            for table, columns in schema_result['schema'].items():
                with st.expander(f"Table: {table}"):
                    df = pd.DataFrame(columns)
                    st.dataframe(df, use_container_width=True)

    if run_query_btn and query:
        st.session_state.page_number = 1  
        with st.spinner("Processing your query..."):
            result = asyncio.run(run_query(database, query))
            st.session_state.query_result = result
            if 'generated_sql' in result:
                st.session_state.generated_sql = result['generated_sql']

    if st.session_state.generated_sql:
        st.subheader("Generated SQL")
        st.code(st.session_state.generated_sql, language="sql")

    if st.session_state.query_result:
        if 'error' in st.session_state.query_result:
            st.error(f"Query Error: {st.session_state.query_result['error']}")
        elif 'results' in st.session_state.query_result:
            if isinstance(st.session_state.query_result['results'], list):
                st.subheader("Query Results")
                df = pd.DataFrame(st.session_state.query_result['results'])

                display_paginated_results(df, page_size=page_size)

                st.download_button(
                    label="Export All Data as CSV",
                    data=df.to_csv(index=False).encode('utf-8'),
                    file_name='query_results.csv',
                    mime='text/csv',
                    key="export_csv"
                )
            else:
                st.success(f"Operation Successful: {st.session_state.query_result['results']}")


if __name__ == "__main__":
    main()
