import streamlit as st
from snowflake.snowpark.session import Session

from snowflake.cortex import Complete

# Constants
MODELS = ["mistral-large2"]

def init_messages():
    """Initialize chat messages in session state."""
    if "messages" not in st.session_state:
        st.session_state.messages = []

def init_service_metadata(session):
    """Initialize service metadata from Snowflake Cortex."""
    if "service_metadata" not in st.session_state:
        services = session.sql("SHOW CORTEX SEARCH SERVICES;").collect()
        metadata = []
        for service in services:
            svc_name = service["name"]
            search_col = session.sql(
                f"DESC CORTEX SEARCH SERVICE {svc_name};"
            ).collect()[0]["search_column"]
            metadata.append({"name": svc_name, "search_column": search_col})
        st.session_state.service_metadata = metadata

def query_cortex_search_service(session, service_name, query, columns=None, filter=None):
    """
    Query a Cortex Search Service directly using Snowflake SQL.
    """
    if columns is None:
        columns = ["chunk"]
    if filter is None:
        filter = {}

    # Construct a search query
    search_query = f"""
    SELECT *
    FROM CORTEX_SEARCH({service_name}, '{query}', LIMIT {st.session_state.num_retrieved_chunks});
    """
    results = session.sql(search_query).collect()
    return results


def complete(model, prompt):
    """Generate text using the selected model."""
    return Complete(model, prompt).replace("$", "\$")

def app():
    """Main Streamlit app."""
    st.title(":speech_balloon: Chatbot with Snowflake Cortex")

    # Initialize session state variables
    init_messages()

    # Snowflake connection
    connection_params = {
        "account": st.secrets["snowflake_account"],
        "user": st.secrets["snowflake_user"],
        "password": st.secrets["snowflake_password"],
        "role": st.secrets["snowflake_role"],
        "warehouse": st.secrets["snowflake_warehouse"],
        "database": st.secrets["snowflake_database"],
        "schema": st.secrets["snowflake_schema"],
    }
    session = Session.builder.configs(connection_params).create()

    init_service_metadata(session)

    st.sidebar.title("Configuration")
    selected_service = st.sidebar.selectbox(
        "Select Cortex Service",
        [s["name"] for s in st.session_state.service_metadata]
    )
    st.session_state["selected_service"] = selected_service

    st.sidebar.text("Snowflake Connection Active")
    st.sidebar.button("Clear Messages", on_click=lambda: st.session_state.messages.clear())

    # Chat functionality
    question = st.text_input("Ask a question:")
    if question:
        st.session_state.messages.append({"role": "user", "content": question})

        # Display messages
        st.write("### Chat History")
        for msg in st.session_state.messages:
            if msg["role"] == "user":
                st.markdown(f"**User:** {msg['content']}")
            else:
                st.markdown(f"**Assistant:** {msg['content']}")

        # Query and generate response
        prompt = f"You asked: {question}"
        response = complete(MODELS[0], prompt)

        st.session_state.messages.append({"role": "assistant", "content": response})
        st.markdown(f"**Assistant:** {response}")

if __name__ == "__main__":
    app()
