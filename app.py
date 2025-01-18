import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.core import Root
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

def query_cortex_service(session, query, columns=None, filter=None):
    """Query Cortex Search Service."""
    if columns is None:
        columns = ["chunk"]
    if filter is None:
        filter = {}

    db, schema = session.get_current_database(), session.get_current_schema()
    cortex_service_name = st.session_state.get("selected_service", "")
    cortex_service = (
        Root(session)
        .databases[db]
        .schemas[schema]
        .cortex_search_services[cortex_service_name]
    )
    context = cortex_service.search(query, columns=columns, filter=filter)
    return context.results

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
        "account": st.secrets["MIB03883"],
        "user": st.secrets["umairulla05"],
        "password": st.secrets["Newboy@123"],
        "role": st.secrets["ACCOUNTADMIN"],
        "warehouse": st.secrets["medical_insight_analysis_wh"],
        "database": st.secrets["medical_insight_analysis_db"],
        "schema": st.secrets["PUBLIC"],
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
