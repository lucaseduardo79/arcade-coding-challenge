"""Streamlit chat UI for the financial query system."""

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="CSE Financial Query System",
    page_icon="💬",
    layout="wide",
)

st.title("CSE Financial Query System")
st.caption("Ask questions about Dipped Products (DIPD) & Richard Pieris Exports (REXP) financial data")

# Initialize agent
if "agent" not in st.session_state:
    from src.agent import FinancialQueryAgent
    st.session_state.agent = FinancialQueryAgent()

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Sidebar
with st.sidebar:
    st.header("About")
    st.markdown(
        "This system uses an LLM to translate your natural language questions "
        "into SQL queries against the financial database.\n\n"
        "**Companies:** DIPD, REXP\n\n"
        "**Data:** Quarterly P&L statements\n\n"
        "**Units:** LKR Thousands"
    )

    st.divider()
    st.header("Sample Questions")
    sample_questions = [
        "What was DIPD's revenue in the latest quarter?",
        "Compare the net income of both companies over the last 3 years",
        "Which company has higher gross profit margins?",
        "Show me the quarterly revenue trend for REXP",
        "What is the year-over-year growth in revenue for DIPD?",
        "What are the operating expenses as a percentage of revenue?",
    ]
    for q in sample_questions:
        if st.button(q, key=f"sample_{q[:20]}", use_container_width=True):
            st.session_state.pending_question = q

    st.divider()
    if st.button("Clear Conversation", use_container_width=True):
        st.session_state.chat_history = []
        st.session_state.agent.reset()
        st.rerun()

# Display chat history
for entry in st.session_state.chat_history:
    with st.chat_message(entry["role"]):
        st.markdown(entry["content"])

# Handle pending question from sidebar
if "pending_question" in st.session_state:
    user_input = st.session_state.pending_question
    del st.session_state.pending_question
else:
    user_input = st.chat_input("Ask a question about the financial data...")

if user_input:
    # Show user message
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # Get agent response
    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            response = st.session_state.agent.query(user_input)
        st.markdown(response)

    st.session_state.chat_history.append({"role": "assistant", "content": response})
