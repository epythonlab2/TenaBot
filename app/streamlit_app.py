import os
from typing import Any, Dict, List

import requests
import streamlit as st

# --------------------------------------------------
# Configuration
# --------------------------------------------------

API_URL = os.getenv("LEGAL_API_URL", "http://127.0.0.1:8000/query")
REQUEST_TIMEOUT = 120
TOP_K = 5

session = requests.Session()


# --------------------------------------------------
# Page Setup
# --------------------------------------------------

st.set_page_config(
    page_title="Ethiopian Legal Assistant", page_icon="⚖️", layout="centered"
)

st.title("⚖️ Ethiopian Legal Assistant")
st.caption("Ask questions about Ethiopian law")

st.markdown(
    """
    <style>
    .block-container {
        max-width: 800px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------
# Session State
# --------------------------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []


# --------------------------------------------------
# Sidebar
# --------------------------------------------------

with st.sidebar:

    st.header("Settings")

    top_k = st.slider("Number of legal sources", min_value=1, max_value=10, value=TOP_K)

    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.rerun()


# --------------------------------------------------
# API Client
# --------------------------------------------------


def fetch_legal_results(query: str, top_k: int) -> List[Dict[str, Any]]:

    payload = {"query": query, "top_k": top_k}

    try:

        response = session.post(API_URL, json=payload, timeout=REQUEST_TIMEOUT)

        response.raise_for_status()

        data = response.json()

        if not isinstance(data, list):
            raise RuntimeError("Invalid API response format")

        return data

    except requests.exceptions.Timeout:
        raise RuntimeError("⏱️ The legal search service timed out.")

    except requests.exceptions.ConnectionError:
        raise RuntimeError("🚨 Cannot connect to the legal API.")

    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(f"❌ API error {exc.response.status_code}")

    except ValueError:
        raise RuntimeError("⚠️ Invalid response from API.")


# --------------------------------------------------
# Format Answer
# --------------------------------------------------


def build_answer(results: List[Dict[str, Any]]) -> str:

    if not results:
        return "⚠️ ይቅርታ፣ ከጥያቄዎ ጋር የሚዛመድ ህግ አልተገኘም።"

    results = sorted(results, key=lambda x: x.get("relevance_score", 0), reverse=True)

    answer = []

    for doc in results:

        content = doc.get("content", "").strip()
        meta = doc.get("metadata", {}) or {}

        article = meta.get("article", "—")
        law_name = meta.get("law_name", "Unknown Law")
        page = meta.get("page_number", "—")

        score = doc.get("relevance_score", 0)
        score_pct = f"{score*100:.1f}%"

        block = f"""
        ### 📜 {law_name}
        **አንቀጽ:** {article}
        **ገጽ:** {page}
        **Relevance:** {score_pct}
        {content}
        ---
        """

        answer.append(block)

    return "\n".join(answer)


# --------------------------------------------------
# Display Chat History
# --------------------------------------------------

for message in st.session_state.messages:

    with st.chat_message(message["role"]):
        st.markdown(message["content"])


# --------------------------------------------------
# User Input
# --------------------------------------------------

prompt = st.chat_input("Ask a question about Ethiopian law...")

if prompt:

    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):

        with st.spinner("Searching Ethiopian law..."):

            try:

                results = fetch_legal_results(prompt, top_k)

                answer = build_answer(results)

                placeholder = st.empty()

                # simulate streaming response
                displayed = ""
                for chunk in answer.split("\n"):
                    displayed += chunk + "\n"
                    placeholder.markdown(displayed)

                st.session_state.messages.append(
                    {"role": "assistant", "content": answer}
                )

            except RuntimeError as err:

                st.error(str(err))

                st.session_state.messages.append(
                    {"role": "assistant", "content": str(err)}
                )

            except Exception:

                error_msg = "⚠️ Unexpected system error."

                st.error(error_msg)

                st.session_state.messages.append(
                    {"role": "assistant", "content": error_msg}
                )
