# embeddings/vector_builder.py

from pathlib import Path
from typing import List

from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_huggingface import HuggingFaceEmbeddings

from src.utils.logger import get_logger

logger = get_logger("vector_builder")

VECTOR_STORE_DIR = Path("vector_store")
EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
MODEL_KWARGS = {"truncation": True, "max_length": 512}


def get_embedding_model() -> HuggingFaceEmbeddings:
    """Load the embedding model."""
    logger.info("Loading embedding model: %s", EMBEDDING_MODEL)
    return HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)


def build_vector_store(chunks: List[Document]) -> None:
    if not chunks:
        logger.warning("No chunks provided")
        return

    # 1. Clean chunks before embedding
    # Sometimes OCR adds weird characters like \x0c (page breaks)
    # that confuse the embedding model.
    for chunk in chunks:
        chunk.page_content = chunk.page_content.replace("\x0c", " ")

        # Ensure 'law_name' exists so FAISS doesn't crash on nulls
        if "law_name" not in chunk.metadata:
            chunk.metadata["law_name"] = "Unknown Proclamation"

    embedding_model = get_embedding_model()

    logger.info("Creating FAISS index for %d chunks...", len(chunks))

    # 2. Use 'from_documents' which handles metadata automatically
    vector_db = FAISS.from_documents(chunks, embedding_model)

    VECTOR_STORE_DIR.mkdir(parents=True, exist_ok=True)
    vector_db.save_local(str(VECTOR_STORE_DIR))
    logger.info("Vector store successfully saved.")
