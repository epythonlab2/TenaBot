# api/rag_api.py

from typing import List, Optional

import numpy as np  # Added for type checking
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.rag.retriever import LegalRetriever
from src.utils.logger import get_logger

logger = get_logger("rag_api")

app = FastAPI(title="Ethiopian Legal RAG API", version="1.1.0")

try:
    retriever = LegalRetriever(top_k=5)
except Exception as e:
    logger.error("Retriever initialization failed: %s", e)
    retriever = None


class QueryRequest(BaseModel):
    query: str
    top_k: Optional[int] = 5
    metadata_filter: Optional[dict] = None


class QueryResponse(BaseModel):
    content: str
    metadata: dict
    relevance_score: Optional[float] = None


@app.post("/query", response_model=List[QueryResponse])
async def query_legal_docs(request: QueryRequest):
    if retriever is None:
        raise HTTPException(
            status_code=500, detail="Legal Engine is offline. Check vector store path."
        )

    try:
        logger.info("Processing legal query: '%s'", request.query)

        # This calls your Reranked retrieval logic
        results = retriever.retrieve(query=request.query)

        if not results:
            return []

        response = []
        for doc in results:
            # 1. Extract the raw score
            raw_score = doc.metadata.get("relevance_score")

            # 2. FIX: Convert NumPy types to Python native types
            # Flashrank often returns np.float32 which Pydantic can't serialize
            clean_score = float(raw_score) if raw_score is not None else None

            # 3. Double-check metadata for any other numpy types just in case
            safe_metadata = {
                k: (v.item() if isinstance(v, np.generic) else v)
                for k, v in doc.metadata.items()
            }

            response.append(
                QueryResponse(
                    content=doc.page_content,
                    metadata=safe_metadata,
                    relevance_score=clean_score,
                )
            )

        return response

    except Exception as e:
        logger.error("API Retrieval Error: %s", str(e))
        raise HTTPException(
            status_code=500, detail=f"Internal processing error: {str(e)}"
        )


@app.get("/health")
def health_check():
    return {"status": "ready" if retriever else "error"}
