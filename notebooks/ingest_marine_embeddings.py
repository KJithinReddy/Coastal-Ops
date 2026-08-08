"""
Ingest marine_documents → marine_embeddings via psycopg2 + sentence-transformers.

Same pattern as HW2 weather embeddings. Do NOT use Spark JDBC writes against Lakebase
for vectors — use this script (or the companion .ipynb) after /marine/sync.

Usage:
    python notebooks/ingest_marine_embeddings.py
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
from pathlib import Path


def _project_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        cwd = Path.cwd().resolve()
        for candidate in (cwd, cwd.parent, *cwd.parents):
            if (candidate / "dashboard" / "lakebase.py").exists():
                return candidate
            if (candidate / "lakebase.py").exists():
                return candidate
        return cwd.parent if cwd.name == "notebooks" else cwd


_ROOT = _project_root()
_DASHBOARD = _ROOT / "dashboard"
for _path in (_DASHBOARD, _ROOT):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

os.environ.setdefault("HF_HOME", "/tmp/.cache/huggingface")
os.environ.setdefault("TRANSFORMERS_CACHE", "/tmp/.cache/huggingface")
os.environ.setdefault("HF_HUB_CACHE", "/tmp/.cache/huggingface")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
Path("/tmp/.cache/huggingface").mkdir(parents=True, exist_ok=True)

from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer

import lakebase

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest-marine")

DOCS_TABLE = os.environ.get("MARINE_DOCS_TABLE", "marine_documents")
EMB_TABLE = os.environ.get("MARINE_EMB_TABLE", "marine_embeddings")
MODEL_NAME = os.environ.get(
    "MARINE_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
)
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "800"))
CHUNK_OVERLAP = int(os.environ.get("CHUNK_OVERLAP", "100"))
BATCH_SIZE = int(os.environ.get("EMBED_BATCH_SIZE", "64"))


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    chunks: list[str] = []
    start = 0
    step = max(1, chunk_size - overlap)
    while start < len(text):
        end = min(len(text), start + chunk_size)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(text):
            break
        start += step
    return chunks


def embedding_id(document_id: str, chunk_index: int) -> str:
    raw = f"{document_id}:{chunk_index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:40]


def vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


def fetch_unembedded_documents() -> list[dict]:
    return lakebase.run_query(
        f"""
        SELECT d.id, d.narrative_text
        FROM {DOCS_TABLE} d
        LEFT JOIN {EMB_TABLE} e ON e.document_id = d.id
        WHERE e.id IS NULL
          AND d.narrative_text IS NOT NULL
          AND length(trim(d.narrative_text)) > 0
        """
    )


def upsert_embeddings(rows: list[tuple]) -> int:
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {EMB_TABLE}
            (id, document_id, chunk_index, chunk_text, embedding, model_name, created_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            document_id = EXCLUDED.document_id,
            chunk_index = EXCLUDED.chunk_index,
            chunk_text = EXCLUDED.chunk_text,
            embedding = EXCLUDED.embedding,
            model_name = EXCLUDED.model_name,
            created_at = now()
    """
    template = "(%s, %s, %s, %s, %s::vector, %s, now())"

    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, template=template, page_size=BATCH_SIZE)
        conn.commit()
    return len(rows)


def main() -> None:
    lakebase.ensure_marine_documents_table(DOCS_TABLE)
    lakebase.ensure_marine_embeddings_table(EMB_TABLE)

    docs = fetch_unembedded_documents()
    logger.info("Found %d unembedded marine documents", len(docs))
    if not docs:
        logger.info("Nothing to do.")
        return

    logger.info("Loading model %s ...", MODEL_NAME)
    model = SentenceTransformer(
        MODEL_NAME,
        cache_folder=os.environ.get("HF_HOME", "/tmp/.cache/huggingface"),
        device="cpu",
    )

    pending_rows: list[tuple] = []
    total_chunks = 0

    for doc in docs:
        doc_id = doc["id"]
        chunks = chunk_text(doc["narrative_text"])
        if not chunks:
            continue

        vectors = model.encode(chunks, show_progress_bar=False)
        for idx, (chunk, vec) in enumerate(zip(chunks, vectors)):
            pending_rows.append(
                (
                    embedding_id(doc_id, idx),
                    doc_id,
                    idx,
                    chunk,
                    vector_literal(vec.tolist()),
                    MODEL_NAME,
                )
            )
            total_chunks += 1

        if len(pending_rows) >= BATCH_SIZE:
            upsert_embeddings(pending_rows)
            logger.info("Flushed %d embedding rows", len(pending_rows))
            pending_rows = []

    if pending_rows:
        upsert_embeddings(pending_rows)
        logger.info("Flushed final %d embedding rows", len(pending_rows))

    logger.info(
        "Done. Embedded %d documents into %d chunks in %s",
        len(docs),
        total_chunks,
        EMB_TABLE,
    )


_should_run = __name__ == "__main__"
try:
    _should_run = _should_run or get_ipython() is not None  # noqa: F821
except NameError:
    pass

if _should_run:
    main()
