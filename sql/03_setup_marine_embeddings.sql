-- marine_embeddings: chunked narrative vectors (384-dim = all-MiniLM-L6-v2)

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS marine_embeddings (
    id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL REFERENCES marine_documents (id) ON DELETE CASCADE,
    chunk_index INT NOT NULL,
    chunk_text TEXT NOT NULL,
    embedding VECTOR(384) NOT NULL,
    model_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_marine_embeddings_document_id
ON marine_embeddings (document_id);

CREATE INDEX IF NOT EXISTS idx_marine_embeddings_embedding
ON marine_embeddings
USING hnsw (embedding vector_cosine_ops);

SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_name = 'marine_embeddings'
ORDER BY ordinal_position;
