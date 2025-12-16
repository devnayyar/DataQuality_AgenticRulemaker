# memory/faiss_store.py (FAISS RAG for Feedback)
import faiss
import numpy as np
import json
import os
from sentence_transformers import SentenceTransformer
MODEL = SentenceTransformer("all-MiniLM-L6-v2")
DIM = 384
INDEX_PATH = "data/memory/faiss.index"
META_PATH = "data/memory/metadata.json"
class FeedbackRAG:
    def __init__(self):
        self.model = MODEL
        os.makedirs("data/memory", exist_ok=True)
        if os.path.exists(INDEX_PATH):
            self.index = faiss.read_index(INDEX_PATH)
        else:
            self.index = faiss.IndexFlatL2(DIM)
        self.metadata = json.load(open(META_PATH)) if os.path.exists(META_PATH) else {}
    def add_feedback(self, text: str, table_name: str, decision: str, rules: list):
        emb = self.model.encode([text]).astype("float32")
        self.index.add(emb)
        idx = self.index.ntotal - 1
        self.metadata[str(idx)] = {
            "text": text,
            "table": table_name,
            "decision": decision,
            "rules": rules
        }
        self.save()
    def search(self, query: str, k: int = 5):
        if self.index.ntotal == 0:
            return []
        q_emb = self.model.encode([query]).astype("float32")
        distances, ids = self.index.search(q_emb, k)
        results = []
        for d, i in zip(distances[0], ids[0]):
            if i != -1 and str(i) in self.metadata:
                results.append({"distance": float(d), **self.metadata[str(i)]})
        return results
    def save(self):
        faiss.write_index(self.index, INDEX_PATH)
        with open(META_PATH, "w") as f:
            json.dump(self.metadata, f, indent=2)
rag = FeedbackRAG()