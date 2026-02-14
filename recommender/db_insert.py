## This script is used to insert data into the database
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd

from utils import fast_pg_insert

load_dotenv()
CONNECTION = os.getenv("CONNECTION")

# TODO: Read the embedding files
# TODO: Read documents files
# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
ds = load_dataset("Whispering-GPT/lex-fridman-podcast")

DATA_DIR = Path(__file__).parent / "data"
DOCUMENTS_DIR = DATA_DIR / "documents"
EMBEDDING_DIR = DATA_DIR / "embedding"


def load_documents_and_embeddings():
    segments = []
    podcasts_dict = {}

    for doc_path in DOCUMENTS_DIR.glob("batch_request_*.jsonl"):
        emb_path = EMBEDDING_DIR / doc_path.name.replace("batch_request_", "")
        if not emb_path.exists():
            continue

        emb_by_id = {}
        with open(emb_path) as f:
            for line in f:
                obj = json.loads(line)
                cid = obj.get("custom_id")
                if cid and obj.get("response", {}).get("body", {}).get("data"):
                    emb = obj["response"]["body"]["data"][0]["embedding"]
                    emb_by_id[cid] = emb

        with open(doc_path) as f:
            for line in f:
                obj = json.loads(line)
                cid = obj.get("custom_id")
                if cid not in emb_by_id:
                    continue
                body = obj.get("body", {})
                meta = body.get("metadata", {})
                podcast_id = meta.get("podcast_id")
                title = meta.get("title", "")
                start = meta.get("start_time")
                stop = meta.get("stop_time")
                content = body.get("input", "")

                if not podcast_id:
                    continue
                podcasts_dict[podcast_id] = title
                segments.append({
                    "id": cid,
                    "start_time": start,
                    "end_time": stop,
                    "content": content,
                    "embedding": emb_by_id[cid],
                    "podcast_id": podcast_id,
                })

    return podcasts_dict, segments


podcasts_dict, segments = load_documents_and_embeddings()

podcast_df = pd.DataFrame([{"id": pid, "title": title} for pid, title in podcasts_dict.items()])

segment_df = pd.DataFrame(segments)
segment_df["embedding"] = segment_df["embedding"].apply(lambda v: "[" + ",".join(str(x) for x in v) + "]")
segment_df = segment_df[["id", "start_time", "end_time", "content", "embedding", "podcast_id"]]

# TODO: Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time
fast_pg_insert(podcast_df, CONNECTION, "podcast", ["id", "title"])
fast_pg_insert(segment_df, CONNECTION, "podcast_segment", ["id", "start_time", "end_time", "content", "embedding", "podcast_id"])
