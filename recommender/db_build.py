## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = os.getenv("CONNECTION")

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"

CREATE_PODCAST_TABLE = """
CREATE TABLE IF NOT EXISTS podcast (
    id TEXT PRIMARY KEY,
    title TEXT
)
"""

CREATE_SEGMENT_TABLE = """
CREATE TABLE IF NOT EXISTS podcast_segment (
    id TEXT PRIMARY KEY,
    start_time FLOAT,
    end_time FLOAT,
    content TEXT,
    embedding vector(128),
    podcast_id TEXT REFERENCES podcast(id)
)
"""

conn = psycopg2.connect(CONNECTION)
try:
    with conn.cursor() as cursor:
        cursor.execute(CREATE_EXTENSION)
        cursor.execute(CREATE_PODCAST_TABLE)
        cursor.execute(CREATE_SEGMENT_TABLE)
    conn.commit()
finally:
    conn.close()
