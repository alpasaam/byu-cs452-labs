## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
CONNECTION = os.getenv("CONNECTION")


def run_query(cursor, query, title):
    cursor.execute(query)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    print(f"\n--- {title} ---")
    print("Query:", query.strip())
    print("Columns:", ", ".join(cols))
    for row in rows:
        print(row)
    return rows


def main():
    conn = psycopg2.connect(CONNECTION)
    cur = conn.cursor()

    question1 = 'Q1) What are the five most similar segments to segment "267:476"'
    query1 = """
    WITH q AS (SELECT embedding FROM podcast_segment WHERE id = '267:476')
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time,
           (ps.embedding <-> (SELECT embedding FROM q)) AS distance
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id
    WHERE ps.id != '267:476'
    ORDER BY ps.embedding <-> (SELECT embedding FROM q)
    LIMIT 5
    """
    run_query(cur, query1, question1)

    question2 = 'Q2) What are the five most dissimilar segments to segment "267:476"'
    query2 = """
    WITH q AS (SELECT embedding FROM podcast_segment WHERE id = '267:476')
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time,
           (ps.embedding <-> (SELECT embedding FROM q)) AS distance
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id
    WHERE ps.id != '267:476'
    ORDER BY ps.embedding <-> (SELECT embedding FROM q) DESC
    LIMIT 5
    """
    run_query(cur, query2, question2)

    question3 = "Q3) What are the five most similar segments to segment '48:511'"
    query3 = """
    WITH q AS (SELECT embedding FROM podcast_segment WHERE id = '48:511')
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time,
           (ps.embedding <-> (SELECT embedding FROM q)) AS distance
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id
    WHERE ps.id != '48:511'
    ORDER BY ps.embedding <-> (SELECT embedding FROM q)
    LIMIT 5
    """
    run_query(cur, query3, question3)

    question4 = "Q4) What are the five most similar segments to segment '51:56'"
    query4 = """
    WITH q AS (SELECT embedding FROM podcast_segment WHERE id = '51:56')
    SELECT p.title, ps.id, ps.content, ps.start_time, ps.end_time,
           (ps.embedding <-> (SELECT embedding FROM q)) AS distance
    FROM podcast_segment ps
    JOIN podcast p ON ps.podcast_id = p.id
    WHERE ps.id != '51:56'
    ORDER BY ps.embedding <-> (SELECT embedding FROM q)
    LIMIT 5
    """
    run_query(cur, query4, question4)

    question5a = 'Q5) For each of the following podcast segments, find the five most similar podcast episodes. a) Segment "267:476"'
    query5a = """
    WITH seg AS (SELECT embedding, podcast_id FROM podcast_segment WHERE id = '267:476'),
         ep_avg AS (
           SELECT podcast_id, AVG(embedding) AS avg_emb
           FROM podcast_segment
           GROUP BY podcast_id
         )
    SELECT p.title, (ea.avg_emb <-> (SELECT embedding FROM seg)) AS distance
    FROM ep_avg ea
    JOIN podcast p ON p.id = ea.podcast_id
    WHERE ea.podcast_id != (SELECT podcast_id FROM seg)
    ORDER BY ea.avg_emb <-> (SELECT embedding FROM seg)
    LIMIT 5
    """
    run_query(cur, query5a, question5a)

    question5b = "Q5) For each of the following podcast segments, find the five most similar podcast episodes. b) Segment '48:511'"
    query5b = """
    WITH seg AS (SELECT embedding, podcast_id FROM podcast_segment WHERE id = '48:511'),
         ep_avg AS (
           SELECT podcast_id, AVG(embedding) AS avg_emb
           FROM podcast_segment
           GROUP BY podcast_id
         )
    SELECT p.title, (ea.avg_emb <-> (SELECT embedding FROM seg)) AS distance
    FROM ep_avg ea
    JOIN podcast p ON p.id = ea.podcast_id
    WHERE ea.podcast_id != (SELECT podcast_id FROM seg)
    ORDER BY ea.avg_emb <-> (SELECT embedding FROM seg)
    LIMIT 5
    """
    run_query(cur, query5b, question5b)

    question5c = "Q5) For each of the following podcast segments, find the five most similar podcast episodes. c) Segment '51:56'"
    query5c = """
    WITH seg AS (SELECT embedding, podcast_id FROM podcast_segment WHERE id = '51:56'),
         ep_avg AS (
           SELECT podcast_id, AVG(embedding) AS avg_emb
           FROM podcast_segment
           GROUP BY podcast_id
         )
    SELECT p.title, (ea.avg_emb <-> (SELECT embedding FROM seg)) AS distance
    FROM ep_avg ea
    JOIN podcast p ON p.id = ea.podcast_id
    WHERE ea.podcast_id != (SELECT podcast_id FROM seg)
    ORDER BY ea.avg_emb <-> (SELECT embedding FROM seg)
    LIMIT 5
    """
    run_query(cur, query5c, question5c)

    question6 = "Q6) For podcast episode id = VeH7qKZrOWI, find the five most similar podcast episodes."
    query6 = """
    WITH target AS (
      SELECT AVG(embedding) AS avg_emb FROM podcast_segment WHERE podcast_id = 'VeH7qKZr0WI'
    ),
    ep_avg AS (
      SELECT podcast_id, AVG(embedding) AS avg_emb
      FROM podcast_segment
      GROUP BY podcast_id
    )
    SELECT p.title, (ea.avg_emb <-> (SELECT avg_emb FROM target)) AS distance
    FROM ep_avg ea
    JOIN podcast p ON p.id = ea.podcast_id
    WHERE ea.podcast_id != 'VeH7qKZr0WI'
    ORDER BY ea.avg_emb <-> (SELECT avg_emb FROM target)
    LIMIT 5
    """
    run_query(cur, query6, question6)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
