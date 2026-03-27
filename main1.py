"""
First beta version of Nexus. Right now uses pandas to convert the given csv file in to db
then uses sqlite to extract its schema, and then uses a reasoning model like qwen to reiterate
user query from english to english in order for sqlcoder to better get the context

The refined question and the db schema are passed into a function that using requests makes an
request to the local api hosted on the other machine to generate sql code.
This is how it generates SQL commands but we have not implemented anything to 
check the response redo it or execute it.
"""



import sqlite3
import pandas as pd
import requests

# Load CSV
df = pd.read_csv("archive\olist_geolocation_dataset.csv")

# Create DB
conn = sqlite3.connect("olist.db")

# Store table
df.to_sql("geolocation", conn, if_exists="replace", index=False)


def get_schema():
    conn = sqlite3.connect("olist.db")
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(geolocation)")
    columns = cursor.fetchall()

    schema = "Table: geolocation\nColumns:\n"

    for col in columns:
        schema += f"- {col[1]} ({col[2]})\n"

    conn.close()
    return schema

print(get_schema())
schema = get_schema()
print("Database created successfully")


SQL_API = "http://172.168.0.8:1337/v1/chat/completions"  # Laptop A IP
MODEL_NAME = "sqlcoder-7b-q5_k_m.gguf"

QWEN_API = "http://127.0.0.1:1337/v1/chat/completions"
QWEN_MODEL = "Qwen3.5-9B.Q5_K_S.gguf"

def refine_question(question, schema):
    print("Refining Question.")
    prompt = f"""
You are a data analyst assistant.

Your job is to rewrite the user's question into a clear,
precise database query instruction.

Make it:
- specific
- unambiguous
- suitable for SQL generation

Do NOT mention SQL.
Do NOT generate SQL.
Only rewrite the question.

Database schema:
{schema}

User question:
{question}

Return ONLY the improved question.
"""

    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3
    })

    data = response.json()
    return data['choices'][0]['message']['content']

def generate_sql(question, schema):
    prompt = f"""
You are an expert SQL generator.

Database schema:
{schema}

Rules:
- Use only given columns
- Do not hallucinate columns
- Use SQLite syntax
- Return ONLY SQL query (no explanation)

Question:
{question}
"""

    response = requests.post(
        SQL_API,
        json={
            "model": MODEL_NAME,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "temperature": 0
        },
        headers={
            "Host": "localhost"
        }
    )

    print("Raw response:", response.text)
    data = response.json()
    return data['choices'][0]['message']['content']

refined = refine_question("How many columns are there in the database.", schema)

print("\nRefined Question:\n", refined)

sql_query = generate_sql(refined, schema)

print("\nGenerated SQL:\n", sql_query)