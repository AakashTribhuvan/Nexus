"""In this version I have added many features that make it actually be able to understand and talk to you instead of just generating sql
1)added a function to handle semantic understanding
2)added a function to get sample data from dataset
3)added a function to properly estimate intent and refine the question
4)added the ability to execute the sql
5)validation loop in order to make sure the right sql is 
6)better prompts for the explanation function, intent handling and semantic handling
"""

import sqlite3
import pandas as pd
import requests
import json

# ================= DB SETUP =================

df = pd.read_csv(r"archive\olist_order_payments_dataset.csv")

conn = sqlite3.connect("olist.db")
df.to_sql("payments", conn, if_exists="replace", index=False)
conn.close()


def get_schema():
    conn = sqlite3.connect("olist.db")
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(payments)")
    columns = cursor.fetchall()

    schema = "Table: payments\nColumns:\n"
    for col in columns:
        schema += f"- {col[1]} ({col[2]})\n"

    conn.close()
    return schema


schema = get_schema()



# ================= CONFIG =================

SQL_API = "http://172.168.0.8:1337/v1/chat/completions"
MODEL_NAME = "sqlcoder-7b-q5_k_m.gguf"

QWEN_API = "http://127.0.0.1:1337/v1/chat/completions"
QWEN_MODEL = "Qwen2.5-Coder-7B-Instruct-Q6_K_L.gguf"


# ================= 1. SEMANTIC UNDERSTANDING =================
def handle_semantic_query(question, schema, sample_data):
    print("🧠 Understanding data semantics...")

    prompt = f"""
You are a data analyst.

Schema:
{schema}

Sample data:
{sample_data}

Question:
{question}

Your job:
- Explain what the column likely represents
- Infer meaning from column name + sample values
- Be practical and realistic

Rules:
- Do NOT mention SQL
- Answer like a human analyst
"""

    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    })

    return response.json()['choices'][0]['message']['content']

# ================= SAMPLE DATA FOR SEMANTIC UNDERSTANDING =================
def get_sample_data():
    conn = sqlite3.connect("olist.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM payments LIMIT 5")
    rows = cursor.fetchall()
    columns = [d[0] for d in cursor.description]

    conn.close()

    text = "Columns: " + ", ".join(columns) + "\n\n"

    for r in rows:
        text += ", ".join(str(x) for x in r) + "\n"

    return text
# ================= 1. INTENT + REFINEMENT =================

def analyze_question(question, schema):
    print("🔍 Analyzing Question...")

    json_format = '{"intent":"schema|data|semantic|invalid","refined":"text"}'

    prompt = f"""
    Classify and refine a database-related question.

    Schema:
    {schema}

    Question:
    {question}

    Intent definitions:
    - schema → structure
    - data → requires SQL
    - invalid → unrelated
            
    Intent definitions:
    - schema → asking about structure (columns, table, fields, schema, number of columns)
    - data → asking about data inside the table (counts, sums, filters, analysis)
    - invalid → completely unrelated to the database

    Rules:
    - Questions like "how many columns", "what fields exist" = schema
    - DO NOT mark valid DB questions as invalid
    - If schema → return original question
    - If data → rewrite clearly
    - If invalid → return original

    Examples:
    Q: How many columns are there?
    A: "intent":"schema","refined":"How many columns are in the table?"

    Q: total payment value
    A: "intent":"data","refined":"Calculate total payment_value from payments table"

    STRICT OUTPUT:
    {json_format}
    """

    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    })

    raw = response.json()['choices'][0]['message']['content']
    raw = raw.replace("```json", "").replace("```", "").strip()

    try:
        parsed = json.loads(raw)
        return parsed["intent"], parsed["refined"]
    except:
        print("⚠️ Failed parsing:", raw)
        return "data", question

# ================= 2. SQL GENERATION =================

def generate_sql(question, schema):
    print("⚙️ Generating SQL...")

    prompt = f"""
Generate SQLite query.

Schema:
{schema}

Rules:
- ONLY return SQL
- NO explanation
- NO markdown
- NO code block
- Use only given columns
- Limit results to 5 unless specified

Question:
{question}
"""

    response = requests.post(
        SQL_API,
        json={
            "model": MODEL_NAME,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0
        },
        headers={"Host": "localhost"}
    )

    raw = response.json()['choices'][0]['message']['content']
    return raw.strip()


# ================= 3. VALIDATION =================

def validate_sql(query, schema):
    print("🧠 Validating SQL...")

    prompt = f"""
Validate SQL.

Schema:
{schema}

Query:
{query}

Rules:
- ONLY output VALID or INVALID
- No explanation
"""

    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    })

    return response.json()['choices'][0]['message']['content'].strip()


# ================= 4. EXECUTION =================

def run_query(query):
    conn = sqlite3.connect("olist.db")
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        conn.close()
        return {"columns": columns, "rows": rows}

    except Exception as e:
        conn.close()
        return {"error": str(e)}


# ================= 5. FORMAT =================

def format_result(result):
    if "error" in result:
        return f"ERROR: {result['error']}"

    cols = result["columns"]
    rows = result["rows"][:5]

    text = " | ".join(cols) + "\n"
    text += "-" * 40 + "\n"

    for r in rows:
        text += " | ".join(str(x) for x in r) + "\n"

    return text


# ================= 6. EXPLANATION =================

def explain(question, result_text):
    print("🧠 Explaining...")

    prompt = f"""
Explain data result.

Question:
{question}

Result:
{result_text}

Rules:
- Simple explanation
- No SQL mention
"""

    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    })

    return response.json()['choices'][0]['message']['content']



# ================= MAIN LOOP =================

while True:
    q = input("\nAsk (exit to quit): ")

    if q.lower() == "exit":
        break

    intent, refined = analyze_question(q, schema)
    print("Intent:", intent)

    if intent == "schema":
        print(schema)
        continue

    if intent == "invalid":
        print("Invalid question.")
        continue

    if intent == "semantic":
        answer = handle_semantic_query(q, schema, sample_data)
        print("\n", answer)
        continue

    print("Refined:", refined)

    sql = generate_sql(refined, schema)
    print("SQL:", sql)

    val = validate_sql(sql, schema)
    print("Validation:", val)

    if "INVALID" in val:
        print("Query rejected.")
        continue

    result = run_query(sql)

    formatted = format_result(result)
    print("\nResult:\n", formatted)

    explanation = explain(q, formatted)
    print("\nExplanation:\n", explanation)