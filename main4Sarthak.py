import sqlite3
import pandas as pd
import requests
import json
import webbrowser
import os
# ================= DB SETUP =================
# Ensure the dataset exists or change this path to your local CSV
try:
    df = pd.read_csv(r"archive\olist_order_payments_dataset.csv")
    conn = sqlite3.connect("olist.db")
    df.to_sql("payments", conn, if_exists="replace", index=False)
    conn.close()
except Exception as e:
    print(f"Note: Could not auto-load CSV: {e}. Ensure olist.db exists.")

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
# Adjust these URLs if your Local AI server (LM Studio/Ollama) uses different ports
SQL_API = "http://172.168.0.8:1337/v1/chat/completions"
MODEL_NAME = "sqlcoder-7b-q5_k_m.gguf"

QWEN_API = "http://127.0.0.1:1337/v1/chat/completions"
QWEN_MODEL = "Qwen2.5-Coder-7B-Instruct-Q6_K_L.gguf"

# ================= HELPER: CLEAN SQL =================
def clean_sql_output(raw_sql):
    return raw_sql.replace("sql", "").replace("", "").strip()

# ================= 1. SEMANTIC UNDERSTANDING =================
def get_full_schema():
    conn = sqlite3.connect("olist.db")
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    full_schema_text = ""
    
    for table in tables:
        table_name = table[0]
        full_schema_text += f"\nTable: {table_name}\n"
        
        # Get columns for this specific table
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        for col in columns:
            full_schema_text += f"  - {col[1]} ({col[2]})\n"
            
    conn.close()
    return full_schema_text

def generate_mermaid_schema(dynamic_schema):
    print("📊 Analyzing entire database structure...")
    
    prompt = f"""
    You are a Senior Data Architect. Generate a Mermaid.js ER diagram based on this dynamic schema:
    
    {dynamic_schema}
    
    Instructions:
    1. Start with 'erDiagram'.
    2. Map every table provided.
    3. Identify potential relationships (e.g., if two tables share an 'order_id' or 'customer_id', draw a line between them like: table1 ||--o{{ table2 : "links")
    4. Output ONLY the raw Mermaid code. No markdown, no backticks, no talk.
    """
    
    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1
    })
    
    return response.json()['choices'][0]['message']['content'].strip()


def render_mermaid_html(mermaid_code):
    # Strip markdown backticks if Qwen adds them
    clean_code = mermaid_code.replace("```mermaid", "").replace("```", "").strip()
    
    html_content = f"""
    <html>
    <head><title>DB Visualizer</title></head>
    <body style="background: #1e1e1e; color: white; font-family: sans-serif; display: flex; justify-content: center;">
        <pre class="mermaid">
            {clean_code}
        </pre>
        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true, theme: 'dark' }});
        </script>
    </body>
    </html>
    """
    with open("schema_vis.html", "w") as f:
        f.write(html_content)
    
    webbrowser.open('file://' + os.path.realpath("schema_vis.html"))
    print("🚀 ER Diagram opened in your browser.")
    
def handle_semantic_query(question, schema, sample_data):
    print("🧠 Understanding data semantics...")
    prompt = f"""
You are a data analyst.
Schema: {schema}
Sample data: {sample_data}
Question: {question}

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

# ================= 2. INTENT + REFINEMENT =================
def analyze_question(question, schema):
    print("🔍 Analyzing Question...")
    json_format = '{"intent":"schema|data|semantic|invalid","refined":"text"}'
    prompt = f"""
    Classify and refine a database-related question.
    Schema: {schema}
    Question: {question}

    Intent definitions:
    - schema → structure (columns, table fields)
    - data → requires SQL (counts, sums, filters)
    - semantic → asking what a column means or represents
    - visualize → asking for a visual graph or ER diagram of the database
    - invalid → unrelated to the DB

    STRICT OUTPUT: {json_format}
    """
    try:
        response = requests.post(QWEN_API, json={
            "model": QWEN_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0
        })
        raw = response.json()['choices'][0]['message']['content']
        raw = raw.replace("json", "").replace("", "").strip()
        parsed = json.loads(raw)
        return parsed["intent"], parsed["refined"]
    except Exception as e:
        print(f"⚠️ Intent Error: {e}")
        return "data", question

# ================= 3. SQL GENERATION & REPAIR =================
def generate_sql(question, schema):
    print("⚙️ Generating SQL...")
    prompt = f"Generate SQLite query.\nSchema:\n{schema}\nRules: ONLY SQL, no markdown.\nQuestion: {question}"
    response = requests.post(SQL_API, json={
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    }, headers={"Host": "localhost"})
    return clean_sql_output(response.json()['choices'][0]['message']['content'])
def repair_sql(original_question, broken_sql, error_msg, schema):
    print("🔧 Attempting to repair SQL...")

    prompt = f"""
You are an expert SQLite debugger.

Schema:
{schema}

User Question:
{original_question}

Broken SQL:
{broken_sql}

Error:
{error_msg}

Your task:
- Fix the query
- Use ONLY valid columns from schema
- Keep logic same as question

Rules:
- ONLY return fixed SQL
- NO explanation
- NO markdown
"""

    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    })

    return clean_sql_output(response.json()['choices'][0]['message']['content'])
# ================= 4. VALIDATION & EXECUTION =================
def validate_sql(query, schema):
    print("🧠 Validating SQL...")
    prompt = f"Validate SQL for SQLite.\nSchema: {schema}\nQuery: {query}\nRules: ONLY output VALID or INVALID."
    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0
    })
    return response.json()['choices'][0]['message']['content'].strip()

def run_query(query):
    try:
        conn = sqlite3.connect("olist.db")
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        conn.close()
        return {"columns": columns, "rows": rows}
    except Exception as e:
        return {"error": str(e)}

# ================= 5. FORMAT & EXPLAIN =================
def format_result(result):
    if "error" in result: return f"ERROR: {result['error']}"
    cols = result["columns"]
    rows = result["rows"][:5]
    text = " | ".join(cols) + "\n" + "-" * 40 + "\n"
    for r in rows:
        text += " | ".join(str(x) for x in r) + "\n"
    return text

def explain(question, result_text):
    print("🧠 Explaining...")
    prompt = f"Explain this data result simply for the question: {question}\nResult:\n{result_text}\nRules: No SQL mention."
    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    })
    return response.json()['choices'][0]['message']['content']

# ================= MAIN LOOP =================
while True:
    q = input("\nAsk (exit to quit): ")
    if q.lower() == "exit": break

    # 1. Get the current state of the DB dynamically
    current_db_schema = get_full_schema() 

    # 2. Analyze intent using the dynamic schema
    intent, refined = analyze_question(q, current_db_schema)
    
    # 3. Handle Visualization Intent (The New Part)
    if intent == "visualize":
        # Generate the diagram code using the full schema
        mermaid_code = generate_mermaid_schema(current_db_schema)
        
        # Log it for debugging
        print("\n--- Generated Mermaid Code ---")
        print(mermaid_code)
        print("------------------------------")
        
        # Launch the browser
        render_mermaid_html(mermaid_code)
        continue

    # 4. Handle Schema Text Intent
    elif intent == "schema":
        print(current_db_schema)
        continue

    # 5. Handle Semantic/Data/Invalid (Your existing logic)
    elif intent == "invalid":
        print("I'm sorry, I can only help with database-related questions."); continue
    
    elif intent == "semantic":
        answer = handle_semantic_query(q, current_db_schema, get_sample_data())
        print("\n", answer); continue

    # 6. SQL Flow (Data Intent)
    sql = generate_sql(refined, current_db_schema)
    print("SQL:", sql)

    # ... (Rest of your SQL Validation and Execution code)