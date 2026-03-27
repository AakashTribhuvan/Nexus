import sqlite3
import pandas as pd
import requests
import json
import webbrowser
import os

# ================= CONFIGURATION =================
# Laptop 2: The SQL Specialist
SQL_API = "http://172.168.0.8:1337/v1/chat/completions"
SQL_MODEL = "sqlcoder-7b-q5_k_m.gguf"

# Laptop 1 (Local): The Reasoning & Explanation Brain
QWEN_API = "http://127.0.0.1:1337/v1/chat/completions"
QWEN_MODEL = "Qwen2.5-Coder-7B-Instruct-Q6_K_L.gguf"

DB_NAME = "analytics_platform.db"
CSV_FOLDER = "archive"

# ================= 1. DYNAMIC DATABASE ENGINE =================
def initialize_database(folder_path):
    """Scans folder for CSVs and creates a multi-table SQLite DB."""
    conn = sqlite3.connect(DB_NAME)
    if not os.path.exists(folder_path):
        print(f"❌ Folder '{folder_path}' not found!")
        return

    print("📥 Initializing Database from CSV Repository...")
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            # Clean filename to create a valid SQL table name
            table_name = file.replace(".csv", "").replace("olist_", "").replace("_dataset", "")
            file_path = os.path.join(folder_path, file)
            
            try:
                df = pd.read_csv(file_path)
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                print(f"  ✅ Loaded '{file}' as table: [{table_name}]")
            except Exception as e:
                print(f"  ⚠️ Failed to load {file}: {e}")
    conn.close()
    print("🚀 All tables synchronized.\n")

def get_full_schema():
    """Extracts schema for ALL tables dynamically."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    schema_text = ""
    for table in tables:
        t_name = table[0]
        schema_text += f"\nTable: {t_name}\nColumns:\n"
        cursor.execute(f"PRAGMA table_info({t_name})")
        for col in cursor.fetchall():
            schema_text += f"  - {col[1]} ({col[2]})\n"
    conn.close()
    return schema_text

def get_multi_table_samples():
    """Gets sample rows from every table for semantic context."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    samples = ""
    for table in tables:
        t_name = table[0]
        cursor.execute(f"SELECT * FROM {t_name} LIMIT 3")
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        samples += f"\n[Table: {t_name}]\nCols: {', '.join(cols)}\n"
        for r in rows:
            samples += f"  Sample: {str(r)}\n"
    conn.close()
    return samples

# ================= 2. INTELLIGENT AGENTS =================
def analyze_intent(question, schema):
    print("🔍 Analyzing Intent...")
    prompt = f"""
    Classify the intent of this database question.
    Schema: {schema}
    Question: {question}

    Intents:
    - visualize: User wants a graph, ER diagram, or visual map.
    - schema: User wants to know the structure/tables/columns in text.
    - semantic: User asks what a column means or represents.
    - data: User wants a specific answer (requires SQL).
    - invalid: Unrelated to this database.

    Output ONLY a JSON: {{"intent": "...", "refined": "clear version of question"}}
    """
    try:
        response = requests.post(QWEN_API, json={
            "model": QWEN_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0
        })
        raw = response.json()['choices'][0]['message']['content'].strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except:
        return {"intent": "data", "refined": question}

def generate_visual_diagram(schema):
    print("📊 Architecting ER Diagram...")
    prompt = f"Generate a Mermaid.js erDiagram for this schema: {schema}\nIdentify relationships based on ID columns. Output ONLY raw mermaid code, no backticks."
    response = requests.post(QWEN_API, json={
        "model": QWEN_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.1
    })
    return response.json()['choices'][0]['message']['content'].strip()

def generate_sql(question, schema):
    print("⚙️ Generating Multi-Table SQL...")
    prompt = f"Schema:\n{schema}\nQuestion: {question}\nRules: SQLite syntax. Use JOINs if needed. ONLY output SQL."
    response = requests.post(SQL_API, json={
        "model": SQL_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0
    }, headers={"Host": "localhost"})
    sql = response.json()['choices'][0]['message']['content']
    return sql.replace("```sql", "").replace("```", "").strip()

# ================= 3. UTILITIES & RENDERING =================
def render_mermaid(code):
    clean_code = code.replace("```mermaid", "").replace("```", "").strip()
    html = f"""
    <html><body style="background:#1e1e1e; display:flex; justify-content:center; padding:20px;">
        <pre class="mermaid">{clean_code}</pre>
        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true, theme: 'dark' }});
        </script>
    </body></html>
    """
    with open("db_map.html", "w") as f: f.write(html)
    webbrowser.open('file://' + os.path.realpath("db_map.html"))

def run_query(sql):
    try:
        conn = sqlite3.connect(DB_NAME)
        df_res = pd.read_sql_query(sql, conn)
        conn.close()
        return df_res
    except Exception as e:
        return str(e)

# ================= MAIN EXECUTION LOOP =================
if __name__ == "__main__":
    initialize_database(CSV_FOLDER)
    
    while True:
        query = input("\n💬 Ask your data (or 'exit'): ")
        if query.lower() == 'exit': break
        
        full_schema = get_full_schema()
        analysis = analyze_intent(query, full_schema)
        intent = analysis['intent']
        refined = analysis['refined']

        if intent == "visualize":
            mermaid = generate_visual_diagram(full_schema)
            render_mermaid(mermaid)
            print("🚀 Diagram generated in browser.")
        
        elif intent == "schema":
            print(f"\n--- DATABASE STRUCTURE ---\n{full_schema}")

        elif intent == "semantic":
            samples = get_multi_table_samples()
            # Call your existing handle_semantic_query logic here...
            print("\n🧠 AI is analyzing data meaning...")

        elif intent == "data":
            sql = generate_sql(refined, full_schema)
            print(f"🤖 SQL Generated: {sql}")
            
            result = run_query(sql)
            if isinstance(result, str):
                print(f"❌ Error: {result}")
            else:
                print("\n--- RESULTS ---")
                print(result.head(10).to_string(index=False))
                # Call your existing explain() logic here...
        
        else:
            print("❌ Intent not recognized or invalid query.")