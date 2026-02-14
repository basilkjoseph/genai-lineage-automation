import os
import json,csv
from google import genai
from google.genai import types
import dwl_tables as dwl
import env_params as ep
import time


# --- Configuration ---
API_KEY = "" 
client = genai.Client(api_key=API_KEY)
MODEL_NAME = "gemini-2.5-pro" 

def get_base_source_lineage(filepath, dwl_table_name):
    # 1. Read File
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            script_content = f.read()
    except FileNotFoundError:
        return None

    # 2. SYSTEM INSTRUCTION (The "Expert Prompt" Rules)
    # Note: We replaced specific table names with placeholders here so this ruleset applies to ANY run.
    expert_system_instruction = """
    You are an expert **Teradata BTEQ SQL Parser, Data Lineage Architect, and Recursive Logic Engine.**
    Your sole objective is to analyze BTEQ scripts to identify the **Ultimate, Persistent Base Source Tables**.

    ### LINEAGE RULES & ANALYSIS LOGIC (STRICT ADHERENCE REQUIRED)
    1. **Recursive Backtracking (CRUCIAL):**
       * Trace the data path backward. If the target is loaded from an **Intermediate Table** (Staging `_STG`, `_WORK`, or Volatile `VT_`), you **MUST** ignore that intermediate table.
       * Recursively find the persistent source that populated that intermediate table.
    
    2. **Exclusion Filters (DO NOT INCLUDE IN OUTPUT):**
       * **Volatile/Work/Staging:** Any table prefixed with `VT_` or in a staging schema.
       * **Audit/Control:** Specifically exclude tables from schemas like `${AUDIT_DB}` or tables named `AUDIT1`, `AUDIT2`, `AUDIT3`, `AUDIT4`.
       * **Keys:** Exclude tables ending in `_KEY` or KEYSYS_ID.

    3. **Syntax Preservation:** * You MUST preserve the exact parameterized schema syntax `${SCHEMA_VAR}.table_name`. 

    ### OUTPUT FORMAT
    Return **ONLY** a valid, parseable JSON object.
    {
      "dwl_target_table": "target_table_name",
      "base_source_tables": ["${DB}.table1", "${DB}.table2"]
    }
    """

    # 3. USER MESSAGE (The specific data for THIS run)
    user_content = f"""
    Analyze the script below for this specific target.
    
    TARGET TABLE: {dwl_table_name}

    ### BTEQ SCRIPT CONTENT
    ---
    {script_content}
    ---
    """

    print(f"Analyzing {dwl_table_name}...")

    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=user_content,
            config=types.GenerateContentConfig(
                temperature=0.0,  # Deterministic
                response_mime_type="application/json",
                system_instruction=expert_system_instruction
            )
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"Error: {e}")
        return None


#Function to map environmental parameters
def map_lineage_with_environmental_parameters(lineage: dict, main_dwl_schema: str) -> dict:
    mapped_lineage = {}

    # Extract target table and prepare full target name
    dwl_table = lineage["dwl_target_table"]
    full_target_name = f"{main_dwl_schema}.{dwl_table}"

    # Ensure key exists
    mapped_lineage.setdefault(full_target_name, [])

    # Iterate all source tables
    for source in lineage["base_source_tables"]:
        # Safety check: ensure "schema.table"
        if "." not in source:
            print(f"Skipping invalid source entry: {source}")
            continue

        # Split only once
        source_schema, source_table = source.split(".", 1)
        source_schema = source_schema.removeprefix("${")
        source_schema = source_schema.removesuffix("}")

        # Map schema using environment params (fallback to original)
        mapped_schema = ep.env_params.get(source_schema, source_schema)

        # Add final mapped source entry
        mapped_lineage[full_target_name].append(f"{mapped_schema}.{source_table}")

    return mapped_lineage



def find_lineage(bteq_file_path, main_dwl_table):
    target_dwl_schema,target_dwl_table=main_dwl_table.split('.')
    full_path = os.path.join(dwl.path, bteq_file_path)
    lineage_data = get_base_source_lineage(full_path, target_dwl_table)
    return map_lineage_with_environmental_parameters(lineage_data,target_dwl_schema)



if __name__ == "__main__":

    with open("Table_Name_Lineage", "w", newline="") as file: 
        writer = csv.writer(file)
        writer.writerow(["SL","DWL_Table_Name", "Underlying_Source_Tables"]) 
        count=1
        for dwl_table_name, bteq_file_path in dwl.table_list:
            lineage = find_lineage(bteq_file_path, dwl_table_name)
            print("Processing:", dwl_table_name, bteq_file_path)
            print("Lineage returned:", lineage)
            if lineage:
                for target_table, source_tables in lineage.items():
                    for table in source_tables:
                        writer.writerow([count,target_table, table])
                    count+=1
            time.sleep(30)

        print("Lineage Analysis Completed!")


