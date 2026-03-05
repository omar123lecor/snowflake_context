import os
import json
from decimal import Decimal
import snowflake.connector as sf

# --- USER SETTINGS ---
DATABASE = "Your database"  # Replace with your DB
OUTPUT_DIR = "context"
TABLES_PER_JSON = 5  # Number of tables per JSON file

# --- Snowflake Connection ---
def connect():
    return sf.connect(
        account="Your account identifier",
        user="Your user name",
        password="Your password",
        warehouse="Your warehouse ",
        role="Your role"  # optional
    )

# --- Helper to make values JSON-serializable ---
def make_json_serializable(obj):
    if isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    elif hasattr(obj, "isoformat"):  # datetime, date, timestamp
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return str(obj)

# --- Build grouped JSONs ---
def build_index_with_samples_grouped():
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(f'USE DATABASE "{DATABASE}"')
        cur.execute(f"""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM "{DATABASE}".INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        tables = cur.fetchall()
        if not tables:
            print("No tables found.")
            return

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Prepare JSON chunks
        chunk = {"database": DATABASE, "schemas": {}}
        file_index = 1
        tables_in_chunk = 0

        for schema, table in tables:
            if schema not in chunk["schemas"]:
                chunk["schemas"][schema] = {"tables": {}}

            # Columns metadata
            cur.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM "{DATABASE}".INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (schema, table))
            columns = [{"name": c[0], "type": c[1], "nullable": c[2]} for c in cur.fetchall()]

            # 5-row sample
            try:
                cur.execute(f'SELECT * FROM "{DATABASE}"."{schema}"."{table}" LIMIT 5')
                sample_rows = []
                for row in cur.fetchall():
                    row_dict = {desc[0]: make_json_serializable(val) for desc, val in zip(cur.description, row)}
                    sample_rows.append(row_dict)
            except Exception as e:
                sample_rows = [{"error": str(e)}]

            # Add to chunk
            chunk["schemas"][schema]["tables"][table] = {
                "columns": columns,
                "sample_rows": sample_rows
            }

            tables_in_chunk += 1

            # Save chunk if reached limit
            if tables_in_chunk >= TABLES_PER_JSON:
                output_file = os.path.join(OUTPUT_DIR, f"{DATABASE}_index_part_{file_index}.json")
                with open(output_file, "w") as f:
                    json.dump(chunk, f, indent=2)
                print(f"Saved {output_file} with {tables_in_chunk} tables.")
                file_index += 1
                chunk = {"database": DATABASE, "schemas": {}}
                tables_in_chunk = 0

        # Save any remaining tables
        if tables_in_chunk > 0:
            output_file = os.path.join(OUTPUT_DIR, f"{DATABASE}_index_part_{file_index}.json")
            with open(output_file, "w") as f:
                json.dump(chunk, f, indent=2)
            print(f"Saved {output_file} with {tables_in_chunk} tables.")

        print("✅ All done!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    build_index_with_samples_grouped()