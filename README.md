# Snowflake Database Schema Extractor

This Python script extracts **metadata and sample data** from a Snowflake database and exports it into structured JSON files.
The generated files are designed to provide a **compact representation of the database structure**, including schemas, tables, column definitions, and example rows.

The output can be used for:

* AI code assistants (such as Roo Code) that require database context
* Documentation of database structures
* Data exploration and schema analysis
* Creating lightweight database snapshots for development or testing

---

## Features

* Connects directly to a **Snowflake database**
* Extracts **all schemas and tables**
* Retrieves **column metadata**:

  * column name
  * data type
  * nullability
* Collects **sample rows (up to 5 per table)**
* Automatically converts non-JSON-compatible values (timestamps, decimals, etc.)
* Splits the output into **multiple JSON files** to keep context manageable
* Groups **multiple tables per JSON file** (configurable)

---

## Output Structure

The script generates JSON files inside the `context/` directory.

Example output files:

```
context/
AIRBNB_index_part_1.json
AIRBNB_index_part_2.json
AIRBNB_index_part_3.json
```

Each JSON file contains:

* Database name
* Schemas
* Tables within each schema
* Column metadata
* Sample rows

Example structure:

```json
{
  "database": "AIRBNB",
  "schemas": {
    "PUBLIC": {
      "tables": {
        "LISTINGS": {
          "columns": [
            {"name": "ID", "type": "NUMBER", "nullable": "NO"},
            {"name": "NAME", "type": "TEXT", "nullable": "YES"}
          ],
          "sample_rows": [
            {"ID": 1001, "NAME": "Cozy Apartment"}
          ]
        }
      }
    }
  }
}
```

---

## Configuration

You can customize the script using the **User Settings** section:

```python
DATABASE = "AIRBNB"
OUTPUT_DIR = "context"
TABLES_PER_JSON = 5
```

| Parameter       | Description                                  |
| --------------- | -------------------------------------------- |
| DATABASE        | Target Snowflake database                    |
| OUTPUT_DIR      | Directory where JSON files will be generated |
| TABLES_PER_JSON | Number of tables stored in each JSON file    |

---

## Requirements

Install the Snowflake Python connector:

```bash
pip install snowflake-connector-python
```

---

## Usage

1. Configure your Snowflake credentials inside the `connect()` function:

```python
account="your_account"
user="your_user"
password="your_password"
warehouse="your_warehouse"
role="your_role"
```

2. Run the script:

```bash
python extract_schema.py
```

3. The JSON files will be generated in the `context/` folder.

---

## Why Split Tables Across Multiple JSON Files?

Large databases may contain **hundreds of tables**.
Splitting them into smaller JSON files helps:

* reduce file size
* improve AI tool context management
* make schema exploration easier
* avoid large monolithic metadata files

---

## Example Use Case

This tool is particularly useful when working with **AI coding assistants** that require database context. Instead of exposing the full database, you can provide a **lightweight JSON representation** that contains:

* structure
* column definitions
* example data

This allows AI tools to better understand the data model while keeping the context size manageable.

---

## License

MIT License
