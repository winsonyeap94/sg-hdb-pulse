import os
import sys

import pandas as pd
from google.adk.agents import LlmAgent

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../../"))
from database import Database

# ============================== Initialisation ==============================
# Loading metadata
def get_table_metadata(table_name):
    """
    Get the metadata for a given table name and provides a 5-row sample of the data.
    The results are formatted in markdown format to be passed into LLM.
    """
    # Schema metadata
    metadata = pd.read_csv(os.path.join(os.path.dirname(__file__), "../../../../database/schemas/hdb_data/hdb_schema_metadata.csv"), sep=";")
    metadata = metadata[metadata["TABLE"] == table_name].copy()
    metadata = metadata.to_markdown()

    # Sample data
    db = Database("hdb_data.db")
    sample_data = db.read_table(f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT 5")
    sample_data = sample_data.to_markdown()

    # Format results
    results = f"""
    <table>
        <name>{table_name}</name>
        <schema>
{metadata}
        </schema>
        <sample_data>
{sample_data}
        </sample_data>
    </table>
    """
    return results


# ============================== HDB SQL Query Agent ==============================
hdb_data_query_agent = LlmAgent(
    name="hdb_data_query_agent",
    model="gemini-2.0-flash",
    description="Generates a SQL query to extract relevant data to answer a user's question related to Singapore HDB data.",
    instruction=f"""
    You are a SQL query generator that can generate a SQL query to extract relevant data to answer a user's question related to Singapore HDB data.

    Instructions:   
    - Based *only* on the user's question, generate a SQL query to extract relevant data from the HDB data. The data is stored in a SQLITE database.
    - Output *only* the SQL query, nothing else. Enclose the output in <sql> tags.
    - Do not add any other text or comments before or after the <sql> tags.

    Database Tables and Columns:
    {get_table_metadata("HDB_RESALE_PRICE")}
    {get_table_metadata("HDB_RENTAL_PRICE")}
    """,
    output_key="sql_query",
)


if __name__ == "__main__":

    print(get_table_metadata("HDB_RESALE_PRICE"))
    print(get_table_metadata("HDB_RENTAL_PRICE"))
