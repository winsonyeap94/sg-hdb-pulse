import os
from pathlib import Path

from database import Database


def initialize_schemas(db_path) -> None:
    """
    Initialize all database schemas by executing SQL files in the schemas directory.
    
    Args:
        db_path (str): Path to the SQLite database file
    """
    # Get the directory containing this script
    current_dir = Path(__file__).parent
    schemas_dir = current_dir / "schemas"
    
    # Initialize database connection
    db = Database(db_path)
    
    try:
        # Walk through all subdirectories in schemas
        for root, _, files in os.walk(schemas_dir):
            for file in files:
                if file.endswith('.sql'):
                    # Get the full path to the SQL file
                    sql_file = Path(root) / file
                    print(f"Executing schema: {sql_file}")
                    
                    # Read and execute the SQL file
                    with open(sql_file, 'r') as f:
                        sql_commands = f.read()
                        
                    # Split the file into individual commands and execute them
                    for command in sql_commands.split(';'):
                        command = command.strip()
                        if command:  # Skip empty commands
                            db.execute(command)
                            
        print("All schemas initialized successfully!")
        
    except Exception as e:
        print(f"Error initializing schemas: {str(e)}")
        raise
        
    finally:
        db.close()

if __name__ == "__main__":

    for database in ["hdb_data.db"]:
        initialize_schemas(db_path=database)
