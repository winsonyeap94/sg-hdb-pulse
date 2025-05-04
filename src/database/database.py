import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Tuple

import pandas as pd


class Database:
    def __init__(self, db_path: str = "hdb_data.db"):
        """Initialize the database connection.
        
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = os.path.join(os.path.dirname(__file__), db_path)
        self.conn = None
        self.cursor = None
        
    def connect(self) -> None:
        """Establish a connection to the database."""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            
    def execute(self, query: str, params: Tuple[Any, ...] = ()) -> None:
        """Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            params (Tuple[Any, ...]): Query parameters
        """
        if not self.conn:
            self.connect()
        self.cursor.execute(query, params)
        self.conn.commit()
        
    def fetch_one(self, query: str, params: Tuple[Any, ...] = ()) -> Optional[Tuple[Any, ...]]:
        """Execute a query and fetch one result.
        
        Args:
            query (str): SQL query to execute
            params (Tuple[Any, ...]): Query parameters
            
        Returns:
            Optional[Tuple[Any, ...]]: Single row result or None
        """
        if not self.conn:
            self.connect()
        self.cursor.execute(query, params)
        return self.cursor.fetchone()
        
    def fetch_all(self, query: str, params: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
        """Execute a query and fetch all results.
        
        Args:
            query (str): SQL query to execute
            params (Tuple[Any, ...]): Query parameters
            
        Returns:
            List[Tuple[Any, ...]]: List of row results
        """
        if not self.conn:
            self.connect()
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
        
    def read_table(self, query: str, params: Tuple[Any, ...] = ()) -> pd.DataFrame:
        """Read a table into a pandas DataFrame.
        
        Args:
            query (str): SQL query to execute
            params (Tuple[Any, ...]): Query parameters
        """
        if not self.conn:
            self.connect()
        df = pd.read_sql_query(query, self.conn, params=params)
        df.columns = [x.upper() for x in df.columns]
        return df

    def create_table(self, table_name: str, columns: List[str]) -> None:
        """Create a new table if it doesn't exist.
        
        Args:
            table_name (str): Name of the table to create
            columns (List[str]): List of column definitions
        """
        columns_str = ", ".join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
        self.execute(query)
        
    def insert(self, table_name: str, data: dict) -> None:
        """Insert a row into a table.
        
        Args:
            table_name (str): Name of the table
            data (dict): Dictionary of column names and values
        """
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.execute(query, tuple(data.values()))

    def bulk_insert_df(self, table_name: str, df: pd.DataFrame, if_exists: str = 'append') -> None:
        """Bulk insert data from a pandas DataFrame into a table.
        
        Args:
            table_name (str): Name of the table to insert into
            df (pd.DataFrame): DataFrame containing the data to insert
            if_exists (str): How to behave if the table already exists.
                           Options: 'fail', 'replace', 'append' (default)
        """
        if if_exists == 'replace':
            # Drop the existing table
            self.execute(f"DELETE FROM {table_name}")
        
        if not self.conn:
            self.connect()
        
        chunk_size = 1000  # Adjust this number based on your data and SQLite's variable limit
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            chunk.to_sql(
                name=table_name,
                con=self.conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            self.conn.commit()

    @staticmethod
    def datetime_to_sqlite(dt: datetime) -> str:
        """Convert a datetime object to SQLite compatible string.
        
        Args:
            dt (datetime): Python datetime object
            
        Returns:
            str: ISO 8601 formatted string
        """
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def sqlite_to_datetime(dt_str: str) -> datetime:
        """Convert a SQLite datetime string to Python datetime object.
        
        Args:
            dt_str (str): ISO 8601 formatted string
            
        Returns:
            datetime: Python datetime object
        """
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close() 