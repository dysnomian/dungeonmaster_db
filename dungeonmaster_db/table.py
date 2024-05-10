"""
Generic class for a table. Includes methods for creating and dropping the table, as well as inserting and deleting rows.
"""

from typing import List

from psycopg2 import sql

from dungeonmaster_db.adapter import DbConnection as db_conn

class Table:
    def __init__(self, table_name: str, create_sql: str):
        self.table_name = table_name
        self.create_sql = create_sql

    def create_table(self) -> None:
        with db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(self.create_sql)
                print(f"Table {self.table_name} created.")
            finally:
                cursor.close()
    
    def drop_table(self, cascade: bool = False) -> None:
        with db_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} {}")
                    .format(
                        sql.Identifier(self.table_name),
                        sql.SQL("CASCADE" if cascade else "")
                    )
                )
                print(f"Table {self.table_name} dropped.")
            finally:
                cursor.close()

    @classmethod
    def drop(cls, table_names: List[str] = []) -> None:
        with db_conn() as conn:
            cursor = conn.cursor()
            for table_name in table_names:
                try:
                    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
                        sql.Identifier(table_name)
                    ))
                    print(f"Table {table_name} dropped.")
                finally:
                    cursor.close()
    
    @classmethod
    def all(cls) -> List[str]:
        with db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            return [table[0] for table in tables]
