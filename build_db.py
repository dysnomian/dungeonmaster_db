import yaml

from typing import Annotated, List, Dict, Any

from psycopg2 import sql
from psycopg2.errors import UniqueViolation, DuplicateTable, UndefinedTable

from dungeonmaster_db.adapter import DbConnection as db_conn
from dungeonmaster_db.adapter import create_database, drop_database

def create_tables() -> None:
    "Create the tables in the database if they do not exist."
    with db_conn() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Locations (
                    id SERIAL PRIMARY KEY NOT NULL,
                    starting_location_id INTEGER REFERENCES Locations(id) ON DELETE CASCADE,
                    parent_id INTEGER REFERENCES Locations(id) ON DELETE CASCADE,
                    name VARCHAR(255) NOT NULL UNIQUE,
                    category VARCHAR(100),
                    interior_description TEXT,
                    exterior_description TEXT,
                    notes TEXT
                )
            """)
            print("Table Locations created.")
        except DuplicateTable:
            print("Table Locations already exists.")

        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Connections (
                    id SERIAL PRIMARY KEY NOT NULL,
                    start_location_id INTEGER REFERENCES Locations(id) ON DELETE CASCADE,
                    end_location_id INTEGER REFERENCES Locations(id) ON DELETE CASCADE,
                    name VARCHAR(255) NOT NULL UNIQUE,
                    description TEXT,
                    category VARCHAR(100),
                    is_one_way BOOLEAN DEFAULT FALSE,
                    is_hidden BOOLEAN DEFAULT FALSE,
                    is_fast_travel_path BOOLEAN DEFAULT FALSE,
                    describe_end_location_exterior BOOLEAN DEFAULT FALSE
                )
            """)
            print("Table Connections created.")
        except DuplicateTable:
            print("Table Connections already exists.")

        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Sessions (
                    id SERIAL PRIMARY KEY NOT NULL,
                    current_location_id INTEGER REFERENCES Locations(id) ON DELETE CASCADE,
                    time_in_game VARCHAR(200)
                )
            """)
            print("Table Sessions created.")
        except DuplicateTable:
            print("Table Sessions already exists.")

        cursor.close()

def drop_tables(table_names: List[str] = ['Locations', 'Connections', 'Sessions']) -> None:
    "Drop the tables in the database if they exist."
    with db_conn() as conn:
        cursor = conn.cursor()
        for table_name in table_names:
            try:
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier(table_name)
                ))
                print(f"Table {table_name} dropped.")
            except UndefinedTable:
                print(f"Table {table_name} does not exist.")
        cursor.close()

def parse_seed_data(
        filename: Annotated[str, "The name of the YAML file to seed data from."] = "seed_data.yml",
    ) -> Dict[str, List[Dict[str, Any]]]:
    "Parse seed data from a YAML file and return it as a dictionary."
    seed_data = {"Locations": {}, "Connections": {}}
    try:
        with open(filename, 'r') as f:
            seed_data = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"File {filename} not found.")
    return seed_data


def seed_tables(table_names: List[str] = ['Locations', 'Connections', 'Sessions']) -> None:
    "Seed the tables with some initial data."
    seed_data = parse_seed_data()

    with db_conn() as conn:
    
        if 'Locations' in table_names:
            seed_locations = seed_data["Locations"]
            # iterate through the locations and insert them into the database
            for location in seed_locations:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO Locations (id, name, category, interior_description, exterior_description, notes)
                        VALUES
                        (%(id)s, %(name)s, %(category)s, %(interior_description)s, %(exterior_description)s, %(notes)s)
                    """, {
                        'id': location.get('id', None),
                        'name': location['name'],
                        'category': location.get('category', None),
                        'interior_description': location.get('interior_description', None),
                        'exterior_description': location.get('exterior_description', None),
                        'notes': location.get('notes', None)
                    })
                    print(f"Location {location['id']} \"{location['name']}\" seeded.")
                except UniqueViolation:
                    print(f"Location {location['id']} \"{location['name']}\" already seeded.")
                finally:
                    cursor.close()

        if 'Connections' in table_names:
            seed_connections = seed_data["Connections"]
            # iterate through the connections and insert them into the database
            for connection in seed_connections:
                cursor = conn.cursor()
                try:
                    cursor.execute("""
                        INSERT INTO Connections (id, start_location_id, end_location_id, name, description, category, is_one_way, is_hidden, is_fast_travel_path, describe_end_location_exterior)
                        VALUES
                        (%(id)s, %(start_location_id)s, %(end_location_id)s, %(name)s, %(description)s, %(category)s, %(is_one_way)s, %(is_hidden)s, %(is_fast_travel_path)s, %(describe_end_location_exterior)s)
                    """, {
                        'id': connection.get('id', None),
                        'start_location_id': connection['start_location_id'],
                        'end_location_id': connection['end_location_id'],
                        'name': connection['name'],
                        'description': connection.get('description', None),
                        'category': connection.get('category', None),
                        'is_one_way': connection.get('is_one_way', False),
                        'is_hidden': connection.get('is_hidden', False),
                        'is_fast_travel_path': connection.get('is_fast_travel_path', False),
                        'describe_end_location_exterior': connection.get('describe_end_location_exterior', False),
                    })
                    print(f"Connection {connection['id']} \"{connection['name']}\" seeded.")
                except UniqueViolation:
                    print(f"Connection {connection['id']} \"{connection['name']}\" already seeded.")
                finally:
                    cursor.close()

        if 'Sessions' in table_names:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO Sessions (id, current_location_id, time_in_game)
                    VALUES
                    (%(id)s, %(current_location_id)s, %(time_in_game)s)
                """, {
                    'id': 1,
                    'current_location_id': 23,
                    'time_in_game': '12:00 PM'
                })
                print(f"Session 1 seeded.")
            except UniqueViolation:
                print(f"Session 1 already seeded.")
            finally:
                cursor.close()

def reset_database() -> None:
    "Drop and recreate the database."
    drop_tables()
    drop_database()
    create_database()
    create_tables()
    seed_tables()


if __name__ == "__main__":
    create_database()
    create_tables()
    seed_tables()
