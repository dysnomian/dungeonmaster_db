import os
import yaml

from typing import Annotated, List, Dict, Any

import psycopg2
from psycopg2 import sql

DB_NAME = os.environ.get('DUNGEONMASTER_DB_NAME', 'dungeonmaster_dev')
DB_USER = os.environ.get('DUNGEONMASTER_DB_USER', 'dungeonmaster')
DB_PASSWORD = os.environ.get('DUNGEONMASTER_DB_PASSWORD', 'password')
DB_HOST = os.environ.get('DUNGEONMASTER_DB_HOST', 'localhost')

DB_PARAMS = {
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'dbname': DB_NAME
}

def create_database() -> None:
    "Idempotently create the database if it does not exist."

    initial_connection_params = {
        'user': 'postgres',
        'password': None,
        'host': DB_HOST
    }
    conn = psycopg2.connect(**initial_connection_params)
    cursor = conn.cursor()
    cursor.execute(
        sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(DB_NAME)
        )
    )
    cursor.close()
    conn.close()

def drop_database() -> None:
    "Drop the database if it exists."
    initial_connection_params = {
        'user': 'postgres',
        'password': None,
        'host': DB_HOST
    }

    conn = psycopg2.connect(**initial_connection_params)
    cursor = conn.cursor()
    cursor.execute(
        sql.SQL("DROP DATABASE IF EXISTS {}").format(
            sql.Identifier(DB_NAME)
        )
    )
    cursor.close()
    conn.close()

def create_tables() -> None:
    "Create the tables in the database if they do not exist."
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Locations (
                id SERIAL PRIMARY KEY NOT NULL,
                starting_location_id INTEGER REFERENCES Locations(id) ON DELETE CASCADE,
                parent_id INTEGER REFERENCES Locations(id) ON DELETE CASCADE,
                name VARCHAR(255) NOT NULL UNIQUE,
                category VARCHAR(100),
                description TEXT,
                notes TEXT
            )
        """)
        print("Table Locations created.")
    except psycopg2.errors.DuplicateTable:
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
                is_fast_travel_path BOOLEAN DEFAULT FALSE
            )
        """)
        print("Table Connections created.")
    except psycopg2.errors.DuplicateTable:
        print("Table Connections already exists.")

    cursor.close()
    conn.commit()
    conn.close()

def drop_tables(table_names: List[str] = ['Locations', 'Connections']) -> None:
    "Drop the tables in the database if they exist."
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    for table_name in table_names:
        try:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(
                sql.Identifier(table_name)
            ))
            print(f"Table {table_name} dropped.")
        except psycopg2.errors.UndefinedTable:
            print(f"Table {table_name} does not exist.")
    cursor.close()
    conn.commit()
    conn.close()

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


def seed_tables(table_names: List[str] = ['Locations', 'Connections']) -> None:
    "Seed the tables with some initial data."
    conn = psycopg2.connect(**DB_PARAMS)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    seed_data = parse_seed_data()

    if 'Locations' in table_names:
        seed_locations = seed_data["Locations"]
        # iterate through the locations and insert them into the database
        for location in seed_locations:
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO Locations (id, name, category, description, notes)
                    VALUES
                    (%(id)s, %(name)s, %(category)s, %(description)s, %(notes)s)
                """, {
                    'id': location.get('id', None),
                    'name': location['name'],
                    'category': location.get('category', None),
                    'description': location.get('description', None),
                    'notes': location.get('notes', None)
                })
                print(f"Location {location['id']} \"{location['name']}\" seeded.")
            except psycopg2.errors.UniqueViolation:
                print(f"Location {location['id']} \"{location['name']}\" already seeded.")
            finally:
                cursor.close()

    if 'Connections' in table_names:
        seed_connections = seed_data["Connections"]
        # iterate through the connections and insert them into the database
        for connection in seed_connections:
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO Connections (id, start_location_id, end_location_id, name, description, category, is_one_way, is_hidden, is_fast_travel_path)
                    VALUES
                    (%(id)s, %(start_location_id)s, %(end_location_id)s, %(name)s, %(description)s, %(category)s, %(is_one_way)s, %(is_hidden)s, %(is_fast_travel_path)s)
                """, {
                    'id': connection.get('id', None),
                    'start_location_id': connection['start_location_id'],
                    'end_location_id': connection['end_location_id'],
                    'name': connection['name'],
                    'description': connection.get('description', None),
                    'category': connection.get('category', None),
                    'is_one_way': connection.get('is_one_way', False),
                    'is_hidden': connection.get('is_hidden', False),
                    'is_fast_travel_path': connection.get('is_fast_travel_path', False)
                })
                print(f"Connection {connection['id']} \"{connection['name']}\" seeded.")
            except psycopg2.errors.UniqueViolation:
                print(f"Connection {connection['id']} \"{connection['name']}\" already seeded.")
            finally:
                cursor.close()

    conn.commit()
    conn.close()

def reset_database() -> None:
    "Drop and recreate the database."
    # drop_database()
    # create_database()
    drop_tables()
    create_tables()
    seed_tables()


if __name__ == "__main__":
    create_database()
    create_tables()
