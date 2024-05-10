from psycopg2.errors import DuplicateTable

from dungeonmaster_db.adapter import DbConnection as db_conn

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

create_tables()