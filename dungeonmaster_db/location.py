from typing import List

from dungeonmaster_db.adapter import DbConnection as db_conn
from dungeonmaster_db.table import Table

locations_table = Table(
    "locations", 
    """
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
    """
)
locations_table.create_table()

class Location:
    """
    Model for rows in the Locations table.
    """
    def columns(self):
        return [
            "id",
            "starting_location_id",
            "parent_id",
            "name",
            "category",
            "interior_description",
            "exterior_description",
            "notes"
        ]

    def __init__(self, id, **kwargs):
        self.id = id
        self.reload()

    def __repr__(self):
        return f"<Location {self.id} name={self.name}> category={self.category} interior_description={self.interior_description} exterior_description={self.exterior_description} notes={self.notes} exits={self.exits()}"

    def reload(self):
        "Reload the row from the database."
        with db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM locations WHERE id = %s",
                (self.id,)
            )
            row = cursor.fetchone()
            cursor.close()

        if row is not None:
            for column, value in zip(self.columns(), row):
                setattr(self, column, value)

    def exits(self) -> List:
        "Return the exits for this location."
        connections = []
        with db_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM connections WHERE start_location_id = %s OR end_location_id = %s",
                (self.id, self.id)
            )
            result = cursor.fetchall()
            for row in result:
                connections.append({
                    "id": row[0],
                    "exit_location": Location(row[1]) if row[1] == self.id else Location(row[2]),
                    "start_location_id": row[1],
                    "end_location_id": row[2],
                    "name": row[3],
                    "description": row[4],
                    "category": row[5],
                    "is_one_way": row[6],
                    "is_hidden": row[7],
                    "is_fast_travel_path": row[8],
                    "describe_end_location_exterior": row[9]
                })

        return connections





    





    

