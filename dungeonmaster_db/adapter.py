"""
Adapter module for the Dungeonmaster database, to be imported by setup scripts, models, queries, etc.
"""

import os

from typing import Dict, Union

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
from psycopg2.errors import DuplicateDatabase, InsufficientPrivilege

DUNGEONMASTER_ENV = os.environ.get('DUNGEONMASTER_ENV', 'dev')

DB_NAME = os.environ.get('DUNGEONMASTER_DB_NAME', f"dungeonmaster_{DUNGEONMASTER_ENV}")
DB_HOST = os.environ.get('DUNGEONMASTER_DB_HOST', 'localhost')

DB_PARAMS = {
    'user': os.environ.get('DUNGEONMASTER_DB_USER', f"dungeonmaster_{DUNGEONMASTER_ENV}"),
    'password': os.environ.get('DUNGEONMASTER_DB_PASSWORD', None),
    'host': DB_HOST,
    'dbname': DB_NAME
}

SUPERUSER_DB_PARAMS = {
    'user': os.environ.get('POSTGRES_SUPERUSER', 'postgres'),
    'password': os.environ.get('POSTGRES_PASSWORD', None),
    'host': DB_HOST,
    'dbname': 'postgres'
}

class DbConnection:
    """
    Convience class for managing database connections. Configures itself with the
    DB_PARAMS environment variables.
    
    Example usage:
    with DbConnection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        users = cursor.fetchall()
    """

    def __init__(self, db_params: Dict[str, Union[str, None]] = {}):
        self.db_params = {
            "user": db_params.get('user', DB_PARAMS['user']),
            "password": db_params.get('password', DB_PARAMS['password']),
            "host": db_params.get('host', DB_PARAMS['host']),
            "dbname": db_params.get('dbname', DB_PARAMS['dbname'])
        }

    def __enter__(self):
        self.conn = psycopg2.connect(**self.db_params)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

class SuperuserDbConnection:
    """
    Convience class for managing superuser database connections. Configures itself with the
    SUPERUSER_DB_PARAMS environment variables.

    Example usage:
    with SuperuserDbConnection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users")
        users = cursor.fetchall()
    """
    
    def __init__(self, db_params: Dict[str, Union[str, None]] = {}):
        self.db_params = {
            "user": db_params.get('user', SUPERUSER_DB_PARAMS['user']),
            "password": db_params.get('password', SUPERUSER_DB_PARAMS['password']),
            "host": db_params.get('host', SUPERUSER_DB_PARAMS['host']),
            "dbname": db_params.get('dbname', SUPERUSER_DB_PARAMS['dbname'])
        }
    
    def __enter__(self):
        self.conn = psycopg2.connect(**self.db_params)
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

def create_database():
    "Idempotently create the database if it does not exist."

    with SuperuserDbConnection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(DB_NAME)
                )
            )
            print(f"Database {DB_NAME} created.")
        except DuplicateDatabase:
            print(f"Database {DB_NAME} already exists.")
        except InsufficientPrivilege:
            print(f"User does not have permission to create database {DB_NAME}.")
        finally:
            cursor.close()

def drop_database():
    "Drop the database if it exists."

    with SuperuserDbConnection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(DB_NAME)
                )
            )
            print(f"Database {DB_NAME} dropped.")
        except InsufficientPrivilege:
            print(f"User does not have permission to drop database {DB_NAME}.")
        finally:
            cursor.close()