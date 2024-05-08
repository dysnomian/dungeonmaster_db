"""Adapter module for the Dungeonmaster database, to be imported by models."""

import os



DB_NAME = os.environ.get('DUNGEONMASTER_DB_NAME', 'dungeonmaster_dev')

DB_PARAMS = {
    'user': os.environ.get('DUNGEONMASTER_DB_USER', 'dungeonmaster'),
    'password': os.environ.get('DUNGEONMASTER_DB_PASSWORD', 'password'),
    'host': os.environ.get('DUNGEONMASTER_DB_HOST', 'localhost'),
    'dbname': DB_NAME
}