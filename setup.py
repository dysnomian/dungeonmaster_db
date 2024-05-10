from importlib.metadata import entry_points
from setuptools import setup, find_packages

setup(
    name='dungeonmaster_db',
    version='0.1.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "psycopg2",
        "pyyaml"
    ],
    entry_points={
        'console_scripts': [
            'reset_dungeonmaster_db=dungeonmaster_db.build_db:reset_database',
            'seed_dungeonmaster_db=dungeonmaster_db.build_db:seed_database'
        ]
    }
)