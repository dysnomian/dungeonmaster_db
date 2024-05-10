import dungeonmaster_db.adapter as adapter
import dungeonmaster_db.build_db as build_db

def main():
    build_db.reset_database()


if __name__ == "__main__":
    main()