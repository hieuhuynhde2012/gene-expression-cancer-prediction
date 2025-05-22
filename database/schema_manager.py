from pathlib import Path


MYSQL_SCHEMA_PATH = Path(r"F:\gene-expression-cancer-prediction\oltp_db\mysql\schema.sql")
POSTGRES_SCHEMA_PATH = Path(r"F:\gene-expression-cancer-prediction\oltp_db\postgresql\schema.sql")

# ========== MYSQL ========== #
def create_mysql_schema(connection, cursor):
    database = "bio_project"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    connection.commit()
    connection.database = database

    try:
        with open(MYSQL_SCHEMA_PATH, "r", encoding="utf-8") as sql_file:
            sql_script = sql_file.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"Executed command: {cmd}")
            connection.commit()
            print("MySQL schema created successfully.")
    except Exception as e:
        connection.rollback()
        print(f"Error executing MySQL schema: {e}")

def validate_mysql_schema(cursor):
    cursor.execute("SHOW TABLES")
    # Lấy tên bảng từ dict trả về
    tables = {list(table.values())[0] for table in cursor.fetchall()}
    required_tables = {"genes", "patients", "samples", "gene_expression", "transcripts"}

    missing = required_tables - tables
    if missing:
        print(f"Missing tables in MySQL: {missing}")
        return False

    cursor.execute("SELECT * FROM patients LIMIT 1")
    if not cursor.fetchone():
        print("MySQL: Patients table is empty.")
        return False

    print("Validated schema in MySQL.")
    return True

# ========== POSTGRESQL ========== #
def create_postgres_schema(connection, cursor):
    connection.autocommit = True
    cursor.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")

    try:
        with open(POSTGRES_SCHEMA_PATH, "r", encoding="utf-8") as sql_file:
            sql_script = sql_file.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"Executed command: {cmd}")
            print("PostgreSQL schema created successfully.")
    except Exception as e:
        print(f"Error executing PostgreSQL schema: {e}")
        connection.rollback()

def validate_postgres_schema(cursor):
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    tables = {row[0] for row in cursor.fetchall()}
    required_tables = {"dimpatient", "dimgene", "dimdate", "factgeneexpression"}

    missing = required_tables - tables
    if missing:
        print(f"Missing tables in PostgreSQL: {missing}")
        return False

    cursor.execute("SELECT * FROM dimpatient LIMIT 1")
    if not cursor.fetchone():
        print("PostgreSQL: DimPatient table is empty.")
        return False

    print("Validated schema in PostgreSQL.")
    return True
