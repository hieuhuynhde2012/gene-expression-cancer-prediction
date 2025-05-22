import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.schema_manager import (create_mysql_schema, validate_mysql_schema)
from config.database_config import get_database_config
from database.mysql_connect import MySQLConnect
# from database.postgres_connect import PostgresConnect

def main(config):
    #===============MySQL=====================
    with MySQLConnect(
        config["mysql"].host,
        config["mysql"].port,
        config["mysql"].user,
        config["mysql"].password,
    ) as mysql_client:
        connection, cursor = mysql_client.connection, mysql_client.cursor
        if not connection or not cursor:
            print("Failed to connect to MySQL database.")
            return
        # Create MySQL schema
        create_mysql_schema(connection, cursor)
        cursor.execute(
            "INSERT INTO patients (patient_id, case_id, age_at_diagnosis, gender, treatment_outcome, survival_time_months) VALUES (%s, %s, %s, %s, %s, %s)", 
            ("P001", "C001", 45, "M", "Complete Response", 12)
        )
        connection.commit()
        print("Inserted sample data into MySQL patients table.")
        validate_mysql_schema(cursor)
        
        
if __name__ == "__main__":
    config = get_database_config()
    main(config)