import sys
import json
import mysql.connector
from datetime import datetime

# Database connection parameters
db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',  # Replace with your MySQL password
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

# Format date for storage in the database (YYYY-MM-DD)
def format_date(date_input):
    if not date_input:
        return None
    if isinstance(date_input, int):  # Handle integer year
        return f"{date_input}-01-01"  # Default to January 1st
    try:
        date_object = datetime.strptime(date_input, "%Y-%m-%d")
        return date_object.strftime("%Y-%m-%d")  # Store as YYYY-MM-DD
    except ValueError:
        return None

def update_confirmation_statement_table(data):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Extract data for confirmation_statement table
        company_details = data.get("company_details", {})
        company_number = company_details.get("company_number")
        confirmation_statement = company_details.get("confirmation_statement", {})

        if not confirmation_statement:
            print(f"No confirmation statement data for company {company_number}. Skipping update.")
            return

        # Use format_date for proper date formatting
        last_statement_date = format_date(confirmation_statement.get("last_made_up_to"))
        next_statement_date = format_date(confirmation_statement.get("next_made_up_to"))
        due_date = format_date(confirmation_statement.get("next_due"))

        # Insert or update the confirmation_statement table
        query = """
        INSERT INTO confirmation_statement (company_number, last_statement_date, next_statement_date, due_date)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        last_statement_date = VALUES(last_statement_date),
        next_statement_date = VALUES(next_statement_date),
        due_date = VALUES(due_date)
        """
        cursor.execute(query, (company_number, last_statement_date, next_statement_date, due_date))
        connection.commit()
        print(f"Confirmation statement for company {company_number} updated successfully.")

    except mysql.connector.Error as error:
        print(f"Error updating confirmation statement table: {error}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    # Read JSON input from stdin
    input_data = sys.stdin.read()
    data = json.loads(input_data)
    update_confirmation_statement_table(data)
