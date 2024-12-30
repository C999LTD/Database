import sys
import json
import mysql.connector
from datetime import datetime
import dateutil.parser  # Added for robust date parsing

# Database connection parameters
db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

def format_date(date_string):
    if not date_string:
        print("Date string is None or empty.")
        return None
    try:
        # Use dateutil.parser for robust date parsing
        date_object = dateutil.parser.parse(date_string)
        print(f"Parsed date string '{date_string}' to '{date_object}'.")
        return date_object.strftime("%Y-%m-%d")  # Format as MySQL-compatible YYYY-MM-DD
    except (ValueError, TypeError) as e:
        print(f"Invalid date format: {date_string} | Error: {e}")
        return None

def update_accounts_table(data):
    try:
        print("Establishing database connection...")
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Extract data for accounts table
        # print("Extracting company details from JSON data...")
        company_details = data.get("company_details", {})
        company_number = company_details.get("company_number")
        accounts = company_details.get("accounts", {})

        if not accounts:
            print(f"No accounts data for company {company_number}. Skipping update.")
            return

        # Debug print to check if 'made_up_to' exists
        # print(f"Accounts data for company {company_number}: {accounts}")

        last_accounts_date = format_date(accounts.get("last_accounts", {}).get("made_up_to"))
        next_accounts_date = format_date(accounts.get("next_accounts", {}).get("period_end_on"))
        due_date = format_date(accounts.get("next_accounts", {}).get("due_on"))

        # Debug print to confirm parsed dates
        # print(f"Parsed last_accounts_date: {last_accounts_date}")
        # print(f"Parsed next_accounts_date: {next_accounts_date}")
        # print(f"Parsed due_date: {due_date}")

        # Check if all dates are None
        if not last_accounts_date and not next_accounts_date and not due_date:
            print("All parsed dates are None. Skipping database update.")
            return

        # Insert or update the accounts table
        query = """
        INSERT INTO accounts (company_number, last_accounts_date, next_accounts_date, due_date)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        last_accounts_date = VALUES(last_accounts_date),
        next_accounts_date = VALUES(next_accounts_date),
        due_date = VALUES(due_date)
        """
        # print(f"Executing query: {query}")
        # print(f"With values: {company_number}, {last_accounts_date}, {next_accounts_date}, {due_date}")
        cursor.execute(query, (company_number, last_accounts_date, next_accounts_date, due_date))
        connection.commit()
        print(f"Accounts data for company {company_number} updated successfully.")

    except mysql.connector.Error as error:
        print(f"Error updating accounts table: {error}")
    finally:
        if connection.is_connected():
            print("Closing database connection...")
            cursor.close()
            connection.close()

if __name__ == "__main__":
    # print("Reading JSON input...")
    input_data = sys.stdin.read()
    # print(f"Raw JSON input: {input_data}")
    data = json.loads(input_data)
    # print("Starting accounts table update process...")
    update_accounts_table(data)
    print("Process completed.")
