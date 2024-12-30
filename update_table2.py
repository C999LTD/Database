import sys
import json
import mysql.connector

db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

def update_previous_names_table(data):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        company_details = data.get("company_details", {})
        company_number = company_details.get("company_number")
        previous_names = company_details.get("previous_company_names", [])

        if not previous_names:
            print(f"No previous names for company {company_number}. Skipping update.")
            return

        # Extract names
        previous_name_columns = []
        for i, prev_name in enumerate(previous_names[:5]):
            name = prev_name.get('name')
            print(f"Processing name {i+1}: {name}")
            previous_name_columns.append(name)

        # Pad with None if less than 5 names
        while len(previous_name_columns) < 5:
            previous_name_columns.append(None)

        print("Final values to insert:", previous_name_columns)

        query = """
        INSERT INTO previous_names (company_number, previous_name_1, previous_name_2, previous_name_3, previous_name_4, previous_name_5)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        previous_name_1 = VALUES(previous_name_1),
        previous_name_2 = VALUES(previous_name_2),
        previous_name_3 = VALUES(previous_name_3),
        previous_name_4 = VALUES(previous_name_4),
        previous_name_5 = VALUES(previous_name_5)
        """
        cursor.execute(query, (company_number, *previous_name_columns))
        connection.commit()
        print(f"Previous names for company {company_number} updated successfully.")

    except mysql.connector.Error as error:
        print(f"Error updating previous names table: {error}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    input_data = sys.stdin.read()
    # print("Raw input data:", input_data)  # Commented
    data = json.loads(input_data)
    update_previous_names_table(data)