import sys
import json
import mysql.connector

# Database connection parameters
db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',  # Replace with your MySQL password
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

def update_registered_office_table(data):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Extract data for registered_office_address table
        company_details = data.get("company_details", {})
        company_number = company_details.get("company_number")
        registered_office = company_details.get("registered_office_address", {})

        if not registered_office:
            print(f"No registered office data for company {company_number}. Skipping update.")
            return

        house_name_number = registered_office.get("address_line_1")
        street_name = registered_office.get("address_line_2")
        locality = registered_office.get("locality")
        town_city = registered_office.get("postal_town")
        county = registered_office.get("region")
        postcode = registered_office.get("postal_code")

        # Insert or update the registered_office_address table
        query = """
        INSERT INTO registered_office_address (company_number, house_name_number, street_name, locality, town_city, county, postcode)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        house_name_number = VALUES(house_name_number),
        street_name = VALUES(street_name),
        locality = VALUES(locality),
        town_city = VALUES(town_city),
        county = VALUES(county),
        postcode = VALUES(postcode)
        """
        cursor.execute(query, (company_number, house_name_number, street_name, locality, town_city, county, postcode))
        connection.commit()
        print(f"Registered office data for company {company_number} updated successfully.")

    except mysql.connector.Error as error:
        print(f"Error updating registered office address table: {error}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    # Read JSON input from stdin
    input_data = sys.stdin.read()
    data = json.loads(input_data)
    update_registered_office_table(data)
