import sys
import json
import mysql.connector

# Database connection parameters
db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

def update_company_table(data):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Extract data for company_name table
        company_details = data.get("company_details", {})
        company_number = company_details.get("company_number")
        company_name = company_details.get("company_name")
        company_status = company_details.get("company_status")
        jurisdiction = company_details.get("jurisdiction", "")
        sic_codes = company_details.get("sic_codes", [])

        # Assign SIC codes to appropriate columns
        sic_code_1 = sic_codes[0] if len(sic_codes) > 0 else None
        sic_code_2 = sic_codes[1] if len(sic_codes) > 1 else None
        sic_code_3 = sic_codes[2] if len(sic_codes) > 2 else None
        sic_code_4 = sic_codes[3] if len(sic_codes) > 3 else None

        # Insert or update the company_name table
        query = """
        INSERT INTO company_name (company_number, company_name, company_status, jurisdiction, sic_code_1, sic_code_2, sic_code_3, sic_code_4)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        company_name = VALUES(company_name),
        company_status = VALUES(company_status),
        jurisdiction = VALUES(jurisdiction),
        sic_code_1 = VALUES(sic_code_1),
        sic_code_2 = VALUES(sic_code_2),
        sic_code_3 = VALUES(sic_code_3),
        sic_code_4 = VALUES(sic_code_4)
        """
        cursor.execute(query, (company_number, company_name, company_status, jurisdiction, sic_code_1, sic_code_2, sic_code_3, sic_code_4))
        connection.commit()
        print(f"Company {company_number} updated successfully.")

    except mysql.connector.Error as error:
        print(f"Error updating company table: {error}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    # Read JSON input from stdin
    input_data = sys.stdin.read()
    data = json.loads(input_data)
    update_company_table(data)
