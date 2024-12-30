import subprocess
import json
import pandas as pd
import requests
import mysql.connector
from datetime import datetime

# Database connection parameters
db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',  # Replace with your MySQL password
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

# Companies House API key
api_key = 'baff87ec-0ba3-415c-9cb4-974ccf25c883'
base_url = 'https://api.company-information.service.gov.uk'

# Format date to DD Month YYYY
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

# Display dates optional  DD Month YYYY
def format_date_for_display(date_input):
    if not date_input:
        return None
    try:
        date_object = datetime.strptime(date_input, "%Y-%m-%d")
        return date_object.strftime("%d %B %Y")  # Format as DD Month YYYY
    except ValueError:
        return None



# Preprocess data to format dates
def preprocess_dates(data):
    for section in ["company_details", "officer_details", "psc_details"]:
        if section in data:
            if isinstance(data[section], dict):
                for key, value in data[section].items():
                    if "date" in key and value:
                        data[section][key] = format_date(value)  # Store as YYYY-MM-DD
            elif isinstance(data[section], list):
                for item in data[section]:
                    for key, value in item.items():
                        if "date" in key and value:
                            item[key] = format_date(value)  # Store as YYYY-MM-DD
    return data


# Load company numbers from Excel
def get_company_numbers():
    excel_file = 'company_numbers.xlsx'  # Your Excel file name
    df = pd.read_excel(excel_file, dtype=str, skiprows=1, header=None)  # Treat all columns as strings, skip header row
    df.columns = ['company_number'] + [f'col_{i}' for i in range(1, len(df.columns))]  # Name the first column and assign default names to others
    print("Columns in Excel file:", df.columns)  # Debugging line to check column names
    company_numbers = [num.zfill(8) for num in df['company_number'].astype(str)]  # Ensure 8 characters
    return company_numbers

# Save data to temporary staging table
def save_to_staging_table(data):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO api_response (response_data, processed_status, company_number)
        VALUES (%s, %s, %s)
        """
        json_data = json.dumps(data)  # Convert Python dictionary to JSON string
        cursor.execute(insert_query, (json_data, 'Pending', data['company_details']['company_number']))
        connection.commit()
        print("Data saved to temporary staging table.")
    except mysql.connector.Error as error:
        print(f"Error saving to database: {error}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Fetch data from Companies House API
def fetch_company_data(company_number):
    url = f"{base_url}/company/{company_number}"
    response = requests.get(url, auth=(api_key, ''))
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {company_number}: {response.status_code}")
        return None

# Fetch officers from Companies House API
def fetch_officer_data(company_number):
    url = f"{base_url}/company/{company_number}/officers"
    response = requests.get(url, auth=(api_key, ''))
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch officers for {company_number}: {response.status_code}")
        return {}

# Fetch persons with significant control (PSC) from Companies House API
def fetch_psc_data(company_number):
    url = f"{base_url}/company/{company_number}/persons-with-significant-control"
    response = requests.get(url, auth=(api_key, ''))
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch PSC data for {company_number}: {response.status_code}")
        return {}

# Main process for fetching company, officer, and PSC data
def main():
    try:
        company_numbers = get_company_numbers()
        for company_number in company_numbers:
            print(f"Processing company number: {company_number}")

            # Fetch company data
            company_data = fetch_company_data(company_number)
            if not company_data:
                continue

            # Fetch officer data
            officer_data = fetch_officer_data(company_number)

            # Fetch PSC data
            psc_data = fetch_psc_data(company_number)

            # Combine company, officer, and PSC data
            combined_data = {
                "company_details": company_data,
                "officer_details": officer_data,
                "psc_details": psc_data
            }

            # Preprocess combined data to format dates
            combined_data = preprocess_dates(combined_data)

            # Save combined data to the staging table
            save_to_staging_table(combined_data)

            # Call subprocess to update Company Name Table (Table 1)
            subprocess.run([
                "python3", "update_table1.py"
            ], input=json.dumps(combined_data), text=True)

            # Call subprocess to update Previous Name Table (Table 2)
            subprocess.run([
                "python3", "update_table2.py"
            ], input=json.dumps(combined_data), text=True)

            # Call subprocess to update Confirmation Statement Table (Table 3)
            subprocess.run([
                "python3", "update_table3.py"
            ], input=json.dumps(combined_data), text=True)

            # Call subprocess to update Accounts Table (Table 4)
            subprocess.run([
                "python3", "update_table4.py"
            ], input=json.dumps(combined_data), text=True)

             # Call subprocess to update Registered Office Table (Table 5)
            subprocess.run([
                "python3", "update_table5.py"
            ], input=json.dumps(combined_data), text=True)

            # Call subprocess to update Officers Table (Table 6 & 7)
            subprocess.run([
                "python3", "update_table6.py"
            ], input=json.dumps(combined_data), text=True)

            # Call subprocess to update Unique Officers Table (Table 8)
            subprocess.run([
                "python3", "update_table7.py"
            ], input=json.dumps(combined_data), text=True)

    except Exception as e:
        print(f"An error occurred in the main process: {e}")

if __name__ == "__main__":
    main()