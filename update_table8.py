import mysql.connector
from datetime import datetime, timedelta

# Database configuration
db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

# This code is not tested. We need examples to test this. this code updates table 9


def log_yesterdays_resignations():
    # Connect to the database
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Step 1: Calculate yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).date()

    # Step 2: Fetch officers who resigned yesterday
    fetch_query = """
    SELECT
        orl.officer_id,
        o.person_id,
        orl.company_number,
        orl.role,
        orl.resigned_on,
        o.name,
        o.is_corporate
    FROM officer_roles orl
    JOIN officers o ON orl.officer_id = o.officer_id
    WHERE orl.resigned_on = %s;
    """
    cursor.execute(fetch_query, (yesterday,))
    resigned_roles = cursor.fetchall()

    # Step 3: Process each resigned officer
    for officer_id, person_id, company_number, role, resigned_on, name, is_corporate in resigned_roles:
        # Log the resignation in `resigned_officers`
        log_query = """
        INSERT INTO resigned_officers (company_number, person_id, officer_id, name, is_corporate, resigned_role, resigned_on)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(log_query, (company_number, person_id, officer_id, name, is_corporate, role, resigned_on))

        # Fetch all remaining active roles for this officer
        roles_query = """
        SELECT GROUP_CONCAT(DISTINCT role SEPARATOR ', ') AS active_roles
        FROM officer_roles
        WHERE officer_id = %s AND (resigned_on IS NULL OR resigned_on > CURDATE());
        """
        cursor.execute(roles_query, (officer_id,))
        active_roles = cursor.fetchone()[0]  # Comma-separated list of active roles

        # Step 4: Update the `roles` field in `unique_officers_per_company`
        update_query = """
        UPDATE unique_officers_per_company
        SET roles = %s
        WHERE company_number = %s AND person_id = %s;
        """
        cursor.execute(update_query, (active_roles, company_number, person_id))

    # Commit the changes
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()

# Run the function
log_yesterdays_resignations()
