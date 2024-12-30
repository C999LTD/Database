import mysql.connector
from datetime import datetime

# This code is not tested. need examples. this code updates Table 10

# Database connection
db = mysql.connector.connect(
    user='your_user',
    password='your_password',
    host='your_host',
    database='your_database'
)
cursor = db.cursor()

# Fetch current officers from the database
cursor.execute("SELECT person_id FROM unique_officers_per_company")
existing_officers = set(row[0] for row in cursor.fetchall())

# Example new data (replace this with your actual new data source)
new_data = [
    {'company_number': '12345678', 'person_id': 'P001', 'name': 'John Doe', 'role': 'Director', 'appointed_on': '2024-01-01', 'is_corporate': False},
    {'company_number': '12345678', 'person_id': 'P002', 'name': 'Jane Smith', 'role': 'PSC', 'notified_on': '2024-01-05', 'is_corporate': False}
]

# Compare and insert new entries
for officer in new_data:
    if officer['person_id'] not in existing_officers:
        cursor.execute(
            """
            INSERT INTO new_officers_pscs (company_number, person_id, name, role, appointed_on, notified_on, is_corporate)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (officer['company_number'], officer['person_id'], officer['name'], officer['role'], officer.get('appointed_on'), officer.get('notified_on'), officer['is_corporate'])
        )
        db.commit()

# Fetch new officers added yesterday
cursor.execute("""
    SELECT *
    FROM new_officers_pscs
    WHERE DATE(created_at) = CURDATE() - INTERVAL 1 DAY
""")
yesterday_officers = cursor.fetchall()

# Process and print the results
if yesterday_officers:
    print("Officers added yesterday:")
    for officer in yesterday_officers:
        print(officer)
else:
    print("No officers were added yesterday.")

# Close connections
cursor.close()
db.close()
