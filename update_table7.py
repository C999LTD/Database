import mysql.connector
import logging
import sys
import json
from difflib import SequenceMatcher
from typing import Dict

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

def normalize_name(name: str) -> str:
    """Normalize names to handle different formats."""
    name = name.lower().strip()

    # Remove titles
    titles = ['mr', 'mrs', 'ms', 'dr', 'prof']
    for title in titles:
        if name.startswith(title + ' '):
            name = name[len(title):].strip()

    # Handle comma-separated format (SURNAME, First names)
    if ',' in name:
        surname, firstnames = name.split(',', 1)
        name = f"{firstnames.strip()} {surname.strip()}"

    return ' '.join(name.split())

def normalize_full_name(name: str) -> str:
    """
    Normalize a full name string by converting to lowercase and stripping extra spaces.
    """
    return " ".join(name.lower().split())

def names_are_similar(name1: str, name2: str, threshold: float = 0.8) -> bool:
    """
    Compare two names for similarity based on a threshold.
    """
    name1_normalized = normalize_full_name(name1)
    name2_normalized = normalize_full_name(name2)
    similarity = SequenceMatcher(None, name1_normalized, name2_normalized).ratio()
    return similarity >= threshold


def update_unique_officers(cursor, company_number: str):
    try:
        officers_query = """
            SELECT
                o.person_id,
                o.name,
                o.is_corporate,
                COUNT(DISTINCT CASE
                    WHEN o.is_corporate = 1 AND r.role = 'corporate-director' THEN 'Director'
                    WHEN o.is_corporate = 1 AND r.role = 'corporate-secretary' THEN 'Secretary'
                    ELSE r.role
                END) as role_count,
                GROUP_CONCAT(DISTINCT CASE
                    WHEN o.is_corporate = 1 AND r.role = 'corporate-director' THEN 'Director'
                    WHEN o.is_corporate = 1 AND r.role = 'corporate-secretary' THEN 'Secretary'
                    ELSE r.role
                END ORDER BY r.role ASC) AS roles,
                o.date_of_birth,
                o.address_line_1,
                o.address_line_2,
                o.locality,
                o.postal_code,
                o.country,
                MAX(CASE WHEN r.role != 'PSC' THEN r.appointed_on END) AS last_appointed_on,
                MAX(CASE WHEN r.role != 'PSC' THEN r.resigned_on END) AS last_resigned_on,
                MAX(CASE WHEN r.role = 'PSC' THEN r.notified_on END) AS last_notified_on,
                MAX(CASE WHEN r.role = 'PSC' THEN r.ceased_on END) AS last_ceased_on
            FROM officers o
            LEFT JOIN officer_roles r ON o.officer_id = r.officer_id
                AND o.company_number = r.company_number
            WHERE o.company_number = %s
            AND (
                (r.role != 'PSC' AND (r.resigned_on IS NULL OR r.resigned_on > CURDATE()))
                OR
                (r.role = 'PSC' AND (r.ceased_on IS NULL OR r.ceased_on > CURDATE()))
            )
            GROUP BY o.person_id, o.name;
        """

        cursor.execute(officers_query, (company_number,))
        results = cursor.fetchall()

        officers_by_name = {}
        for row in results:
            person_id, name, is_corporate, role_count, roles, dob, addr1, addr2, locality, postal, country, appointed_on, resigned_on, notified_on, ceased_on = row

            normalized_name = normalize_name(name)

            if normalized_name not in officers_by_name:
                officers_by_name[normalized_name] = row
            else:
                existing = officers_by_name[normalized_name]
                combined_roles = f"{existing[4]},{roles}" if existing[4] and roles else (existing[4] or roles)
                combined_count = existing[3] + role_count

                officers_by_name[normalized_name] = (
                    existing[0],
                    existing[1],
                    existing[2],
                    combined_count,
                    combined_roles,
                    existing[5] or dob,
                    existing[6] or addr1,
                    existing[7] or addr2,
                    existing[8] or locality,
                    existing[9] or postal,
                    existing[10] or country,
                    max(filter(None, [existing[11], appointed_on])) if any(x is not None for x in [existing[11], appointed_on]) else None,
                    max(filter(None, [existing[12], resigned_on])) if any(x is not None for x in [existing[12], resigned_on]) else None,
                    max(filter(None, [existing[13], notified_on])) if any(x is not None for x in [existing[13], notified_on]) else None,
                    max(filter(None, [existing[14], ceased_on])) if any(x is not None for x in [existing[14], ceased_on]) else None
                )

        for row in officers_by_name.values():
            person_id, name, is_corporate, role_count, roles, dob, addr1, addr2, locality, postal, country, appointed_on, resigned_on, notified_on, ceased_on = row

            insert_query = """
                INSERT INTO unique_officers_per_company (
                    company_number, person_id, name, is_corporate, role_count, roles,
                    date_of_birth, address_line_1, address_line_2, locality,
                    postal_code, country, last_appointed_on, last_resigned_on,
                    last_notified_on, last_ceased_on
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    is_corporate = VALUES(is_corporate),
                    role_count = VALUES(role_count),
                    roles = VALUES(roles),
                    date_of_birth = COALESCE(VALUES(date_of_birth), date_of_birth),
                    address_line_1 = COALESCE(VALUES(address_line_1), address_line_1),
                    address_line_2 = COALESCE(VALUES(address_line_2), address_line_2),
                    locality = COALESCE(VALUES(locality), locality),
                    postal_code = COALESCE(VALUES(postal_code), postal_code),
                    country = COALESCE(VALUES(country), country),
                    last_appointed_on = VALUES(last_appointed_on),
                    last_resigned_on = VALUES(last_resigned_on),
                    last_notified_on = VALUES(last_notified_on),
                    last_ceased_on = VALUES(last_ceased_on)
            """

            cursor.execute(insert_query, (
                company_number, person_id, name, is_corporate, role_count, roles,
                dob, addr1, addr2, locality, postal, country,
                appointed_on, resigned_on, notified_on, ceased_on
            ))

    except Exception as e:
        logging.error(f"Error updating unique_officers_per_company: {str(e)}")
        raise

def main():
    try:
        input_data = sys.stdin.read()
        data = json.loads(input_data)

        company_number = data['company_details']['company_number']

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        update_unique_officers(cursor, company_number)

        connection.commit()
        logging.info(f"Unique officers updated successfully for company {company_number}")

    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse input JSON: {str(e)}")
    except mysql.connector.Error as e:
        logging.error(f"Database error: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    main()
