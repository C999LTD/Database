import json
import mysql.connector
import logging
import sys
import uuid
from typing import Dict

logging.basicConfig(level=logging.DEBUG,
                   format='%(asctime)s - %(levelname)s - %(message)s')

db_config = {
    'user': 'Companies999',
    'password': 'U5QE0#8!7eDTwjGC6FZTVpJ7w8oAJg^',
    'host': 'Companies999.mysql.eu.pythonanywhere-services.com',
    'database': 'Companies999$NewMasters'
}

def is_corporate_entity(name: str, role: str = None, kind: str = None) -> bool:
    corporate_indicators = [
        'LIMITED', 'LTD', 'PLC', 'LLC', 'CORPORATION', 'CORP', 'INC',
        'INCORPORATED', 'LP', 'LLP', '& CO', 'AND CO', 'GMBH', 'SA', 'AG', 'NV'
    ]
    corporate_roles = ['corporate-secretary', 'corporate-director', 'corporate-nominee-director']
    corporate_kinds = ['corporate-entity-person-with-significant-control']

    if (role and role in corporate_roles) or (kind and kind in corporate_kinds):
        return True

    name_words = set(name.upper().split())
    return any(indicator in name_words for indicator in corporate_indicators)

def map_role(officer_role: str) -> str:
    role_mapping = {
        'director': 'Director',
        'corporate-director': 'Director',
        'secretary': 'Secretary',
        'corporate-secretary': 'Secretary'
    }
    return role_mapping.get(officer_role.lower(), officer_role)

def update_officers_and_roles(cursor, officer: Dict, company_number: str, company_name: str):
    try:
        person_id = str(officer.get('person_id', uuid.uuid4()))
        name = str(officer.get('name', ''))
        role = str(officer.get('officer_role', ''))
        is_corporate = bool(officer.get('is_corporate', False)) or is_corporate_entity(name, role)
        officer_id = f"{company_number}_{person_id}_{role}"

        address = officer.get('address', {})
        if isinstance(address, dict):
            address_line_1 = str(address.get('address_line_1', ''))
            address_line_2 = str(address.get('address_line_2', ''))
            locality = str(address.get('locality', ''))
            postal_code = str(address.get('postal_code', ''))
            country = str(address.get('country', ''))
        else:
            address_line_1 = address_line_2 = locality = postal_code = country = ''

        identification = {}
        if is_corporate:
            identification = {
                'links': officer.get('links', {}),
                'officer_role': officer.get('officer_role', ''),
                'person_number': officer.get('person_number', ''),
                'appointed_on': officer.get('appointed_on', ''),
                'resigned_on': officer.get('resigned_on', '')
            }

        identification_json = json.dumps(identification)

        dob = officer.get('date_of_birth')
        if dob and isinstance(dob, dict):
            try:
                month = str(dob.get('month', '')).zfill(2)
                year = str(dob.get('year', ''))
                dob = f"{year}-{month}-01"
            except:
                dob = None

        officers_query = """
            INSERT INTO officers (
                officer_id, company_number, person_id, name, is_corporate, date_of_birth,
                identification, address_line_1, address_line_2, locality,
                postal_code, country
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                is_corporate = VALUES(is_corporate),
                date_of_birth = COALESCE(VALUES(date_of_birth), date_of_birth),
                identification = COALESCE(VALUES(identification), identification),
                address_line_1 = COALESCE(VALUES(address_line_1), address_line_1),
                address_line_2 = COALESCE(VALUES(address_line_2), address_line_2),
                locality = COALESCE(VALUES(locality), locality),
                postal_code = COALESCE(VALUES(postal_code), postal_code),
                country = COALESCE(VALUES(country), country)
        """

        officers_params = (
            officer_id, company_number, person_id, name, is_corporate, dob,
            identification_json, address_line_1, address_line_2,
            locality, postal_code, country
        )

        cursor.execute(officers_query, officers_params)

        roles_query = """
            INSERT INTO officer_roles (
                officer_id, company_number, role,
                appointed_on, resigned_on, notified_on, ceased_on
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                appointed_on = COALESCE(VALUES(appointed_on), appointed_on),
                resigned_on = COALESCE(VALUES(resigned_on), resigned_on),
                notified_on = COALESCE(VALUES(notified_on), notified_on),
                ceased_on = COALESCE(VALUES(ceased_on), ceased_on)
        """

        roles_params = (
            officer_id,
            company_number,
            map_role(role),  # Map the role to match ENUM values
            officer.get('appointed_on'),
            officer.get('resigned_on'),
            officer.get('notified_on'),
            officer.get('ceased_on')
        )

        cursor.execute(roles_query, roles_params)

    except Exception as e:
        logging.error(f"Error in update_officers_and_roles: {str(e)}")
        logging.error(f"Officer data: {officer}")
        raise

def update_psc_roles(cursor, psc: Dict, company_number: str, company_name: str):
    try:
        person_id = str(psc.get('person_id', uuid.uuid4()))
        name = str(psc.get('name', ''))
        role = 'PSC'
        kind = psc.get('kind', '')
        is_corporate = is_corporate_entity(name, kind=kind)
        officer_id = f"{company_number}_{person_id}_{role}"

        address = psc.get('address', {})
        if isinstance(address, dict):
            address_line_1 = str(address.get('address_line_1', ''))
            address_line_2 = str(address.get('address_line_2', ''))
            locality = str(address.get('locality', ''))
            postal_code = str(address.get('postal_code', ''))
            country = str(address.get('country', ''))
        else:
            address_line_1 = address_line_2 = locality = postal_code = country = ''

        identification = {
            'links': psc.get('links', {}),
            'kind': psc.get('kind', ''),
            'nationality': psc.get('nationality', ''),
            'country_of_residence': psc.get('country_of_residence', ''),
            'natures_of_control': psc.get('natures_of_control', []),
            'name_elements': psc.get('name_elements', {}),
            'ceased': psc.get('ceased', False)
        }

        identification_json = json.dumps(identification)  # Added this line

        officers_query = """
            INSERT INTO officers (
                officer_id, company_number, person_id, name, is_corporate,
                identification, address_line_1, address_line_2, locality,
                postal_code, country
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                is_corporate = VALUES(is_corporate),
                identification = COALESCE(VALUES(identification), identification),
                address_line_1 = COALESCE(VALUES(address_line_1), address_line_1),
                address_line_2 = COALESCE(VALUES(address_line_2), address_line_2),
                locality = COALESCE(VALUES(locality), locality),
                postal_code = COALESCE(VALUES(postal_code), postal_code),
                country = COALESCE(VALUES(country), country)
        """

        officers_params = (
            officer_id, company_number, person_id, name, is_corporate,
            identification_json, address_line_1, address_line_2,
            locality, postal_code, country
        )

        cursor.execute(officers_query, officers_params)

        roles_query = """
            INSERT INTO officer_roles (
                officer_id, company_number, role, notified_on, ceased_on
            ) VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                notified_on = COALESCE(VALUES(notified_on), notified_on),
                ceased_on = COALESCE(VALUES(ceased_on), ceased_on)
        """

        roles_params = (
            officer_id,
            company_number,
            role,
            psc.get('notified_on'),
            psc.get('ceased_on')
        )

        cursor.execute(roles_query, roles_params)

    except Exception as e:
        logging.error(f"Error in update_psc_roles: {str(e)}")
        logging.error(f"PSC data: {psc}")
        raise

def main():
    try:
        input_data = sys.stdin.read()
        data = json.loads(input_data)

        company_number = data['company_details']['company_number']
        company_name = data['company_details']['company_name']

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        officers = data.get('officer_details', {}).get('items', [])
        # logging.info(f"Processing {len(officers)} officers")
        for officer in officers:
            update_officers_and_roles(cursor, officer, company_number, company_name)

        pscs = data.get('psc_details', {}).get('items', [])
        # logging.info(f"Processing {len(pscs)} PSCs")
        for psc in pscs:
            update_psc_roles(cursor, psc, company_number, company_name)

        connection.commit()
        logging.info("Update completed successfully")

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