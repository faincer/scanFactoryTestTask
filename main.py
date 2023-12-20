from contextlib import contextmanager
import pprint
import re
import sqlite3
from typing import List

DATABASE = 'domains.db'


class DomainsTable:
    TABLE_NAME = 'domains'

    PROJECT_ID_COLUMN = 'project_id'
    NAME_COLUMN = 'name'


class RulesTable:
    TABLE_NAME = 'rules'
    
    PROJECT_ID_COLUMN = 'project_id'
    REGEXP_COLUMN = 'regexp'


def generate_regex(domains: List[str], min_frequency: int = 2) -> str:
    def update_counter(domain_counter, lvl_domains):
        for i in range(len(lvl_domains)):
            if i > len(domain_counter) - 1:
                domain_counter.append(dict())

            if lvl_domains[i] in domain_counter[i].keys():
                domain_counter[i][lvl_domains[i]] += 1
            else:
                domain_counter[i][lvl_domains[i]] = 1

        
    domain_counter = []
    for d in domains:
        lvl_domains = d.split('.')[::-1]
        update_counter(domain_counter, lvl_domains)
    
    regex_patterns = []
    for subdomain_count in domain_counter:
        # Select subdomains with frequency greater than or equal to min_frequency
        valid_subdomains = [subdomain for subdomain, count in subdomain_count.items() if count >= min_frequency]

        if len(valid_subdomains) == 0:
            break
    
        # Escape special characters and build the regular expression pattern
        regex_pattern = '|'.join(re.escape(subdomain) for subdomain in valid_subdomains)
        regex_pattern = f"(({regex_pattern})\.)" if regex_patterns else f"({regex_pattern})"
        regex_patterns.append(regex_pattern)

    result_regexp = '?'.join(regex_patterns[::-1])
    return result_regexp + '$'


def update_rules_for_project(conn: sqlite3.Connection, project_id: str) -> None:
    # Get all domains for the given project_id
    domains = conn.execute(f"SELECT {DomainsTable.NAME_COLUMN} FROM {DomainsTable.TABLE_NAME} WHERE {DomainsTable.PROJECT_ID_COLUMN} = '{project_id}'").fetchall()

    # Generate the regular expression for filtering "garbage" domains
    regex_pattern = generate_regex([ent[0] for ent in domains])

    # Check if a rule already exists for the project_id
    existing_rule = conn.execute(f"SELECT {RulesTable.REGEXP_COLUMN} FROM {RulesTable.TABLE_NAME} WHERE {RulesTable.PROJECT_ID_COLUMN} = '{project_id}'").fetchone()
    if existing_rule:
        # Update a current one
        conn.execute(f"UPDATE {RulesTable.TABLE_NAME} SET {RulesTable.REGEXP_COLUMN} = ? WHERE {RulesTable.PROJECT_ID_COLUMN} = ?", (regex_pattern, project_id))
    else:
        # Insert a new rule
        conn.execute(f"INSERT INTO {RulesTable.TABLE_NAME} ({RulesTable.REGEXP_COLUMN}, {RulesTable.PROJECT_ID_COLUMN}) VALUES (?, ?)", (regex_pattern, project_id))


@contextmanager
def get_db_conn():
    connection = sqlite3.connect(DATABASE)
    try:
        yield connection
    except Exception as exc:
        pprint.pprint("Something goes wrong")
        pprint.pprint(exc)
    finally:
        connection.close()


if __name__ == "__main__":
    with get_db_conn() as conn: 
        # Fetch all the table names
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        assert all(ent[0] in {DomainsTable.TABLE_NAME, RulesTable.TABLE_NAME} for ent in tables), 'Not found required tables'

        # Execute a query to get a list of project ids in the database
        domains = conn.execute(f"SELECT DISTINCT({DomainsTable.PROJECT_ID_COLUMN}) FROM {DomainsTable.TABLE_NAME}").fetchall()
        
        for d in domains:
            update_rules_for_project(conn, d[0])
