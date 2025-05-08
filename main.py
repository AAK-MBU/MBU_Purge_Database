"""This module contains a generic function for executing stored procedures in a database
via the pyodbc library. The function connects to the database and executes the stored
procedure with provided parameters, returning the success status and any error messages."""
import json
import os
from typing import Dict, Any, Union, Tuple
from dateutil import parser
import pyodbc
from mbu_dev_shared_components.database.logging import log_event, _send_heartbeat
# from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure

from config import LOG_DB, LOG_CONTEXT, ENV


# CHANGE TO IMPORT FROM MBU_DEV SHARED when PR #38 in develop is in main 
def execute_stored_procedure(
        connection_string: str,
        stored_procedure: str,
        params: Dict[str, Tuple[type, Any]] | None = None
) -> Dict[str, Union[bool, str, Any]]:
    """
    Executes a stored procedure with the given parameters.

    Args:
        connection_string (str): The connection string to connect to the database.
        stored_procedure (str): The name of the stored procedure to execute.
        params (Dict[str, Tuple[type, Any]], optional): A dictionary of parameters to pass to the stored procedure.
                                 Each value should be a tuple of (type, actual_value).

    Returns:
        Dict[str, Union[bool, str, Any]]: A dictionary containing the success status, an error message (if any),
                                           and additional data.
    """
    result = {
        "success": False,
        "error_message": None,
    }

    type_mapping = {
        "str": str,
        "int": int,
        "float": float,
        "datetime": parser.isoparse,
        "json": lambda x: json.dumps(x, ensure_ascii=False)
    }

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:
                if params:
                    param_placeholders = ', '.join([f"@{key} = ?" for key in params.keys()])
                    param_values = []

                    for key, value in params.items():
                        if isinstance(value, tuple) and len(value) == 2:
                            value_type, actual_value = value
                            if value_type in type_mapping:
                                param_values.append(type_mapping[value_type](actual_value))
                            else:
                                param_values.append(actual_value)
                        else:
                            raise ValueError("Each parameter value must be a tuple of (type, actual_value).")

                    sql = f"EXEC {stored_procedure} {param_placeholders}"
                    tryres = cursor.execute(sql, tuple(param_values))
                    print(tryres)
                else:
                    sql = f"EXEC {stored_procedure}"
                    rows_updated = cursor.execute(sql)
                    print("Should be executed")
                conn.commit()
                result["success"] = True
                result["rows_updated"] = rows_updated.rowcount
    except pyodbc.Error as e:
        result["error_message"] = f"Database error: {str(e)}"
    except ValueError as e:
        result["error_message"] = f"Value error: {str(e)}"
    except Exception as e:
        result["error_message"] = f"An unexpected error occurred: {str(e)}"

    return result


def list_stored_procedures():
    """
    Lists all stored procedures in the specified database using a trusted connection.

    Returns:
        list: A list of stored procedure names.
    """
    # Get the connection string from the environment variable
    connection_string = os.getenv("DbConnectionString")

    # Establish a connection to the database using the trusted connection
    conn = pyodbc.connect(connection_string)

    # Create a cursor object
    with conn.cursor() as cursor:

        # Execute the query to list all stored procedures
        cursor.execute("""
        SELECT name
        FROM sys.procedures
        """)

        # Fetch all results
        procedures = cursor.fetchall()

    # Close the connection
    conn.close()

    # Return the names of the stored procedures
    return [procedure.name for procedure in procedures]


def main():
    """Function to run purge procedure on database and log heartbeat and events"""
    _send_heartbeat(
        servicename="SQL data base purge",
        status="RUNNING",
        details="",
        db_env=ENV
        )
    log_event(
        log_db=LOG_DB,
        level="INFO",
        message="Running purge of database",
        context=LOG_CONTEXT,
        db_env=ENV
    )
    res = execute_stored_procedure(
        os.getenv("DbConnectionString"),
        'RPA.journalizing.sp_UpdatePurgeMarker',
    )
    log_event(
        log_db=LOG_DB,
        level="INFO",
        message=f"Purged {res["rows_updated"]} forms",
        context=LOG_CONTEXT,
        db_env=ENV
    )


if __name__ == '__main__':
    main()
