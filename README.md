# SQL-Handlers

Used to assist with performing MS SQL CRUD operations with Python. 

Uses Pandas, Pyodbc, and SQLAlchemy libraries.

Example usage:

    from sql_database import SQLDatabase

    # connection string must be formatted for MS SQL Server.
    ex_connection_string = r'Driver={ODBC Driver 17 for SQL Server}; Server=XXXXXXX; Database=<DatabaseName>; Trusted_Connection=yes;'

    sql_database = SQLDatabase(ex_connection_string)

    # Return a Pandas Dataframe from a database query

        df1 = sql_database.query("SELECT * FROM <TableName>;")

        #if returning results from stored procedure must set NOCOUNT ON AND OFF

        df2 = sql_database.query("SET NOCOUNT ON; EXEC <STOREPROCEDURENAME>; SET NOCOUNT OFF;")

    # Perform any CRUD Operation (parameters are optional)
    sql_database.execute("EXEC <uspStoredProcedure> ?, ?, ?;", param1, param2, param3)

    # Bulk Insert using fast execute_many
    sql_database.bulk_insert(df, <DatabaseTableName>)

