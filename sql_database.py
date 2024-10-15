from connection_string import ConnectionString
from sql_handler import MsSqlHandler

class SQLDatabase(MsSqlHandler):

    @classmethod
    def prod(cls, read_only=True):
        return cls(ConnectionString().prod, read_only)

    @classmethod
    def qa(cls, read_only=False):
        return cls(ConnectionString().qa, read_only)

    @classmethod
    def dev(cls, read_only=False):
        return cls(ConnectionString().dev, read_only)

    @classmethod
    def local(cls, read_only=False):
        return cls(ConnectionString().local, read_only)

    def get_tables(self):
        return self.query(
            """
            SELECT * FROM sys.tables
            WHERE SCHEMA_NAME(schema_id) = 'dbo';
            """
            )

    def get_columns(self, table_name):
         return self.query(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            """
            )       

    def get_views(self):
        return self.query(
            """
            SELECT * FROM sys.objects
            WHERE type_desc = 'VIEW';
            """
            )

    def get_view_definition(self, view_name):
        return self.query(
            f"""
            EXEC sp_helptext {view_name};
            """
            )

    def get_procedures(self):
        return self.query(
            """SELECT * FROM INFORMATION_SCHEMA.ROUTINES
           WHERE ROUTINE_TYPE = 'PROCEDURE';
            """
            )

    def get_procedure_definition(self, procedure_name):
        return self.query(
            f"""
        SELECT * FROM INFORMATION_SCHEMA.PARAMETERS 
        WHERE SPECIFIC_NAME='{procedure_name}';
        """)

