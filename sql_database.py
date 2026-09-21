from .ConnectionString import ConnectionString
from .SQLHandlers import SQLHandler

class SQLDatabase(SQLHandler):

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

    def tables(self):
        return self.query(
            """
            SELECT * FROM sys.tables
            WHERE SCHEMA_NAME(schema_id) = 'dbo';
            """
            )

    def cols(self, table_name):
         return self.query(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            """, 
            table_name
         ).squeeze()     

    def views(self):
        return self.query(
            """
            SELECT * FROM sys.objects
            WHERE type_desc = 'VIEW';
            """
            )

    def view(self, view_name):
        return self.query(
            f"""
            EXEC sp_helptext {view_name};
            """
            )

    def procs(self):
        return self.query(
            """SELECT * FROM INFORMATION_SCHEMA.ROUTINES
           WHERE ROUTINE_TYPE = 'PROCEDURE';
            """
            )

    def proc(self, procedure_name):
        return self.query(
            f"""
        SELECT * FROM INFORMATION_SCHEMA.PARAMETERS 
        WHERE SPECIFIC_NAME='{procedure_name}';
        """)
        
    def constraints(self, table_name=None):
        return self.query(
            f"""
            SELECT
                tc.CONSTRAINT_NAME,
                tc.CONSTRAINT_TYPE,
                tc.TABLE_NAME,
                kcu.COLUMN_NAME
            FROM
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            LEFT JOIN
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE
                ('{table_name}' IS NULL OR tc.TABLE_NAME = '{table_name}')
                AND tc.TABLE_SCHEMA = 'dbo'  -- optional if needed
        """)

    def triggers(self, table_name=None):
        return self.query(
            f"""
            SELECT
                tr.name AS TriggerName,
                tr.is_disabled,
                tr.is_instead_of_trigger,
                tr.create_date,
                tr.modify_date,
                m.definition AS TriggerDefinition
            FROM
                sys.triggers AS tr
            JOIN
                sys.tables AS t ON tr.parent_id = t.object_id
            JOIN
                sys.sql_modules AS m ON tr.object_id = m.object_id
            WHERE
                ('{table_name}' IS NULL OR t.name = '{table_name}')
                AND SCHEMA_NAME(t.schema_id) = 'dbo' -- optional schema filter

        """)

