class ConnectionString():

              def __init__(self):

                             self.prod = r'Driver={ODBC Driver 17 for SQL Server}; Server= XXX; Database=XXX; Trusted_Connection=yes;'

                             self.dev =  r'Driver={ODBC Driver 17 for SQL Server}; Server= XXX; Database= XXX; Trusted_Connection=yes;'

                             self.qa =  r'Driver={ODBC Driver 17 for SQL Server}; Server= XXX; Database= XXX; Trusted_Connection=yes;'

                             self.local = r'Driver={ODBC Driver 17 for SQL Server};Server=.;Database= XXX;Trusted_Connection=yes;TrustServerCertificate=yes;'

 

              def create_string(self, db_type: str):

                             """

                             db_type: str 'local', 'dev', 'qa', or 'prod'

                             """

                             return getattr(self, db_type)