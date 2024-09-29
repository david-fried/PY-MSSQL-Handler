class ConnectionString:
	
	def __init__(self):
		# Replace with own connection strings. Examples below are formatted for MS SQL Server.
		self.prod = r'Driver={ODBC Driver 17 for SQL Server}; Server=XXXXXXX; Database=<ProdDatabaseName>; Trusted_Connection=yes;'
		self.dev =  r'Driver={ODBC Driver 17 for SQL Server}; Server=XXXXXXX; Database=<DevDatabasename>; Trusted_Connection=yes;'
		self.qa =  r'Driver={ODBC Driver 17 for SQL Server}; Server=XXXXXXX; Database=<QADatabaseName>; Trusted_Connection=yes;'
		self.local = r'Driver={ODBC Driver 17 for SQL Server}; Server=(localDB)\MSSQLLocalDB; Database=MyLocalDb;'

	def create_string(self, db_type: str):
		"""
		db_type: str 'local', 'dev', 'qa', or 'prod'
		"""
		return getattr(self, db_type)
