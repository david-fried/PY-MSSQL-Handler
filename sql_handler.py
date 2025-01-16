import pandas as pd
import pyodbc
import sqlalchemy
import urllib
from functools import wraps
from typing import Iterable, Tuple, Any, Union


class MsSqlHandler:

	"""
	A class used to represent a MsSqlHandler object, which is used to perform SQL CRUD operations on a Microsoft SQL Server Database using Pandas, Pyodbc, SQLAlchemy. 
		
	Attributes:
	-----------
	connection_string: str
		Connnection string for the target database formatted for pyodbc and SQL Server.
	read_only: str
		Toggle this property to enable (read_only=False) or disable (read_only=True) ability to perform write operations with the MsSqlHandler Object. Default is False.

	Methods:
	--------
	start_transaction()
		Opens a pyodbc connection and cursor on the target database using the connection string.
	commit_transaction()
		Commits a write operation to the target database.
	end_transaction()
		Closes the connection and cursor.
	query(sql_query_statement, parameters):
		Returns a pandas dataframe from a query results set on the target database.
	execute(sql_statement, parameters):
		Performs any Create, Update, Insert, or Delete operation on the target database. Method disabled when read_only=True.
	bulk_insert(dataframe, table_name, remove_nulls, handle_nulls)
		Bulk inserts data from a pandas dataframe on the target database (uses Pyodbc's fast execute_many). Method disabled when read_only=True.
	iter_execute(parameterized_sql_statement, values, error_handling)
		Loops through each row of data to perform create, update, insert, or delete operation on the target database. Method disabled when read_only=True.

	Notes:
	------
	All database connection operations are handed internally. Initialize a MsSqlHandler object by simply setting the connection_string property. Nothing else is necessary to begin performing CRUD operations
 	using the query, execute, bulk_insert, or iter_execute method.

	The execute, bulk-insert, and iter-execute methods perform write operations, and only work when the read_only property is set to False.
	
	The start_transaction, commit_transaction, and end_transaction methods are used to commit transaction manually. 
	Specifically, they are used when you (a) want to use more than one write operation method, and (b) want to control when operations are committed.
	Once a MsSqlHandler object is initialized, call the start_transaction method to open a connection. Perform write operations with the 
	execute, bulk-insert, and/or iter-execute methods. Then, call the commit_transaction method when wanting to commit.
	Finally you must call end_transaction to close the connection and prevent deadlocks.
	Note: Any parameterized sql statement must use question marks for parameter placeholders (see pyodbc documentation; e.g., e.g., 'SELECT * FROM Employees WHERE LastName = ?').
	"""

	def __init__(self, connection_string: str, read_only=False):
		"""
		Constructs all the necessary attributes for the MsSqlHandler object.

		Parameters:
		-----------
		connection_string: str
			Connnection string for the target database formatted for pyodbc and SQL Server.
		read_only: str
			Toggle this property to enable (read_only=True) or disable (read_only=False) ability to perform write operations with the MsSqlHandler Object.
			Default is False.
		"""
		self.connection_string = connection_string
		self.read_only = read_only
		self._ongoing_transaction = False
		self._conn = None
		self._cursor = None

	def _allow_method(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			if args[0].read_only:
				print('Cannot perform operation. Access is read-only.')
				return None
			return func(*args, **kwargs)
		return wrapper

	def _handle_connection(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			self = args[0]
			func_name = func.__name__
			if self._discreet_transaction(func_name):
				self._open_conn()			
			results = None
			try:
				results = func(*args, **kwargs)
			except Exception as e:
				self._conn.rollback()
				print('Rolling back transaction.')
				print(e)
				self.end_transaction()
			else:
				if self._discreet_transaction(func_name):
					self.commit_transaction()
					self.end_transaction()
			return results
		return wrapper

	def _open_conn(self, autocommit=False, *args, **kwargs):
		self._conn = pyodbc.connect(self.connection_string, autocommit=False, *args, **kwargs)
		self._cursor = self._conn.cursor()

	def _close_conn(self):
		self._cursor.close()
		self._conn.close()
		self._cursor = None
		self._conn = None

	def _discreet_transaction(self, func_name):
		return (not self._ongoing_transaction and not func_name == 'query')

	def _sqlalchemy_conn(self):
		quoted = urllib.parse.quote_plus(self.connection_string)
		x='mssql+pyodbc:///?odbc_connect={}'.format(quoted)
		engine = sqlalchemy.create_engine(x)
		return engine

	def start_transaction(self, timeout: int = None) -> None:
		"""
		Opens a pyodbc database connection and cursor

		Parameters:
		----------
		timeout: int
			Set timeout. Defaults is None.
		"""
		self._open_conn()
		if timeout is not None:
			self._conn.timeout=timeout
		self._ongoing_transaction = True

	def commit_transaction(self) -> None:
		"""
		Commits a write operation to a SQL database.
		"""
		self._conn.commit()

	def end_transaction(self) -> None:
		"""
		Closes a pyodbc database connection and cursor
		"""
		self._close_conn()
		self._ongoing_transaction = False

	def query(self, sql_query_statement: str, *parameters: Union[int, str, float], **kwargs: dict) -> pd.DataFrame:
		"""
		Returns a pandas dataframe from a query results set using a sqlalchemy engine and pandas read_sql method.

		Parameters:
		-----------
		sql_query_statement: str
			A SQL statement to query the target database (required). Parameter markers are '?' (e.g., 'SELECT * FROM Employees WHERE LastName = ?').
		*parameters: int, str, or float
			Must specify if passing in a parameterized sql query statement (optional).
		**kwargs: dict
			Additional keyword arguments to pass to pandas read_sql method (optional).
			
		Returns:
		--------
		pandas.DataFrame
			A dataframe of the query results set using pyodbc's read_sql method.

		Notes:
		------
		If returning the results from a stored procedure be sure to specify SET NOCOUNT ON before executing stored procedure and SET NOCOUNT OFF after (e.g., "SET NOCOUNT ON Employees; EXEC uspGetEmployees; SET NOCOUNT OFF Employees").
		"""
		initial_read_only_setting = self.read_only
		self.read_only = True
		engine = self._sqlalchemy_conn()
		dataframe = pd.read_sql(sql_query_statement, engine, params=parameters, **kwargs)
		self.read_only = initial_read_only_setting
		return dataframe

	@_handle_connection
	@_allow_method
	def execute(self, sql_statement: str, *parameters: Union[int, str, float]) -> None:
		"""
		Performs any Create, Update, Insert, or Delete operation on a SQL database using a pyodbc connection on the target database.

		Parameters:
		-----------
		sql_statement: str
			If parameterized must include *parameters.
		*parameters (int, str, float, or datetime)
			Parameters to pass to a parameterized sql statement.
		Returns:
		--------
		None

		Notes:
		------
		Method disabled when read_only=True.
		"""
		
		self._cursor.execute(sql_statement, parameters)

	@_handle_connection
	@_allow_method
	def iter_execute(self, parameterized_sql_statement: str, values: Iterable[Tuple[Union[int, str, float], ...]], error_handling: str ='raise') -> None:
		"""
		Loops through each row of data to perform create, update, insert, or delete operation on the target database.

		Parameters:
		-----------
		parameterized_sql_statement: str
			Must be parameterized sql statement, typically used to perform insert or update operation on each row of values.
		values: iterable of tuple of any size containing int, str, or float
			The parameterized sql statement performs the operation on each tuple in elements. Each tuple in iterable must contain the same number of values that equal the number of parameter markers (i.e., question marks). 		
		error_handling: str
			Specify how to handle when operation fails for a row of values. Values are 'raise' or 'continue'. The default is 'raise'. If you specify continue it will provide the error message of the failed execute and move on to the next row in the sequence.
		
		Returns:
		--------
		None

		Notes:
		------
		Method disabled when read_only=True.
		As mentioned, for the values argument the number of values in each tuple in the iterable must equal the number of parameter markers (i.e., question marks) passed to the parameterized_sql_statement
		For example, if the length of each tuple is two the number of parameter markers in the parameterized sql statement must be two:
			values =	[(1, 'Mary'),
						(2, 'Bob'),
						(3, 'Bill')]

		parameterized_sql_statement = 'INSERT INTO Employees (ID, FirstName) VALUES (?, ?);'

		"""

		if '?' not in parameterized_sql_statement:
			raise ValueError('Sql statement must be parameterized.')

		if error_handling =='raise': # raise exception and rollback all transactions (pyodbc default)
			for row in values:
				self._cursor.execute(parameterized_sql_statement, row)

		elif error_handling == 'continue': # print error to the console and move on to next row
			for row in values:
				try:
					self._cursor.execute(parameterized_sql_statement, row)
				except Exception as e:
					print()
					print(e)
					print(row)
					print()

		else:
			raise TypeError("Invalid argument for 'error_handling'. Argument must be 'raise' or 'continue'.")

	def bulk_insert(self, dataframe: pd.DataFrame, table_name: str, **kwargs: dict) -> None:

		"""
		Performs a bulk insert using pyodbc's fast execute_many on the target database.

		Parameters:
		-----------
		dataframe: pandas.Dataframe
			Pandas DataFrame containing the data to insert (required).
		table_name: str
			 Name of target table in the target database (required).
		**kwargs: dict
			remove_nulls: bool
				Removes rows with nulls before inserting data (optional). Defaults to False.
			subset_cols: list[str]
				Use when remove_nulls == True. List of column name strings to look for nulls when deciding to remove rows with nulls.
			handle_nulls: '*' or list[str]
				Prevent errors when inserting data due to nulls. Specify '*' to attempt to handle nulls for all columns. Otherwise, specify a column name containing nulls.
			identity_insert_on: bool
				Set Identity Insert On(bool): False (default) or True.
		values: iterable of tuples
			The parameterized sql statement performs the operation on each tuple in the iterable. The length of each tuple must equal the number of parameter markers (i.e., question marks) in the parameterized sql statement.	
		error_handling: str
			Specify how to handle when operation fails for a row of values. Arguments are 'raise' or 'continue'. The default is 'raise'. If you specify an error message will be provided after failed execute of a row and will attempt to execute the sql statemetn on the next row in the sequence.
		
		Returns:
		--------
		None
		"""
		remove_nulls = kwargs.get('remove_nulls', False)
		subset_cols = kwargs.get('subset_cols', None)
		handle_nulls = kwargs.get('handle_nulls', None)
		identity_insert_on = kwargs.get('identity_insert_on', False)

		if remove_nulls:
			if subset_cols is not None:
				dataframe.dropna(inplace=True, subset=subset_cols)
			else:
				dataframe.dropna(inplace=True)

		if handle_nulls == '*':
			for col in dataframe.columns:
				if dataframe[col].dtype == 'object':
					dataframe[col] = dataframe[col].fillna('')
			dataframe = dataframe.astype(object).where(pd.notnull(dataframe), None)
			self._insert_values(dataframe, table_name, identity_insert_on)

		elif handle_nulls is not None:
			values, nulls = self._separate_nulls(dataframe, handle_nulls)
			self._insert_values(values, table_name, identity_insert_on)
			self._insert_values(nulls, table_name, identity_insert_on)

		else:
			self._insert_values(dataframe, table_name, identity_insert_on)

	@_handle_connection
	@_allow_method
	def _insert_values(self, dataframe: pd.DataFrame, table_name: str, identity_insert_on: bool) -> None:
		"""helper method for bulk_insert method"""
		c = list(dataframe.columns)
		columns = ','.join(c)
		params = ','.join(['?' for i in range(len(c))])
		sql_insert_statement = f'INSERT INTO {table_name} ({columns}) VALUES ({params});'
		if identity_insert_on:
			sql_insert_statement = f'SET IDENTITY_INSERT {table_name} ON; ' + sql_insert_statement + f' SET IDENTITY_INSERT {table_name} OFF;'
		values = dataframe.values.tolist()
		if len(values) > 0:
			self._cursor.fast_executemany = True
			self._cursor.executemany(sql_insert_statement, values)
			#self._cursor.commit()

	def _separate_nulls(self, dataframe: pd.DataFrame, column: str) -> (pd.DataFrame, pd.DataFrame):
		"""helper method if a column for handle_nulls parameter is specified for bulk_insert method."""
		values = dataframe.copy()
		values = values.loc[~values[column].isna()]
		nulls = dataframe.copy()
		nulls = nulls.loc[nulls[column].isna()]
		nulls[column] = nulls[column].astype(str)
		nulls[column] = None
		return values, nulls
