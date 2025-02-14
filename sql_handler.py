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
	"""

	def __init__(self, connection_string: str, read_only=False):

		"""

		connection_string (str): Formatted for pyodbc.

		read_only (bool): Defaults to False.

		"""

		self.connection_string = connection_string

		self.read_only = read_only



	def allow_method(func):

		@wraps(func)

		def wrapper(*args, **kwargs):

			if args[0].read_only:

				print('Cannot perform operation. Access is read-only.')

				return None

			return func(*args, **kwargs)

		return wrapper



	def _conn(self):

		"""Open a database connection"""

		return pyodbc.connect(self.connection_string)



	def _sqlalchemy_conn(self):

		quoted = urllib.parse.quote_plus(self.connection_string)

		x='mssql+pyodbc:///?odbc_connect={}'.format(quoted)

		engine = sqlalchemy.create_engine(x)

		return engine

	

	def query(self, sql_query_statement: str, *parameters, **kwargs) -> pd.DataFrame:

		"""

		Purpose: Useful for querying SQL database.

		Required arguments:

			sql_query_statement (str): If parameterized than must pass a tuple of values to the parameters argument.

							

		Optional arguments:

			*parameters (int, str, float, or datetime)

			pandas_dataframe (bool): True (default) of False. True returns pandas dataframe and False returns a list of tuples.

			**kwargs: see pandas.read_sql documentation

		"""

		engine = self._sqlalchemy_conn()

		return pd.read_sql(sql_query_statement, engine, params=parameters, **kwargs)



	@allow_method

	def execute(self, sql_statement: str, *parameters) -> None:

		"""

		Purpose: Perform any Create, Update, Insert, Delete operation.

		Required arguments:

			sql_statement (str): If parameterized than must include *parameters.

									

		Optional arguments:

			*parameters (int, str, float, or datetime)

		"""

		conn = self._conn()

		with conn:

			conn.cursor().execute(sql_statement, parameters)



	@allow_method

	def iter_execute(self, parameterized_sql_statement: str, values, error_handling ='raise') -> None:

		"""

		Purpose: Perform an update/insert operation several times using parameterized sql statement.


		Required arguments:


		parameterized_sql_statement (str): Used to perform either update or insert operation.

		Must be parameterized (i.e., contain question marks in place of values; see pyodbc documentation)

			Example statement: 'INSERT INTO USERS (ID, Name) VALUES (?, ?);'


		values (iterable): The data to insert or update. Must be an iterable of tuples.

		Each tuple in iterable must contain number of values that equal the number

		of parameter markers (i.e., question marks).

		For example, if there are two parameter markers,

		each tuple must be of length equal to 2.

				[(1, 'Mary'),

				(2, 'Bob'),

				(3, 'Bill')]

		error_handling: If one statement fails raise exception ('raise') or ignore and continue inserting rows('ignore')

		"""

		if '?' not in parameterized_sql_statement:

					raise ValueError('Sql statement must be parameterized.')



		conn = self._conn()


		if error_handling =='raise': # raise exception and rollback all transactions (pyodbc default)

			with conn:

				c = conn.cursor()

				for row in values:

					c.execute(parameterized_sql_statement, row)


		elif error_handling == 'ignore': # commit each row of values and ignore exceptions


			with conn:

				c = conn.cursor()

				for row in values:

					try:

						c.execute(parameterized_sql_statement, row)

					except Exception as e:

						print()

						print(e)

						print(row)

						print()

					else:

						c.commit()

		else:

			raise TypeError("Invalid argument for 'error_handling'. Argument must be 'raise' or 'ignore'.")





	@allow_method

	def bulk_insert(self, df: pd.DataFrame, table_name: str, **kwargs) -> None:

		"""

		Purpose: Perform a bulk insert using pyodbc's fast execute_many.



		Required arguments:

			df (pd.DataFrame): Pandas DataFrame containing the data to insert.

			table_name (str): Name of SQL database table to insert the data.



		Optional arguments:

			remove_nulls (bool): Remove rows with nulls before inserting. Defaults to False.

			subset_cols (None, list): Use when remove_nulls is True. List of column name strings to look for nulls when deciding to remove rows with nulls.

			handle_nulls (None, str): Prevent errors when inserting data. Specify '*' for all columns. This is usually the best option. Otherwise, specify a column name containing nulls.

			identity_insert_on (bool): False (default) or True.

		"""

		remove_nulls = kwargs.get('remove_nulls', False)

		subset_cols = kwargs.get('subset_cols', None)

		handle_nulls = kwargs.get('handle_nulls', None)

		identity_insert_on = kwargs.get('identity_insert_on', False)



		if remove_nulls:

			if subset_cols is not None:

				df.dropna(inplace=True, subset=subset_cols)

			else:

				df.dropna(inplace=True)



		if handle_nulls == '*':

			df = df.astype(object).where(pd.notnull(df), None)

			df = df.replace([np.nan, np.inf, np.NaN, -np.inf, pd.NA, pd.NaT], None)

			self._insert_values(df, table_name, identity_insert_on)


		elif handle_nulls is not None:

			values, nulls = self._separate_nulls(df, handle_nulls)

			self._insert_values(values, table_name, identity_insert_on)

			self._insert_values(nulls, table_name, identity_insert_on)


		else:

			self._insert_values(df, table_name, identity_insert_on)



	@allow_method

	def _insert_values(self, df: pd.DataFrame, table_name: str, identity_insert_on: bool) -> None:

		"""helper method for bulk_insert method"""

		c = list(df.columns)

		columns = ','.join(c)

		params = ','.join(['?' for i in range(len(c))])

		sql_insert_statement = f'INSERT INTO {table_name} ({columns}) VALUES ({params});'

		if identity_insert_on:

					sql_insert_statement = f'SET IDENTITY_INSERT {table_name} ON; ' + sql_insert_statement + f' SET IDENTITY_INSERT {table_name} OFF;'

		values = df.values.tolist()

		if len(values) > 0:

			conn = self._conn()

			conn.autocommit=False

			with conn:

				c = conn.cursor()

				c.fast_executemany = True

				c.executemany(sql_insert_statement, values)

				c.commit()



	def _separate_nulls(self, df: pd.DataFrame, column: str) -> (pd.DataFrame, pd.DataFrame):

		"""helper method if a column for handle_nulls parameter is specified for bulk_insert method."""

		values = df.copy()

		values = values.loc[~values[column].isna()]

		nulls = df.copy()

		nulls = nulls.loc[nulls[column].isna()]

		nulls[column] = nulls[column].astype(str)

		nulls[column] = None

		return values, nulls