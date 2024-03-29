import sqlite3
import os
import sys

from log import Log


class Db:
	def __init__(self, db_name: str):
		self.logger = Log('db.log')
		try:
			if not isinstance(db_name, str):
				raise TypeError('Database name need to be a string type!')
			if '.db' in db_name:
				db_name = os.path.splitext(db_name)[0]
			self.db_name = db_name
			self.__connection = sqlite3.connect(f'{self.db_name}.db')
			self.__cursor = self.__connection.cursor()
			self.logger.log(f'Connection to database ({self.db_name}) was successful!', __file__, sys._getframe().f_code.co_name)
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)

	def __del__(self) -> None:
		try:
			self.__connection.commit()
			self.__connection.close()
			self.logger.log(f'Disconnection to database ({self.db_name}) was successful!', __file__, sys._getframe().f_code.co_name)
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)

	def exec_sql(self, command: str):
		'''
		Method for executing sql script
		Examples:
		exec_sql(\'SELECT age, id FROM table_name\')
		OUTPUT: [(38, 1), (32, 2), (84, 3)]
		-----------------------------------
		exec_sql(\'INSERT INTO table_name (name, age) VALUES (\"Michael\", 12)\')
		-----------------------------------
		exec_sql(\'UPDATE table_name SET name="Poul" WHERE name=\"Tomas\"\')
		-----------------------------------
		exec_sql(\'DELETE FROM table_name WHERE age < 18\')
		'''
		try:
			if not isinstance(command, str):
				self.logger.log('SQL command need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			command = command.lower()
			if 'select' in command:
				self.__cursor.execute(command)
				result = self.__cursor.fetchall()
				self.logger.log('SQL script executing was successful!', __file__, sys._getframe().f_code.co_name)
				return result
			self.__cursor.execute(command)
			self.__connection.commit()
			self.logger.log('SQL script executing was successful!', __file__, sys._getframe().f_code.co_name)
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)

	def select(self, table_name: str, columns=None, where=None):
		'''
		Method for simply executing construction "SELECT" in sql script
		Examples:
		select(\'table_name\')
		meaning SELECT * FROM table_name
		-----------------------------------
		select(\'table_name\', [\'name\', \'age\'])
		meaning SELECT name, age FROM table_name
		-----------------------------------
		select(\'table_name\', [\'name\', \'age\'], \'age > 18\')
		meaning SELECT name, age FROM table_name WHERE age > 18
		'''
		try:
			if not isinstance(table_name, str):
				self.logger.log('Table name need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			command = 'SELECT '
			if columns is None:
				command += '*'
			else:
				if isinstance(columns, str):
					command += columns
				elif isinstance(columns, tuple) or isinstance(columns, list):
					for column in columns:
						if not isinstance(column, str):
							self.logger.log('Column names need to be a string!', __file__, sys._getframe().f_code.co_name)
							return
						command += column + ', '
					command = command[:-2]
				else:
					self.logger.log('Columns need to be a string, tuple or list type!', __file__, sys._getframe().f_code.co_name)
					return
			command += f' FROM {table_name}'
			if where is None:
				self.__cursor.execute(command)
				result = self.__cursor.fetchall()
				self.logger.log('Command \"SELECT\" was successful!', __file__, sys._getframe().f_code.co_name)
				return result
			if not isinstance(where, str):
				self.logger.log('Command after "WHERE" need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			command += f' WHERE {where}'
			self.__cursor.execute(command)
			result = self.__cursor.fetchall()
			self.logger.log(f'Command \"SELECT\" to {table_name} was successful!', __file__, sys._getframe().f_code.co_name)
			return result
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)

	def insert_one(self, table_name: str, columns_values: dict):
		'''
		Method for simply executing construction "INSERT" in sql script
		Examples:
		insert_one(\'table_name\', {\'name\': \'John\', \'age\': 34})
		meaning INSERT INTO table_name (name, age) VALUES ('John', 34)
		-----------------------------------
		insert_one(\'table_name\', {\'name\': \'John\', \'age\': 34, \'email\': \'example@example.com\', \'balance\': 2300.85})
		meaning INSERT INTO table_name (name, age, email, balance) VALUES (\"John\", 34, \"example@example.com\", 2300.85)
		'''
		try:
			if not isinstance(table_name, str):
				self.logger.log('Table name need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			if not isinstance(columns_values, dict):
				self.logger.log('Columns and values need to be a dictionary type!', __file__, sys._getframe().f_code.co_name)
				return
			command = f'INSERT INTO {table_name} ('
			column_names, column_values = [], []
			for key, value in columns_values.items():
				if not isinstance(key, str):
					self.logger.log('Keys into the dictionary need to be a string type!', __file__, sys._getframe().f_code.co_name)
					return
				if not isinstance(value, str) and not isinstance(value, int) and not isinstance(value, float):
					self.logger.log('Values into the dictionary need to be a string, int or float type!', __file__, sys._getframe().f_code.co_name)
					return
				column_names.append(key)
				column_values.append(value)
			for column in column_names:
				command += column + ', '
			command = command[:-2] + ') VALUES ('
			for value in column_values:
				if isinstance(value, str):
					command += '\'' + str(value) + '\'' + ', '
				else:
					command += str(value) + ', '
			command = command[:-2] + ')'
			self.__cursor.execute(command)
			self.__connection.commit()
			self.logger.log(f'Command \"INSERT\" to {table_name} was successful!', __file__, sys._getframe().f_code.co_name)
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)

	def insert_many(self, table_name: str, columns_values_list: list):
		'''
		Method for simply executing many construction "INSERT" in sql script
		Examples:
		insert_many(\'table_name\', [{\'name\': \'John\', \'age\': 34}, {\'name\': \'Tom\', \'age\': 19, \'balance\': 160.8}])
		meaning INSERT INTO table_name (name, age) VALUES (\"John\", 34); INSERT INTO table_name (name, age, balance) VALUES (\"Tom\", 19, 160.8);
		'''
		if not isinstance(table_name, str):
			self.logger.log('Table name need to be a string type!', __file__, sys._getframe().f_code.co_name)
			return
		if not isinstance(columns_values_list, list):
			self.logger.log('Colums and values need to be a list type!', __file__, sys._getframe().f_code.co_name)
			return
		for columns_values_item in columns_values_list:
			self.insert_one(table_name, columns_values_item)
	
	def update(self, table_name: str, columns_values: dict, where=None):
		'''
		Method for simply executing construction "UPDATE" in sql script
		Examples:
		update(\'table_name\', {\'k1\': \'v1\', \'k2\': 3})
		meaning UPDATE table_name SET k1 = \"v1\", k2 = 3
		-----------------------------------
		update(\'table_name\', {\'k1\': \'v1\', \'k2\': 3}, \'k2 > 18\')
		meaning UPDATE table_name SET k1 = \"v1\", k2 = 3 WHERE k2 > 18
		'''
		try:
			if not isinstance(table_name, str):
				self.logger.log('Table name need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			if not isinstance(columns_values, dict):
				self.logger.log('Colums and values need to be a dictionary type!', __file__, sys._getframe().f_code.co_name)
				return
			command = f'UPDATE {table_name} SET '
			for key, value in columns_values.items():
				command += key + ' = '
				if isinstance(value, str):
					command += f'\"{value}\"'
				else:
					command += f'{value}'
				command += ', '
			command = command[:-2]
			if not where is None:
				command += f' WHERE {where}'
			self.__cursor.execute(command)
			self.__connection.commit()
			self.logger.log(f'Command \"UPDATE\" to {table_name} was successful!', __file__, sys._getframe().f_code.co_name)
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)

	def delete(self, table_name: str, where: str):
		'''
		Method for simply executing construction "DELETE" in sql script
		Examples:
		delete(\'table_name\', \'age > 18\')
		meaning DELETE FROM table_name WHERE age > 18
		-----------------------------------
		delete(\'table_name\', \'name = \"Tomas\"\')
		meaning DELETE FROM table_name WHERE name = \"Tomas\"
		'''
		try:
			if not isinstance(table_name, str):
				self.logger.log('Table name need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			if not isinstance(where, str):
				self.logger.log('Condition "WHERE" need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			command = f'DELETE FROM {table_name} WHERE {where}'
			self.__cursor.execute(command)
			self.__connection.commit()
			self.logger.log(f'Command \"DELETE\" to {table_name} was successful!', __file__, sys._getframe().f_code.co_name)
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)

	def clear_table(self, table_name: str):
		'''
		Method for clear table it\'s using construction "DELETE"
		Exapmles:
		clear_table(\'table_name\')
		meaning DELETE FROM table_name
		'''
		try:
			if not isinstance(table_name, str):
				self.logger.log('Table name need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			command = f'DELETE FROM {table_name}'
			self.__cursor.execute(command)
			self.__connection.commit()
			self.logger.log(f'Table {table_name} was cleared!', __file__, sys._getframe().f_code.co_name)
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)

	def create_table(self, table_name: str, columns: dict, referense=None):
		'''
		Method for simply executiong construction "CREATE" in sql script
		Examples:
		create_table(\'table_name\', [\'id INTEGER PRIMARY KEY\', \'name TEXT NOT NULL\'])
		meaning CREATE TABLE IF NOT EXISTS table_name (id INTEGER PRIMARY KEY, name TEXT NOT NULL)
		-----------------------------------
		create_table(\'table_name\', [\'id INTEGER PRIMARY KEY\', \'content TEXT\'], 'CONSTRAINT contents_titles_fk\nFOREIGN KEY (content) REFERENCES titles (id)')
		meaning CREATE TABLE IF NOT EXISTS table_name (id INTEGER PRIMARY KEY, content TEXT, CONSTRAINT contents_titles_fk\nFOREIGN KEY (content) REFERENCES titles (id))
		'''
		try:
			if not isinstance(table_name, str):
				self.logger.log('Table name need to be a string type!', __file__, sys._getframe().f_code.co_name)
				return
			command = f'CREATE TABLE IF NOT EXISTS {table_name} (\n'
			for column in columns:
				command += column + ',\n'
			if referense is None:
				command = command[:-2] + ')'
			else:
				command += referense + ')'
			self.__cursor.execute(command)
			self.__connection.commit()
			self.logger.log(f'Command \"CREATE\" to {table_name} was successful!', __file__, sys._getframe().f_code.co_name)
		except Exception as ex_message:
			self.logger.log(ex_message, __file__, sys._getframe().f_code.co_name)
