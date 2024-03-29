import os

from datetime import datetime


class Log:
	def __init__(self, log_file_name: str):
		if not isinstance(log_file_name, str):
			raise TypeError('file_name need to be a string type!')
		if not '.log' in log_file_name:
			log_file_name = log_file_name + '.log'
		self.log_dir = os.path.join(os.path.normpath(os.getcwd() + os.sep + os.pardir), 'logs')
		self.log_file_name = os.path.join(log_dir, log_file_name)

	def log(self, message: str, file_name: str, function_name: str):
		'''
		Method for logging errors and actions
		Examples:
		log(\'My temp message\', __file__, sys._getframe().f_code.co_name)
		in log file you\'l see:
		1. Current datetime
		2. File name
		3. Function name
		4. Message
		'''
		if not isinstance(message, str):
			raise TypeError('Message need to be a string type!')
		if not isinstance(file_name, str):
			raise TypeError('Name of file need to be a string type!')
		if not isinstance(function_name, str):
			raise TypeError('Name of function need to be a string type!')
		time_now = datetime.now()
		with open(self.log_file_name, 'a') as file:
			file.write(str(time_now) + '\n')
			file.write('File: ' + file_name + '\t' + 'Function: ' + function_name + '\n')
			file.write(message + '\n')
			file.write('__________________________________' + '\n')
