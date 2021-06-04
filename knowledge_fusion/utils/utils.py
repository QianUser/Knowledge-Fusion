import json
import os
import time
from functools import wraps


def get_cur_time_rep():
	return time.strftime('%Y-%m-%d %H:%M:%S')


def default_return(value):
	def decorate(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			result = func(*args, **kwargs)
			return result if result is not None else value

		return wrapper

	return decorate


class Singleton(type):

	def __init__(cls, *args, **kwargs):
		cls.__instance = None
		super().__init__(*args, **kwargs)

	def __call__(cls, *args, **kwargs):
		if cls.__instance is None:
			cls.__instance = super().__call__(*args, **kwargs)
			return cls.__instance
		else:
			return cls.__instance


def write_json(data, filename, encoding='utf-8'):
	if not os.path.exists(os.path.dirname(filename)):
		os.makedirs(os.path.dirname(filename))
	with open(filename, 'w', encoding=encoding) as file:
		json.dump(data, file)


def read_json(filename, encoding='utf-8'):
	with open(filename, encoding=encoding) as file:
		return json.load(file)