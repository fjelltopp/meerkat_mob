"""
config.py

Configuration and settings
"""
import os


def from_env(env_var, default):
	"""
	Gets value from envrionment variable or uses default

	Args:
		env_var: name of envrionment variable
		default: the default value
	"""
	new = os.environ.get(env_var)
	if new:
		return new
	else:
		return default

class Config(object):
	DB_URL = from_env("DB_URL", "http://dynamodb:8000")
	TABLE_DEVICE_REPORTS = 'gcm_device_reports'

class Production(Config):
	PRODUCTION = True
	DB_URL = from_env("DB_URL", "https://dynamodb.eu-west-1.amazonaws.com")

class Development(Config):
	DEBUG = True
	TESTING = True