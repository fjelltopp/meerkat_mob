"""
meerkat_mob.py

Root Flask app for the Meerkat Mob.
"""

from flask import Flask, jsonify
from flask_restful import Api
import boto3
import logging
import os
import json

# Create the Flask app
app = Flask(__name__)
logging.warning("Config object: {}".format(
    os.getenv('CONFIG_OBJECT', 'config.Development')
))
app.config.from_object(os.getenv('CONFIG_OBJECT', 'config.Development'))
api = Api(app)
logging.info('App loaded')

# Display available tables on front page
@app.route('/')
def default():
	db = boto3.resource(
        'dynamodb',
        endpoint_url=app.config['DB_URL'],
        region_name='eu-west-1'
	)
	return "1"


from meerkat_mob.resources.device_reports import OrganizationReport, DeviceReport

# All urls

# Epi weeks
api.add_resource(OrganizationReport, "/org/<organization>")
api.add_resource(DeviceReport, "/device", "/device/<deviceid>")

