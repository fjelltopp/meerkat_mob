#!/usr/local/bin/python3
"""
This is a utility script to help set up some accounts for testing and
development. It create a registered, manager and root account for every country
currently under active development. NOTE: the passwords for every account is
just 'password'.

Run:
    `local_db.py --clear` (To get rid of any existing db)
    `local_db.py --setup` (To setup the db tables)
    `local_db.py --populate` (To populate the tables with accounts & roles)
    `local_db.py --list` (To list the acounts in the db)

If no flag is provided (i.e. you just run `local_db.py`) it will perform all
steps in the above order.

You can run these commands inside the docker container if there are database
issues.
"""
from meerkat_gcm import app
import boto3
import botocore
import os
import ast
import argparse

# PARSE ARGUMENTS
parser = argparse.ArgumentParser()
parser.add_argument(
    '--setup',
    help='Setup the local dynamodb development database.', action='store_true'
)
parser.add_argument(
    '--list',
    help='List data from the local dynamodb development database.',
    action='store_true'
)
parser.add_argument(
    '--clear',
    help='Clear the local dynamodb development database.',
    action='store_true'
)
parser.add_argument(
    '--populate',
    help='Populate the local dynamodb development database.',
    action='store_true'
)
args = parser.parse_args()
args_dict = vars(args)

# If no arguments are specified assume that we want to do everything.
if all(arg is False for arg in args_dict.values()):
    print("Re-starting the dev database.")
    for arg in args_dict:
        args_dict[arg] = True

# Clear the database
if args.clear:
    db = boto3.resource(
        'dynamodb',
        endpoint_url='http://dynamodb:8000',
        region_name='eu_west'
    )
    try:
        print('Cleaning the dev db.')
        response = db.Table(app.config['TABLE_DEVICE_REPORTS']).delete()
        print('Cleaned the db.')
    except botocore.exceptions.ClientError as e:
        print(e)

# Create the db tables required and perform any other db setup.
if args.setup:
    print('Creating dev db')

    # Create the client for the local database
    db = boto3.client(
        'dynamodb',
        endpoint_url='http://dynamodb:8000',
        region_name='eu_west'
    )

    # Create the required tables in the database
    response = db.create_table(
        TableName=app.config['TABLE_DEVICE_REPORTS'],
        AttributeDefinitions=[
            {'AttributeName': 'id', 'AttributeType': 'S'}
        ],
        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print("Table {} status: {}".format(
        app.config['TABLE_DEVICE_REPORTS'],
        response['TableDescription'].get('TableStatus')
    ))


# Put initial fake data into the database.
if args.populate:

    print('Populating the mob dev db.')

    print('Populated dev db.')

# Finally list all items in the database, so we know what it is populated with.
if args.list:
    print('Listing data in the database.')
    db = boto3.resource(
        'dynamodb',
        endpoint_url='http://dynamodb:8000',
        region_name='eu_west'
    )
    try:
        # List device reports.
        reports_table = db.Table(app.config['TABLE_DEVICE_REPORTS'])
        reports = reports_table.scan().get("Items", [])
        if reports:
            print("Device reports received:")
            for report in reports:
                print("{} - {}".format(
                    report['deviceid'],
                    report['received']
                ))
        else:
            print("No device reports received.")

    except Exception as e:
        print("Listing failed. Has database been setup?")
