"""
Resource for querying Google Cloud Messaging device reports

"""

import boto3

import json
import datetime

from meerkat_mob.util import unwrap_data_types

from flask import request
from flask_restful import Resource, reqparse
import logging





class OrganizationReport(Resource):

    def __init__(self):

        self.dynamoDB_client = boto3.client('dynamodb', region_name='eu-west-1')

    def get(self, organization, start_date=datetime.datetime.now() - datetime.timedelta(weeks=1), end_date=datetime.datetime.now()):
        dynamoDB_client = boto3.client('dynamodb', region_name='eu-west-1')
        #device_reports = dynamoDB_client.Table('gcm_device_reports')

        start_date_string = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        end_date_string = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        

        query = self.dynamoDB_client.query(
            TableName='gcm_device_reports',
            IndexName='organization-received-index',
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='deviceid, received, app_version, app_version_code',
            KeyConditionExpression='organization = :org AND received BETWEEN :start_date AND :end_date',
            ExpressionAttributeValues={
                    ":org": {"S": organization},
                    ":start_date": {"S": start_date_string},
                    ":end_date": {"S" : end_date_string}
                }
            )
        items=query['Items']

        reports=[]

        # sort the returned list by deviceid and the datetime when the report was received 
        items = sorted(items,key=lambda item:item['deviceid']['S'] + item['received']['S'],reverse=True)

        # make sure only the newest report for each device is returned
        for item in items:
            if len(reports) == 0 or reports[-1]['deviceid'] != item['deviceid']['S']:
                reports.append(unwrap_data_types(item))

        retval={'device_reports':reports}
        return retval

class DeviceReport(Resource):

    def __init__(self):

        self.dynamoDB_client = boto3.client('dynamodb', region_name='eu-west-1')


    def get(self, deviceid):
        dynamoDB_client = boto3.client('dynamodb', region_name='eu-west-1')

        start_date = datetime.datetime.now() - datetime.timedelta(weeks=4)
        end_date = datetime.datetime.now() 


        deviceid='imei:' + deviceid

        start_date_string = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        end_date_string = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')

        query = self.dynamoDB_client.query(
            TableName='gcm_device_reports',
            IndexName='deviceid-received-index',
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression='deviceid, received, app_version, app_version_code',
            KeyConditionExpression='deviceid = :deviceid AND received BETWEEN :start_date AND :end_date',
            ExpressionAttributeValues={
                    ":deviceid": {"S": deviceid},
                    ":start_date": {"S": start_date_string},
                    ":end_date": {"S" : end_date_string}
                }
            )


        items=query['Items']

        # sort the items by received timestamp and return newest
        items=sorted(items, key = lambda item: item['received']['S'], reverse=True)

        return {'device_reports' : unwrap_data_types(items[0])}

    def post(self, start_date=datetime.datetime.now() - datetime.timedelta(weeks=1), end_date=datetime.datetime.now()):
        
        device_request = request.json

        # print("DEBUG: " + str(device_request) + "\n")
        
        # Validate request
        try:
            assert(type(device_request) is list)
            for item in device_request:
                assert(type(item) is str)
                assert(item.find('(')==-1)
                assert(item.find(')')==-1)
        except AssertionError:
            return {"message":"Device id list was not a list of valid device id's"}

        start_date_string = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
        end_date_string = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')

        KeyConditionExpression='received BETWEEN :start_date AND :end_date AND deviceid = :deviceid'
        
        retval={'device_reports':[]}
        
        # Iterate through the device id's given as POST parameter and fetch device reports
        for item in device_request:
            if len(item)<6 or item[0:5]!='imei:':
                item = 'imei:' + item

            print("DEBUG: " + str(item) + "\n")
            ExpressionAttributeValues={
                ":start_date": {"S": start_date_string},
                ":end_date": {"S" : end_date_string},
                ":deviceid": {"S" : item}
            }

            query = self.dynamoDB_client.query(
                TableName='gcm_device_reports',
                IndexName='deviceid-received-index',
                Select='SPECIFIC_ATTRIBUTES',
                ProjectionExpression='deviceid, received, app_version, app_version_code',
                KeyConditionExpression=KeyConditionExpression,
                ExpressionAttributeValues=ExpressionAttributeValues
                )

            reports=query['Items']


            if len(reports) > 0:
                # Sort device reports for the device and return latest one
                reports=sorted(reports, key = lambda item: item['received']['S'], reverse=True)
                report=unwrap_data_types(reports[0])

                retval['device_reports'].append(report)

        return retval