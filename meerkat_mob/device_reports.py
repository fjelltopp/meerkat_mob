import boto3

import json
import datetime

from util import unwrap_data_types

import logging

#gcm_device_reports

def get_latest_device_reports_by_organization(organization, start_date=datetime.datetime.now() - datetime.timedelta(weeks=1), end_date=datetime.datetime.now()):
    dynamoDB_client = boto3.client('dynamodb', region_name='eu-west-1')
    #device_reports = dynamoDB_client.Table('gcm_device_reports')

    start_date_string = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
    end_date_string = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
    

    query = dynamoDB_client.query(
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
    return query

def get_latest_device_reports_by_deviceid(deviceid):
    dynamoDB_client = boto3.client('dynamodb', region_name='eu-west-1')

    start_date = datetime.datetime.now() - datetime.timedelta(weeks=4)
    end_date = datetime.datetime.now() 

    start_date_string = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
    end_date_string = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')

    query = dynamoDB_client.query(
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
    items=sorted(items,key=lambda item: item['received']['S'])

    return unwrap_data_types(items[-1])