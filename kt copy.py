from asyncore import write
from email import header
from multiprocessing import AuthenticationError
from pprint import pformat
import re
from webbrowser import get
from wsgiref import headers
import boto3  # import Boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import requests
import hashlib
from flask import Flask, render_template, request, jsonify
import pandas as pd
from flatten_json import flatten
from os import listdir
from os.path import isfile, join

PK = 'T#root#L#en-US#FB#FS#627a85455a69b2000945ba60'
date = '2022-05-24'
queryPath = r'C:\Users\bzhang\Documents\AI\KT\query\queryResult.json'
folderPath = r'C:\Users\bzhang\Documents\AI\KT\outputJSON'
filePath = r'C:\Users\bzhang\Documents\AI\KT\outputJSON\output.json'

#Query data from DynamoDB and export to json file
def query():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('webiny-151559f')

    response = table.query(
        KeyConditionExpression = Key('PK').eq(PK),
        FilterExpression = Attr('createdOn').gte(date),
        ProjectionExpression = "#d, SK, meta",
        ExpressionAttributeNames = {'#d': 'data'}
    )

    data = response['Items']
    with open(queryPath, 'w+') as f:
        f.write(json.dumps(data))

#Split one json file to multiple json files(one record each file)
def split():
    with open(queryPath,'r') as in_json_file:

    # Read the file and convert it to a dictionary
        json_obj_list = json.load(in_json_file)

        for json_obj in json_obj_list:
            filename = json_obj['SK']+'.json'  

            with open(filename, 'w') as out_json_file:
                json.dump(json_obj, out_json_file, indent=4)


#Read through each json file and flatten then export each json file
def flattenJSON():
    dir_path = r'C:\Users\bzhang\Documents\AI\KT\outputJSON'
    out_path = r"C:\Users\bzhang\Documents\AI\KT\outputJSON\output.json"
    # get all json file under dir path
    all_json_files = [
        join(dir_path, f) for f in listdir(dir_path) 
        if isfile(join(dir_path, f)) and f.endswith(".json")
        ]
    array = []          

    for file_path in all_json_files:
        with open(file_path) as input_file:
            json_array = json.load(input_file)
            x = json_array['SK']
            documentID = hashlib.md5(x.encode('utf-8')).hexdigest()
            #print(documentID)
            new_field = {'document_id':documentID}
            json_array.update(new_field)
            flatData = flatten(json_array)
            #print(flatData)
            array.append(flatData)
            print(array)

            with open(out_path, "w") as outfile:
                json.dump(array, outfile)

#API call push records to IBM
def APItoIBM():
    url = 'https://api.us-south.discovery.watson.cloud.ibm.com/instances/5d0181db-0673-4b6b-9c2d-9bfe00f1d771/v2/projects/720c567d-10c4-4276-bfc3-797e8f19b168/collections/a2e9ff7d-2263-dd21-0000-0180b8f0c2f0/documents/?version=2020-08-30'
    headers = {'Accept': 'application/json'}
    files = {'file': open(filePath, "rb")}
     
    response = requests.post(url, headers=headers, files=files, auth=('apikey', 'y8J9pF1zud0AK5E1-dGLFh-YPc0V_6CbmZGQ96kq_0Zc'))
    print(response.status_code)
    print(response.text)


#query()
split()
#flattenJSON()
#APItoIBM()
