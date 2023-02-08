"""
This program is going to get list of latest configurations from the HDInsight Cluster
Using Amari REST API -  https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/configuration.md
"""
import json
import argparse
import requests
from requests.exceptions import HTTPError
from requests.auth import HTTPBasicAuth
"""
Configuration object
"""
class Configuration:
  def __init__(self, confType, tagName, versionNumber,href):
    self.confType = confType
    self.versionNumber = versionNumber
    self.tagName = tagName
    self.href = href

def __str__(self):
    return f'confType:{self.confType},tagName:{self.tagName},version:{self.versionNumber},href{self.href}'

# List of program mandatory arguments
argParser = argparse.ArgumentParser()
argParser.add_argument("-cn", "--clusterName", help="cluster name",required=True)
argParser.add_argument("-u", "--user", help="user name",required=True)
argParser.add_argument("-p", "--password", help="user password",required=True)

args = argParser.parse_args()

clusteName=args.clusterName
ambariUserId=args.user
ambariPassword=args.password

# list of configuration items, we will store conf type as key and value will be a Configuration object (based on latest version)
confItemDict={}

try:
    # get list of configuration
    response = requests.get(f'https://{clusteName}.azurehdinsight.net//api/v1/clusters/{clusteName}/configurations',auth = HTTPBasicAuth(ambariUserId,ambariPassword))
    response.raise_for_status()
    # access JSON content
    confData = response.json()
    if "items" in confData:
        confItems = confData['items']
        for item in confItems:
            key = item['type']
            tag=item['tag']
            version = int(item['version'])
            href = item['href']
            if key in confItemDict.keys():
                confObj = confItemDict[key]
                if(confObj.versionNumber < version):
                    confObj.versionNumber= version
                    confObj.tagName = tag
                    confObj.href = href
            else:
                confObj= Configuration(key,tag,version,href)
                confItemDict[key]=confObj
    else:
        print("Key items doesn't exist in the response data")
    
    # by this time we have got all configuration type and their versions
    # now we have to get configuration for invidual type and maximum version number
    for key in confItemDict:
        confObj = confItemDict[key]
        confRESTEndPoint = f"https://{clusteName}.azurehdinsight.net//api/v1/clusters/{clusteName}/configurations?type={confObj.confType}&tag={confObj.tagName}"
        response = requests.get(confRESTEndPoint,auth = HTTPBasicAuth(ambariUserId,ambariPassword))
        # access JSON content
        confTypeData = response.json()
        if "items" in confTypeData:
            confFile = open(f"{confObj.confType}.json", "w")
            if  len(confTypeData['items'])>0 and 'properties' in confTypeData['items'][0]:
                confFile.write(json.dumps(confTypeData['items'][0]['properties']))
                confFile.close()
                print(f"File:{confObj.confType}.json")
            else:
                confFile.write(json.dumps(confTypeData))    
                print(f"File:{confObj.confType}.json without properties")
        else:
            print(f"For Conf type {confObj.confType} items doesn't exist in the response data")

except HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except Exception as err:
    print(f'Other error occurred: {err}')