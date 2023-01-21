import os
import requests
import datetime
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType, TimestampType
import time


def fetch_bearer_token(tenant, scope, client_id, client_secret):
    auth_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    query_params = {
        "client_id": client_id,
        "scope": scope,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    resp = requests.post(auth_url, data=query_params, headers=headers)
    resp.raise_for_status()
    return resp.json()["access_token"]

print("Bearer Token Access Module Loaded")

#Build schema for log table
schema = StructType([StructField('Log_Timestamp', TimestampType(), True),
                       StructField('Server', StringType(), True),
                       StructField('Model', StringType(), True),
                       StructField('Event_desc', StringType(), True),
                       StructField('Success_YN', StringType(), True)])


# Mode 1: Full Database Refresh
def fullDBRefresh(url,headers):
  body = {
          "CommitMode": "transactional",
          "MaxParallelism": 2,
          "Objects": [],
          "RetryCount": 2,
          "Type": "Full"
  }
  post_req = requests.post(url,json=body,headers=headers)
  return post_req

# Mode 2: Default DB refresh
def defaultDBRefresh(url,headers):
  body = {
          "CommitMode": "transactional",
          "MaxParallelism": 2,
          "Objects": [],
          "RetryCount": 2,
          "Type": "default"
  }
  post_req = requests.post(url,json=body,headers=headers)
  return post_req


# Mode 3: Table Refresh
def tableRefresh(url,headers,tables):
  objects = []
  if str(type(tables[0])) == "<class 'str'>":
    for table in tables:
      item = {"table":table}
      objects.append(item)
  elif str(type(tables[0])) == "<class 'dict'>":
    for dict in tables:
      item = {"table":list(dict.keys())[0], "partition": list(dict.values())[0]}
      objects.append(item)
  body = {
      "Type": "Full",
      "CommitMode": "transactional",
      "MaxParallelism": 2,
      "RetryCount": 2
  }
  body["Objects"] = objects
  post_req = requests.post(url,json=body,headers=headers)
  return post_req


# Logger Module
def loggerModule(resp,authHeaders,interval=30,server="",model=""):
  df_log = spark.createDataFrame([], schema)
  if (resp.status_code == 400 or resp.status_code == 404):
    print("Bad request! " + resp.headers['x-ms-xmlaerror-extended'])
    return
  elif resp.status_code == 401:
    print("Authentication failed!")
    return
  elif resp.status_code == 202:
    print("Server has accepted the request. Logging starting.")
    success = False
    url = resp.headers['Location']
    print(url) #To get runID 
    while success == False:
      status_chk = requests.get(url,headers=authHeaders)
      if (status_chk.json()['status'].lower() == "inprogress"):
        newRow = spark.createDataFrame([(datetime.datetime.now(),server,model,f"Model {model} refresh in progress","N")], schema)
        df_log = df_log.union(newRow)
        print(str(datetime.datetime.now()) + ": Refresh in progress")
        time.sleep(interval)
      elif (status_chk.json()['status'].lower() == "failed"):
        newRow = spark.createDataFrame([(datetime.datetime.now(),server,model,f"Model {model} failed, URL: {url}","N")], schema)
        df_log = df_log.union(newRow)
        print(str(datetime.datetime.now()) +": Refresh failed!")
        break
      elif (status_chk.json()['status'].lower() == "succeeded"):
        newRow = spark.createDataFrame([(datetime.datetime.now(),server,model,f"Model {model} refresh succeeded","Y")], schema)
        df_log = df_log.union(newRow)
        print(str(datetime.datetime.now()) +": Succeeded!")
        success = True
        break
    return df_log
  elif resp.status_code == 409:
    print("Another refresh operation is in progress. Cannot accept request now!")
    newRow = spark.createDataFrame([(datetime.datetime.now(),server,model,f"Model {model} refresh blocked by concurrent refresh, URL: {url}","N")], schema)
    df_log = df_log.union(newRow)
    return df_log

print("Logging Module Loaded")

# Driver Module
def AASRefresh(tenant="",region="westeurope",server="",model="",mode="full",tables=[],logPath="",clientid="",clientsecret="",interval=30):
  scope = "https://*.asazure.windows.net/.default"
  df = spark.createDataFrame([], schema)
  if server.strip() == "" or model.strip() == "":
    print("Missing server/model details")
    return
  try:
    token = fetch_bearer_token(tenant, scope, clientid, clientsecret)
  except Exception as e:
    print(str(e))
    print ("Incorrect authentication. Please check supplied credentials.")
    return
  headers = {"Authorization":f"Bearer {token}"}
  url = f"https://{region}.asazure.windows.net/servers/{server}/models/{model}/refreshes"
  if logPath == "":
    #No logging mode, just hit the API
    if mode.lower() == "full":
      hit = fullDBRefresh(url,headers)
      df = loggerModule(hit,authHeaders=headers,interval=interval)
    elif mode.lower() == "table":
      hit = tableRefresh(url,headers,tables)
      df = loggerModule(hit,authHeaders=headers,interval=interval)
    elif mode.lower() == "default":
      hit = defaultDBRefresh(url,headers)
      df = loggerModule(hit,authHeaders=headers,interval=interval)
  else:
    if mode.lower() == "full":
      hit = fullDBRefresh(url,headers)
      df = loggerModule(hit,authHeaders=headers,server=server,model=model,interval=interval)
    elif mode.lower() == "table":
      hit = tableRefresh(url,headers,tables)
      df = loggerModule(hit,authHeaders=headers,server=server,model=model,interval=interval)
    elif mode.lower() == "default":
      hit = defaultDBRefresh(url,headers)
      df = loggerModule(hit,authHeaders=headers,interval=interval)
    if df is not None:
      try:
        df.write\
        .mode("append")\
        .format("delta")\
        .option("overwriteSchema", "true")\
        .save(f"{logPath}")
      except Exception as e:
        print(str(e))
      display(df)
      
print("Driver Module Loaded: Ready to process!")
