from snowflake import connector as sc
import pandas as pd
import numpy as np
from sqlalchemy.dialects import registry
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
import json

snowflake = sc.connect(
    user='xxxx',
    password='xxxx',
    account='xxxx',
    warehouse='xxxx',
    role='xxxx'
    )

##Generate Bearer Token for API requests
import requests
url = "https://apigtwb2c.us.dell.com/auth/oauth/v2/token"
payload='client_id=xyz'
headers = {
  'Content-Type': 'application/x-www-form-urlencoded'
}

response = requests.request("POST", url, headers=headers, data=payload)
d = {'AUTH_TOKEN' : [response.text]}
df = pd.DataFrame(data=d)

engine = create_engine(URL(
    account = 'xxxx',
    user = 'xxxx',
    password = 'xxxx',
    warehouse = 'xxxx',
    role='xxxx',
    database = 'xxxx',
    schema = 'xxxx'
), pool_pre_ping=True)

connection = engine.connect()

## write current bearer token to table in Snowflake DB
df.to_sql('auth_token', con=engine, if_exists='replace', index=False)

def get_data(query):
    engine = snowflake
    df = pd.read_sql(query, engine)
    return df
  
token_query = ("""
SELECT
try_parse_json(trim(A.AUTH_TOKEN,'[]')):access_token::varchar as TOKEN
FROM AUTH_TOKEN A
""")

auth_token = get_data(token_query)

df = pd.DataFrame(auth_token)
df.to_sql('token', con=engine, if_exists='replace', index=False)

connection.close()
engine.dispose()

token = snowflake.cursor().execute("SELECT * FROM STAGING.DELL_HARDWARE.TOKEN").fetchone()
complete_token = 'Bearer ' + str(token[0])

sc = snowflake.cursor().execute("""
select
12345 as serial_count
;
""").fetchone()
s = pd.DataFrame(sc)
serial_count = s[0]

## example query
serial_query = snowflake.cursor().execute("""
select
distinct(HW_SERIAL) AS HW_SERIAL
FROM SERIALS
;
""")

serials = pd.DataFrame(serial_query)
serial_count = len(serials)
get_arg = str(serials.values.tolist()[0:100]).replace('[','').replace(']','').replace("'","").replace(' ','')


##API Info
url = "https://apigtwb2c.us.dell.com/PROD/sbil/eapi/v5/asset-entitlements?servicetags="
payload={}
headers = {
    'Authorization': complete_token
}

appended_data = []
batch_size = 99

for i in range(0,serial_count, batch_size):
    if i+batch_size <= serial_count:
        j = i+batch_size
        get_arg = str(serials.values.tolist()[i:j]).replace('[','').replace(']','').replace("'","").replace(' ','')
        results = requests.request("GET", url+get_arg, headers=headers, data=payload)
        data = json.loads(results.text)
        normalized_json = pd.json_normalize(data, 'entitlements', 
                                   ['id', 'serviceTag', 'orderBuid', 'shipDate', 'productCode', 'localChannel', 'productId', 'productLineDescription', 'productFamily', 'systemDescription', 'productLobDescription', 'countryCode', 'duplicated', 'invalid', 'itemNumber', 'startDate', 'endDate', 'entitlementType', 'serviceLevelCode', 'serviceLevelDescription', 'serviceLevelGroup'], 
                                record_prefix='ent_', errors='ignore')
        dataframe = pd.DataFrame(normalized_json)
        dataframe.rename(columns={"ent_itemNumber":"ITEM_NUMBER", "ent_startDate":"START_DATE","ent_endDate":"END_DATE","ent_entitlementType" : "ENTITLEMENT_TYPE", "ent_serviceLevelCode":"SERVICE_LEVEL_CODE","ent_serviceLevelDescription":"SERVICE_LEVEL_DESCRIPTION","ent_serviceLevelGroup":"SERVICE_LEVEL_GROUP","id":"ID","serviceTag":"SERVICE_TAG","orderBuid":"ORDER_BUID","shipDate":"SHIP_DATE","productCode":"PRODUCT_CODE","localChannel":"LOCAL_CHANNEL","productId":"PRODUCT_ID","productLineDescription":"PRODUCT_LINE_DESCRIPTION","productFamily":"PRODUCT_FAMILY","systemDescription":"SYSTEM_DESCRIPTION","productLobDescription":"PRODUCT_LOB_DESCRIPTION","countryCode":"COUNTRY_CODE","duplicated":"DUPLICATED","invalid":"INVALID"}, inplace=True)
        dataframe.values.tolist()
        appended_data.append(dataframe)
    else:
        get_arg = str(serials.values.tolist()[i]).replace('[','').replace(']','').replace("'","").replace(' ','')
        results = requests.request("GET", url+get_arg, headers=headers, data=payload)
        data = json.loads(results.text)
        normalized_json = pd.json_normalize(data, 'entitlements', 
                                   ['id', 'serviceTag', 'orderBuid', 'shipDate', 'productCode', 'localChannel', 'productId', 'productLineDescription', 'productFamily', 'systemDescription', 'productLobDescription', 'countryCode', 'duplicated', 'invalid', 'itemNumber', 'startDate', 'endDate', 'entitlementType', 'serviceLevelCode', 'serviceLevelDescription', 'serviceLevelGroup'], 
                                record_prefix='ent_', errors='ignore')
        dataframe = pd.DataFrame(normalized_json)
        dataframe.rename(columns={"ent_itemNumber":"ITEM_NUMBER", "ent_startDate":"START_DATE","ent_endDate":"END_DATE","ent_entitlementType" : "ENTITLEMENT_TYPE", "ent_serviceLevelCode":"SERVICE_LEVEL_CODE","ent_serviceLevelDescription":"SERVICE_LEVEL_DESCRIPTION","ent_serviceLevelGroup":"SERVICE_LEVEL_GROUP","id":"ID","serviceTag":"SERVICE_TAG","orderBuid":"ORDER_BUID","shipDate":"SHIP_DATE","productCode":"PRODUCT_CODE","localChannel":"LOCAL_CHANNEL","productId":"PRODUCT_ID","productLineDescription":"PRODUCT_LINE_DESCRIPTION","productFamily":"PRODUCT_FAMILY","systemDescription":"SYSTEM_DESCRIPTION","productLobDescription":"PRODUCT_LOB_DESCRIPTION","countryCode":"COUNTRY_CODE","duplicated":"DUPLICATED","invalid":"INVALID"}, inplace=True)
        dataframe.values.tolist()
        appended_data.append(dataframe)
      
appended_data = pd.concat(appended_data)

df = pd.DataFrame(appended_data) 

df.to_sql('dell_hardware_data', con=engine, if_exists='replace', index=False)




