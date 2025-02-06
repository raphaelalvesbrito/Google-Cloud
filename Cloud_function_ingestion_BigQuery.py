import mysql.connector
import pandas as pd
import pandas_gbq
import json
from datetime import datetime, timedelta
import os
import sys
import pandas.io.sql as sqlio
from google.cloud import bigquery
from google.oauth2 import service_account

global db_connection

os.environ['GOOGLE_APPLICATION_CREDENTIALS']='./credentials.json'
PROJECT_ID='datalake-owner-prod'


client=bigquery.Client()
credentials = service_account.Credentials.from_service_account_file('credentials.json')

def main(request):
    try:

        db_connection  = mysql.connector.connect(user='user_api', 
                                             password='ABCDE',
                                             host='db-prd.c3qbib0t03ct.us-east-1.rds.amazonaws.com', 
                                             database='warehouse',
                                             port=33306)
        df = sqlio.read_sql_query("SELECT * FROM profile_addresses",db_connection,)   
        

        db_connection.close()

        df.insert(0,"data_insercao", datetime.today().strftime("%Y-%m-%d %H:%M"))
 
        pandas_gbq.to_gbq(df, 'bronze.customer_addresses_membershipid',project_id=PROJECT_ID,credentials=credentials, if_exists='append')
   
    except mysql.connector.Error as err:
        print(err)

    return 'ok'
 
