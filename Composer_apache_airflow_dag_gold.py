from dependencies.cdp_integration import execute_cdp_integration
from dependencies.onetrust_integration import call_cf_onetrust_bronze_customers
from dependencies.subscribers_sfmc import call_api_marketing_cloud

from airflow import DAG
from airflow import configuration
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.common.utils import id_token_credentials as id_token_credential_utils
import google.auth.transport.requests
from dependencies.utils import read_query
from google.auth.transport.requests import AuthorizedSession
import os
from datetime import date, datetime, timedelta
from pytz import timezone
from google.oauth2 import service_account
from google.oauth2 import id_token
import requests
import http.client
import requests
import json
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from google.oauth2 import id_token
import google.auth
from google.cloud import bigquery
from google.auth.transport.requests import AuthorizedSession
import time
import uuid
import re


QUERIES_PATH = os.path.join(
    configuration.get('core', 'dags_folder'), 'queries')
START_DATE = datetime(2017, 1, 1, 00, 00, 0)
QUINZE_MINUTOS = timedelta(minutes=15)
TRINTA_MINUTOS = timedelta(minutes=30)
SESSENTA_MINUTOS = timedelta(minutes=60)
PROJECT_ID = 'cdp-jhsf-prod'
BUCKET_CDP = 'cdp_jhsf_bucket_3'



default_args = {
    'owner': 'Rafael Brito',
    'depends_on_past': False,
    'start_date': START_DATE,
    "retries": 10,
    "retry_delay": timedelta(minutes=2)
}

with DAG(dag_id='dag_bronze_silver_cjfood', 
          default_args=default_args,
          schedule_interval='10 3 * * *',
          catchup=False, max_active_runs=1,
          concurrency=2) as dag:

   ########################################################################
   ######################################################################## 
   #-------------------------------BRONZE---------------------------------
   ########################################################################
   ########################################################################  

    cjfoods_bronze_coupons = SimpleHttpOperator(
        task_id= "cjfoods_bronze_coupons",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "coupons"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}
    )

    cjfoods_bronze_customer_addresses = SimpleHttpOperator(
        task_id= "cjfoods_bronze_customer_addresses",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "customer_addresses"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )


    cjfoods_bronze_customer_documents = SimpleHttpOperator(
        task_id= "cjfoods_bronze_customer_documents",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "customer_documents"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )


    cjfoods_bronze_customer_payments = SimpleHttpOperator(
        task_id= "cjfoods_bronze_customer_payments",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "customer_payments"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )

    cjfoods_bronze_customer_phones = SimpleHttpOperator(
        task_id= "cjfoods_bronze_customer_phones",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "customer_phones"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )

    cjfoods_bronze_customers = SimpleHttpOperator(
        task_id= "cjfoods_bronze_customers",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "customers"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )

    cjfoods_bronze_product = SimpleHttpOperator(
        task_id= "cjfoods_bronze_product",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "product"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )

    cjfoods_bronze_product_categories_product_category = SimpleHttpOperator(
        task_id= "cjfoods_bronze_product_categories_product_category",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "product_categories_product_category"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )

    cjfoods_bronze_product_category = SimpleHttpOperator(
        task_id= "cjfoods_bronze_product_category",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "product_category"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )
    cjfoods_bronze_product_price = SimpleHttpOperator(
        task_id= "cjfoods_bronze_product_price",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "product_price"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )

    cjfoods_bronze_orders = SimpleHttpOperator(
        task_id= "cjfoods_bronze_orders",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "orders"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )   

    cjfoods_bronze_order_products = SimpleHttpOperator(
        task_id= "cjfoods_bronze_order_products",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "order_products"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    ) 

    cjfoods_bronze_order_status_histories = SimpleHttpOperator(
        task_id= "cjfoods_bronze_order_status_histories",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-cjfood-partitioned',
        data='{"table": "order_status_histories"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    ) 

    cjfoods_bronze_seller = SimpleHttpOperator(
        task_id= "cjfoods_bronze_seller",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "seller"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )   
    cjfoods_bronze_seller_category = SimpleHttpOperator(
        task_id= "cjfoods_bronze_seller_category",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "seller_category"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )

    cjfoods_bronze_one_signal_data = SimpleHttpOperator(
        task_id= "cjfoods_bronze_one_signal_data",
        method='POST',
        http_conn_id='http_cloud_functions',
        endpoint='us-central1-cdp-jhsf-prod.cloudfunctions.net/function-get-tables-cjfood',
        data='{"table": "one_signal_data"}',
        headers={"Content-Type": "application/json","Accept": "application/json, text/plain, */*"}

    )

   ########################################################################
   ######################################################################## 
   #-------------------------------SILVER---------------------------------
   ########################################################################
   ########################################################################


   #----------------------------------------------------------------------
   #------------------------------PRODUCTS--------------------------------
   #----------------------------------------------------------------------
    cjfood_silver_products = BigQueryOperator(
        task_id='cjfood_silver_products',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_products_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

    cjfood_silver_categories_per_products = BigQueryOperator(
        task_id='cjfood_silver_categories_per_products',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_categories_per_products_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.silver.categories_per_products_cjfood',
    )

    cjfood_silver_product_categories = BigQueryOperator(
        task_id='cjfood_silver_product_categories',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_product_categories_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.silver.product_categories_cjfood',
    )

    cjfood_silver_product_prices = BigQueryOperator(
        task_id='cjfood_silver_product_prices',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_product_prices_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

   #----------------------------------------------------------------------
   #--------------------------------ORDERS--------------------------------
   #----------------------------------------------------------------------

    cjfood_silver_orders = BigQueryOperator(
        task_id='cjfood_silver_orders',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_orders_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

    cjfood_silver_order_products = BigQueryOperator(
        task_id='cjfood_silver_order_products',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_order_products_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

    cjfood_silver_order_status_histories = BigQueryOperator(
        task_id='cjfood_silver_order_status_histories',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_order_status_histories_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

   #----------------------------------------------------------------------
   #-----------------------------CUSTOMERS--------------------------------
   #----------------------------------------------------------------------

    cjfood_silver_customers = BigQueryOperator(
        task_id='cjfood_silver_customers',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_customers_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

    cjfood_silver_customer_documents = BigQueryOperator(
        task_id='cjfood_silver_customer_documents',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_customer_documents_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

    cjfood_silver_customer_addressess = BigQueryOperator(
        task_id='cjfood_silver_customer_addressess',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_customer_addressess_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

    cjfood_silver_customer_phones = BigQueryOperator(
        task_id='cjfood_silver_customer_phones',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_customer_phones_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

    cjfood_silver_customer_payments = BigQueryOperator(
        task_id='cjfood_silver_customer_payments',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_customer_payments_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

   #----------------------------------------------------------------------
   #-------------------------------SELLERS--------------------------------
   #----------------------------------------------------------------------

    cjfood_silver_sellers = BigQueryOperator(
        task_id='cjfood_silver_sellers',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_sellers_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )


   #----------------------------------------------------------------------
   #-------------------------------one_signal_data------------------------
   #----------------------------------------------------------------------

    cjfood_silver_one_signal_data = BigQueryOperator(
        task_id='cjfood_silver_one_signal_data',
        sql=read_query(QUERIES_PATH, 'app_cjfood/silver/silver_one_signal_data_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.silver.one_signal_data_cjfood'
    )

   ########################################################################
   ######################################################################## 
   #---------------------------------GOLD---------------------------------
   ########################################################################
   ########################################################################


   #----------------------------------------------------------------------
   #--------------------------GOLD - CUSTOMERS----------------------------
   #----------------------------------------------------------------------

    cjfood_gold_customer_addresses = BigQueryOperator(
        task_id='cjfood_gold_customer_addresses',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_customer_addresses_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.customer_addresses_cjfood',
    )

    cjfood_gold_customer_documents = BigQueryOperator(
        task_id='cjfood_gold_customer_documents',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_customer_documents_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.customer_documents_cjfood',
    )

    cjfood_gold_customer_phones = BigQueryOperator(
        task_id='cjfood_gold_customer_phones',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_customer_phones_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.customer_phones_cjfood',
    )

    cjfood_gold_customers = BigQueryOperator(
        task_id='cjfood_gold_customers',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_customers_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.customers_cjfood',
    )

   #----------------------------------------------------------------------
   #--------------------------GOLD - PRODUCTS-----------------------------
   #----------------------------------------------------------------------

    cjfood_gold_products = BigQueryOperator(
        task_id='cjfood_gold_products',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_products_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.products_cjfood'
    )

    cjfood_gold_categories_per_products = BigQueryOperator(
        task_id='cjfood_gold_categories_per_products',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_categories_per_products_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.categories_per_products_cjfood'
    )

    cjfood_gold_product_categories = BigQueryOperator(
        task_id='cjfood_gold_product_categories',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_product_categories_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.product_categories_cjfood'
    )

    cjfood_gold_product_prices = BigQueryOperator(
        task_id='cjfood_gold_product_prices',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_product_prices_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.product_prices_cjfood'
    )

   #----------------------------------------------------------------------
   #----------------------------GOLD - ORDERS-----------------------------
   #----------------------------------------------------------------------

    cjfood_gold_orders = BigQueryOperator(
        task_id='cjfood_gold_orders',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_orders_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.orders_cjfood',
    )

    cjfood_gold_order_products = BigQueryOperator(
        task_id='cjfood_gold_order_products',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_order_products_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.order_products_cjfood',
    )

    cjfood_gold_order_status_histories = BigQueryOperator(
        task_id='cjfood_gold_order_status_histories',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_order_status_histories_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.order_status_histories_cjfood',
    )
   #----------------------------------------------------------------------
   #--------------------------- GOLD - SELLERS ---------------------------
   #----------------------------------------------------------------------

    cjfood_gold_sellers = BigQueryOperator(
        task_id='cjfood_gold_sellers',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_sellers_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.sellers_cjfood',
    )

   #----------------------------------------------------------------------
   #------------------------ GOLD - ONE SIGNAL DATA ----------------------
   #----------------------------------------------------------------------

    cjfood_gold_one_signal_data = BigQueryOperator(
        task_id='cjfood_gold_one_signal_data',
        sql=read_query(QUERIES_PATH, 'app_cjfood/gold/gold_one_signal_data_cjfood.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default',
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table='cdp-jhsf-prod.gold.one_signal_data_cjfood',
    )

   ########################################################################
   ######################################################################## 
   #-------------------------------CDP---------------------------------
   ########################################################################
   ########################################################################


    # cjfood_cdp_products = PythonOperator(
    #     task_id='cjfood_cdp_products',
    #     provide_context=True,
    #     python_callable=execute_cdp_integration,
    #     op_args=[
    #        QUERIES_PATH,
    #        '/app_cjfood/cdp_products.sql',
    #        'export_cjfoods_products_cdp.csv',
    #        'cjfoods_products',
    #        'cjfoods_products',
    #    ],
    # )

    
    query_customers_cdp = read_query(QUERIES_PATH,'app_cjfood/cdp/customers_cdp.sql')
    customers_cdp = DataflowTemplateOperator(
        task_id='customers_cdp',
        template= f'gs://jhsf_stage/template/cjfood/template_cdp_gcs_conector_customers',
        parameters={
            "sql_query": query_customers_cdp,
            "full_path_csv_name": f'gs://{BUCKET_CDP}/cdp/customers_cjfood/customers_'+'{{ ((macros.datetime.now() - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d") }}',
            "fieldnames_csv": "id,birthDate,documentNumber,email,gender,name,createdAt,updatedAt" 
        },
        gcp_conn_id='google_cloud_default',

    )



    cjfood_categories_per_products_cdp = PythonOperator(
        task_id='cjfood_categories_per_products_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_categories_per_products_cdp.sql',
           'export_cjfood_categories_per_products_cdp.csv',
           'categories_per_products_cjfood',
           'cjfood_categories_per_products',
       ],
    )


    cjfood_customer_addresses_cdp = PythonOperator(
        task_id='cjfood_customer_addresses_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_customer_addresses_cdp.sql',
           'export_cjfood_categories_per_products_cdp.csv',
           'customer_addresses_cjfood',
           'cjfood_customer_addresses',
       ],
    )


    cjfood_customer_documents_cdp = PythonOperator(
        task_id='cjfood_customer_documents_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_customer_documents_cdp.sql',
           'export_cjfood_customer_documents_cdp.csv',
           'customer_documents_cjfood',
           'cjfood_customer_documents',
       ],
    )


    cjfood_customer_phones_cdp = PythonOperator(
        task_id='cjfood_customer_phones_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_customer_phones_cdp.sql',
           'export_cjfood_customer_phones_cdp.csv',
           'customer_phones_cjfood',
           'cjfood_customer_phones',
       ],
    )


    cjfood_customers_cdp = PythonOperator(
        task_id='cjfood_customers_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_customers_cdp.sql',
           'export_cjfood_customers_cdp.csv',
           'customers_cjfood',
           'cjfood_customers',
       ],
    )


    # cjfood_one_signal_data_cdp = PythonOperator(
    #     task_id='cjfood_one_signal_data_cdp',
    #     provide_context=True,
    #     python_callable=execute_cdp_integration,
    #     op_args=[
    #        QUERIES_PATH,
    #        '/app_cjfood/cdp/cjfood_one_signal_data_cdp.sql',
    #        'export_cjfood_one_signal_data_cdp.csv',
    #        'one_signal_data_cjfood',
    #        'cjfood_one_signal_data',
    #    ],
    # )


    cjfood_order_products_cdp = PythonOperator(
        task_id='cjfood_order_products_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_order_products_cdp.sql',
           'export_cjfood_order_products_cdp.csv',
           'order_products_cjfood',
           'cjfood_order_products',
       ],
    )


    cjfood_order_status_histories_cdp = PythonOperator(
        task_id='cjfood_order_status_histories_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_order_status_histories_cdp.sql',
           'export_cjfood_order_status_histories_cdp.csv',
           'order_status_histories_cjfood',
           'cjfood_order_status_histories',
       ],
    )


    cjfood_orders_cdp = PythonOperator(
        task_id='cjfood_orders_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_orders_cdp.sql',
           'export_cjfood_orders_cdp.csv',
           'orders_cjfood',
           'cjfood_orders',
       ],
    )

    cjfood_product_categories_cdp = PythonOperator(
        task_id='cjfood_product_categories_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_product_categories_cdp.sql',
           'export_cjfood_product_categories_cdp.csv',
           'product_categories_cjfood',
           'cjfood_product_categories',
       ],
    )

    cjfood_product_prices_cdp = PythonOperator(
        task_id='cjfood_product_prices_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_product_prices_cdp.sql',
           'export_cjfood_product_prices_cdp.csv',
           'product_prices_cjfood',
           'cjfood_product_prices',
       ],
    )

    cjfood_products_cdp = PythonOperator(
        task_id='cjfood_products_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_products_cdp.sql',
           'export_cjfood_products_cdp.csv',
           'products_cjfood',
           'cjfood_products',
       ],
    )

    cjfood_sellers_cdp = PythonOperator(
        task_id='cjfood_sellers_cdp',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           'app_cjfood/cdp/cjfood_sellers_cdp.sql',
           'export_cjfood_sellers_cdp.csv',
           'sellers_cjfood',
           'cjfood_sellers',
       ],
    )

#------------------------ OTHERS ------------------------
cjfoods_bronze_coupons
cjfoods_bronze_one_signal_data  >> cjfood_silver_one_signal_data >> cjfood_gold_one_signal_data

#----------------------- CUSTOMERS -----------------------
cjfoods_bronze_customers >> cjfoods_bronze_customer_addresses >> cjfoods_bronze_customer_documents >> cjfoods_bronze_customer_payments >> cjfoods_bronze_customer_phones >> cjfood_silver_customer_addressess >> cjfood_silver_customer_documents >> cjfood_silver_customer_phones >> cjfood_silver_customer_payments >> cjfood_silver_customers
cjfood_silver_customers >> cjfood_gold_customer_addresses
cjfood_silver_customers >> cjfood_gold_customer_documents
cjfood_silver_customers >> cjfood_gold_customer_phones
cjfood_silver_customers >> cjfood_gold_customers
cjfood_gold_customers >> customers_cdp

#------------------------ PRODUCTS ----------------------
cjfoods_bronze_product >> cjfoods_bronze_product_categories_product_category >> cjfoods_bronze_product_category >> cjfoods_bronze_product_price >> cjfood_silver_product_prices >> cjfood_silver_categories_per_products >> cjfood_silver_product_categories >> cjfood_silver_products
cjfood_silver_products >> cjfood_gold_categories_per_products
cjfood_silver_products >> cjfood_gold_product_categories
cjfood_silver_products >> cjfood_gold_product_prices
cjfood_silver_products >> cjfood_gold_products

#------------------------- ORDERS -----------------------
cjfoods_bronze_orders >> cjfoods_bronze_order_products >> cjfood_silver_orders >> cjfood_silver_order_products
cjfood_silver_order_products >> cjfood_gold_orders
cjfood_silver_order_products >> cjfood_gold_order_products
cjfoods_bronze_order_status_histories >> cjfood_silver_order_status_histories
cjfood_silver_order_status_histories >> cjfood_gold_order_status_histories

#------------------------- SELLERS -----------------------
cjfoods_bronze_seller >> cjfoods_bronze_seller_category >> cjfood_silver_sellers >> cjfood_gold_sellers


#------------------------- CDP -----------------------
cjfood_gold_categories_per_products >> cjfood_categories_per_products_cdp
cjfood_gold_customer_addresses >> cjfood_customer_addresses_cdp
cjfood_gold_customer_documents >> cjfood_customer_documents_cdp
cjfood_gold_customer_phones >> cjfood_customer_phones_cdp
cjfood_gold_customers >> cjfood_customers_cdp
cjfood_gold_order_products >> cjfood_order_products_cdp
cjfood_gold_order_status_histories >> cjfood_order_status_histories_cdp
cjfood_gold_orders >> cjfood_orders_cdp
cjfood_gold_product_categories >> cjfood_product_categories_cdp
cjfood_gold_product_prices >> cjfood_product_prices_cdp
cjfood_gold_products >> cjfood_products_cdp
cjfood_gold_sellers >> cjfood_sellers_cdp
