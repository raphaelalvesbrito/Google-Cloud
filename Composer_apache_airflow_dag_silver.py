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
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
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



default_args = {
    'owner': 'VALTECH',
    'depends_on_past': False,
    'start_date': START_DATE,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

def call_cloud_run(data, **context):
    result = requests.post('https://fisia-centauro-vspra3witq-uc.a.run.app', json={'data' : data})


with DAG(dag_id='dag_bronze_silver_membership_id', 
          default_args=default_args,
          schedule_interval='30 6 * * *',
          catchup=False, max_active_runs=1,
          concurrency=2) as dag:



   ########################################################################
   ######################################################################## 
   #-----------------------------MEMBERSHIP_ID-----------------------------
   ########################################################################
   ########################################################################
    
    
    query_membership_transactions = "SELECT id, buyer_id, store_id, total_value, discount_value, net_value, state, status, description, undefined, points_applied, card_id, installments, is_membership_active, is_pay_active, tax_cashback, tax_cashback_amount, tax_cashback_rewards_amount, tax_administrative, tax_administrative_amount, tax_rewards_transaction, tax_rewards_transaction_amount, tax_credit_card_transaction, tax_credit_card_transaction_amount, tax_split_amount, store_split_amount, store_transfer_amount, points_conversion_factor_in, points_conversion_factor_out, points_expiration, maximum_installments_per_transaction, minimum_transaction_value, zoop_transaction_id, zoop_error_category, zoop_error_message, zoop_payment_status, payment_intent_expiration_time_in_minutes, `type`, created_at, updated_at, order_number, customer_cpf, is_manual, deleted_at, paid_at, row_active, fund_id, sub_store_id, campaign_id, nome_fantasia_without_store, cnpj_without_store, roadpass_transaction_id, card_masked_number, earn_ll_id, pending_points, redeem_ll_id, is_concierge_transaction, is_international, jhsf_pay_order_id, payment_provider, is_additional_card, is_virtual_card, internal_error_description, zoop_error_message_display, zoop_error_response_code FROM transactions"
    bronze_membership_transactions = DataflowTemplateOperator(
        task_id='bronze_membership_transactions',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_transactions,
            "dataset_table_name":  "bronze.transactions_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',
    )

    query_membership_customers = "SELECT id, first_name, last_name, cpf, phone_number, email_address, is_email_confirmed, is_phone_confirmed, is_expired, password, zoop_buyer_id, device_push_id, unique_device_id, login_finger_id, login_face_id, login_iris_id, created_at, updated_at, membership_enabled, deleted_at, gender, birth_date, row_active, is_pj, img_document_front, img_document_behind, customer_kind_id, attendant_customer_id, register_cms, has_roadpass_wallet, created_on_ll_loyalty, income_bracket, document_type, document_number, document_issuer, father_name, mother_name, totvs_code, roadpass_key, jcoins_migrated, block_transaction, llloyalty_migration_id FROM customers"
    bronze_membership_customers = DataflowTemplateOperator(
        task_id='bronze_membership_customers',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_customers,
            "dataset_table_name":  "bronze.customers_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',

    )

    query_membership_customers_wallets = "SELECT id, customer_cpf, balance, created_at, updated_at, deleted_at, row_active, pending_points FROM customer_wallets"
    bronze_membership_customers_wallets = DataflowTemplateOperator(
        task_id='bronze_membership_customers_wallets',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_customers_wallets,
            "dataset_table_name":  "bronze.customer_wallets_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',

    )

    query_membership_stores = "SELECT id, nome_fantasia, razao_social, cnpj, phone_number, email_address, mcc_code, mcc_category, mcc_description, mcc_zoop_id, opening_date, is_membership_active, is_pay_active, tax_cashback, tax_administrative, tax_rewards_transaction, tax_credit_card_transaction, points_conversion_factor_in, points_conversion_factor_out, street, `number`, complement, city, neighborhood, state, zip_code, zoop_store_id, is_zoop_enabled, points_expiration, maximum_installments_per_transaction, minimum_transaction_value, created_at, updated_at, minimum_installment_value, manual_payment_enabled, deleted_at, totvs_code, row_active, municipio_ibge, future_payment_enabled, is_card_payment, is_jcoins_payment, is_sub_store, jhsf_pay_partner_id, is_active_roadpass, is_jcoins_priority_payment, is_concierge_payment_store, liquidation_days FROM stores"
    bronze_membership_stores = DataflowTemplateOperator(
        task_id='bronze_membership_stores',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_stores,
            "dataset_table_name":  "bronze.stores_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',

    )

    query_membership_wallet_executions = "SELECT id, customer_cpf, flow, state, origin, amount, balance, store_id, transaction_id, expiration_time, in_id, created_at, updated_at, deleted_at, row_active, process_date, incorporation_integration_id, nome_fantasia_without_store, cnpj_without_store, roadpass_transaction_id, ll_id, installment_number, cancel_date, installment_value FROM wallet_executions"
    bronze_membership_wallet_executions = DataflowTemplateOperator(
        task_id='bronze_membership_wallet_executions',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_wallet_executions,
            "dataset_table_name":  "bronze.wallet_executions_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',
    )

    query_membership_macro_groups = "SELECT id, macro_name, created_at, updated_at, deleted_at, row_active FROM macro_groups"
    bronze_membership_macro_groups = DataflowTemplateOperator(
        task_id='bronze_membership_macro_groups',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_macro_groups,
            "dataset_table_name":  "bronze.macro_groups_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',
    )

    query_membership_stores_sub_store_macro_group = "SELECT id, store_id, macro_group_id, sub_store_id, created_at, updated_at, deleted_at, row_active FROM stores_sub_store_macro_group"
    bronze_membership_stores_sub_store_macro_group = DataflowTemplateOperator(
        task_id='bronze_membership_stores_sub_store_macro_group',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_stores_sub_store_macro_group,
            "dataset_table_name":  "bronze.stores_sub_store_macro_group_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',
    )

    query_membership_stores_sub_store = "SELECT id, sub_store_name, store_id, created_at, updated_at, deleted_at, row_active FROM stores_sub_store"
    bronze_membership_stores_sub_store = DataflowTemplateOperator(
        task_id='bronze_membership_stores_sub_store',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_stores_sub_store,
            "dataset_table_name":  "bronze.stores_sub_store_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',
    )

    query_membership_customer_addresses = "SELECT id, customer_id, address_title, street, neighborhood, complement, address_number, city, state, country, zip_code, created_at, updated_at, deleted_at, row_active, kind FROM profile_addresses"
    bronze_membership_customer_addresses = DataflowTemplateOperator(
        task_id='bronze_membership_customer_addresses',
        template='gs://jhsf_stage/template/jdbc_to_bigquery_dataflow',
        parameters={
            "sql_query":           query_membership_customer_addresses,
            "dataset_table_name":  "bronze.customer_addresses_membershipid",
            "project_name":        "cdp-jhsf-prod",
            "temp_location":"gs://jhsf_stage/temp_location/",
            "data_insercao": '{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(hours=3))).strftime("%Y-%m-%d %H:%M:00") }}'
        },
        gcp_conn_id='google_cloud_default',
    )

    membershipid_silver_customers = BigQueryOperator(
        task_id='membershipid_silver_customers',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_customers.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )  

    membershipid_silver_customers_wallets = BigQueryOperator(
        task_id='membershipid_silver_customers_wallets',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_customer_wallets.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )  

    membershipid_silver_stores = BigQueryOperator(
        task_id='membershipid_silver_stores',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_stores.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )   

    membershipid_silver_transactions = BigQueryOperator(
        task_id='membershipid_silver_transactions',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_transactions.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )      

    membershipid_silver_wallet_execution = BigQueryOperator(
        task_id='membershipid_silver_wallet_execution',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_wallet_execution.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    ) 

    membershipid_silver_macro_groups = BigQueryOperator(
        task_id='membershipid_silver_macro_groups',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_macro_groups.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    ) 

    membershipid_silver_stores_sub_store_macro_group = BigQueryOperator(
        task_id='membershipid_silver_stores_sub_store_macro_group',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_stores_sub_store_macro_group.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    ) 

    membershipid_silver_stores_sub_store = BigQueryOperator(
        task_id='membershipid_silver_stores_sub_store',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_stores_sub_store.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    ) 

    membershipid_silver_customer_addresses = BigQueryOperator(
        task_id='membershipid_silver_customer_addresses',
        sql=read_query(QUERIES_PATH, 'membershipid/silver_customer_adresses.sql'),
        use_legacy_sql=False,
        gcp_conn_id='bigquery_default'
    )

    membershipid_cdp_profile = PythonOperator(
        task_id='membershipid_cdp_profile',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           '/membershipid/customers_cdp.sql',
           'export_membershipid_profile.csv',
           'membershipid_customers_v2',
           'membershipid_customers_v2',
       ],
    )

    membershipid_cdp_transactions = PythonOperator(
        task_id='membershipid_cdp_transactions',
        provide_context=True,
        python_callable=execute_cdp_integration,
        op_args=[
           QUERIES_PATH,
           '/membershipid/wallets_executions_cdp.sql',
           'export_membershipid_transactions.csv',
           'membershipid_transactions',
           'membershipid_transactions',
       ],
    )
    
    bronze_membership_transactions >> membershipid_silver_transactions
    bronze_membership_customer_addresses >> membershipid_silver_customer_addresses
    bronze_membership_customers >> membershipid_silver_customers
    bronze_membership_customers_wallets >> membershipid_silver_customers_wallets
    bronze_membership_macro_groups >> membershipid_silver_macro_groups
    bronze_membership_stores >> membershipid_silver_stores
    bronze_membership_stores_sub_store >> membershipid_silver_stores_sub_store
    bronze_membership_stores_sub_store_macro_group >> membershipid_silver_stores_sub_store_macro_group
    bronze_membership_wallet_executions >> membershipid_silver_wallet_execution



    [membershipid_silver_customers,membershipid_silver_customers_wallets,
    membershipid_silver_stores, membershipid_silver_wallet_execution,
    membershipid_silver_transactions, membershipid_silver_stores_sub_store,
    membershipid_silver_stores_sub_store_macro_group, membershipid_silver_macro_groups,
    membershipid_silver_customer_addresses] >> membershipid_cdp_transactions >> membershipid_cdp_profile 
