#Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable



#Absolute imports
from datetime import datetime, timedelta
import os
import redshift_connector
import smtplib

#Custom imports
import coderhouse.etl_modules.connect_openaq_api as connect_api
from coderhouse.etl_modules.data_etl import DataETL

default_args={
    'owner': 'ericsig',
    'depends_on_past': True,
    'email': ['ericsig@gmail.com'],
    'email_on_retry': False,
    'email_on_failure': True,
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}


def _create_redshift_table():
    conn = redshift_connector.connect(
        host=  Variable.get('redshift_host'),
        database= Variable.get('redshift_db'), 
        user=Variable.get('redshift_db_user'), 
        password=Variable.get('secret_redshift_db_pass')
    )
    cursor: redshift_connector.Cursor = conn.cursor()
    cursor.execute("create table if not exists locations(id INT PRIMARY KEY, city VARCHAR, name VARCHAR, entity VARCHAR, country VARCHAR, sources VARCHAR, isMobile BOOLEAN, isAnalysis BOOLEAN, parameters VARCHAR, sensorType VARCHAR, lastUpdated TIMESTAMP, firstUpdated TIMESTAMP, measurements VARCHAR,  bounds VARCHAR, manufacturers VARCHAR,  coordinates_latitude FLOAT,coordinates_longitude FLOAT);")

def _connect_openaq_api(ti):
    country_list = connect_api.main()
    ti.xcom_push(key='country_list',value=country_list)



def _etl_openaq_data(ti):
    country_list = ti.xcom_pull(key='country_list', task_ids='connect_openaq_api')
    api_url = "https://api.openaq.org/v2/countries?limit=200&offset=0&sort=asc"
    
    headers = {"Accept": "application/json", "X-API-Key": Variable.get('apikey_openaq')}
    db_user = Variable.get('redshift_db_user')
    db_pass = Variable.get('secret_redshift_db_pass')
    db_host = Variable.get('redshift_host')
    
    #Parameter and limits
    low_threshold = Variable.get('low_threshold')
    high_threshold = Variable.get('high_threshold')
    filter_parameter = Variable.get('filter_parameter')

    data_etl = DataETL(api_url, headers, db_user, db_pass, db_host, country_list, low_threshold, high_threshold, filter_parameter)
    data_etl.extract_data()
    data_etl.transform_data()
    data_etl.load_data()

    if data_etl.anomalies:
        ti.xcom_push(key='anomalies', value=data_etl.anomalies)
    else:
        ti.xcom_push(key='anomalies', value='No anomalies found')

def _send_alert(ti):
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication
    try:
        anomaly= ti.xcom_pull(key='anomalies', task_ids='etl_openaq_data')
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('ericsig@gmail.com', Variable.get('smtp_password'))
        if anomaly== f'No {Variable.get("filter_parameter")} data':
            subject = 'No data about the parameter selected'
            body_text = f'The Open AQ data does not contain data about the parameter selected ({Variable.get("filter_parameter")}) on any location'
        elif anomaly== f'No anomalies in {Variable.get("filter_parameter")}':
            return
        else:
            subject = 'Open AQ Parameter Anomaly'
            body_text = f'An anomaly in the Open AQ parameter {Variable.get("filter_parameter")} has been detected\n'
            for anomaly in anomaly:
                body_text += f'Date: {anomaly["lastUpdated"]},ID: {anomaly["id"]}, Location: {anomaly["name"]}, {Variable.get("filter_parameter")} Measurement: {anomaly[Variable.get("filter_parameter")]}\n'

        msg = MIMEMultipart()
        msg['From'] = 'ericsig@gmail.com'
        msg['To'] = 'ericsig@gmail.com'
        msg['Subject'] = subject

        msg.attach(MIMEText(body_text, 'plain'))

        x.sendmail('ericsig@gmail.com', 'ericsig@gmail.com', msg.as_string())
        print('Success')
    except Exception as exception:
        print('Failure')
        raise
  

etl_dag = DAG(
    default_args=default_args,
    dag_id='OpenAQ_etl',
    description= 'Extract, Transform and Load data from OpenAQ API to Redshift DB',
    start_date=datetime(2023,10,16),
    schedule_interval='@daily',
    template_searchpath='/tmp',
    tags=['ETL', 'OpenAQ', 'environment'],
    catchup=True
)

create_table = PythonOperator(
        task_id='create_redshift_table',
        python_callable=_create_redshift_table,
        dag=etl_dag
    )

connect_to_api = PythonOperator(
    task_id='connect_openaq_api',
    python_callable=_connect_openaq_api,
    dag=etl_dag
)

etl_airquality_data = PythonOperator(
    task_id='etl_openaq_data',
    python_callable=_etl_openaq_data,
    dag=etl_dag
)

send_alert = PythonOperator(
    task_id='send_parameters_alert',
    python_callable=_send_alert,
    dag=etl_dag
)

[create_table, connect_to_api] >> etl_airquality_data >> send_alert