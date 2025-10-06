from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch, helpers
from datetime import timedelta, datetime
import json, logging, os, re
import boto3
from utils import get_secret
logger=logging.getLogger(__name__)
def parse_log_entry(log_entry):
    #log_pattern = r'(?P<ip>[\d.]+) - - \[(?P<timestamp>.*?)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) (?P<Protocol>[\w/\.]+) '
    log_pattern = r'(?P<ip>[\d.]+) - - \[(?P<timestamp>.*?)\] "(?P<method>\w+) (?P<endpoint>[\w/]+) (?P<protocol>[\w/\.]+)" (?P<status>\d{3}) (?P<size>\d+) "(?P<referrer>[^"]+)" "(?P<user_agent>[^"]+)"'
    match = re.match(log_pattern, log_entry)
    if not match: 
        logger.warning(f'Invalid log format:{log_entry}')
        return None
    data=match.groupdict()
    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], '%d/%b/%Y:%H:%M:%S %z')
        data['@timestamp']=parsed_timestamp.isoformat() 
    except ValueError:
        logger.error(f"Timestamp parsing error: {data['timestamp']}")
        return None
    
    return data
    

def consume_index_logs(max_messages=10000, timeout_seconds=100):
    secrets=get_secret('MWAA_Seccrets_V2')
    consumer_config={
        'bootstrap.servers':secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'session.timeout.ms':50000,
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'group.id': 'mwaa_log_indexer',
        'auto.offset.reset': 'latest'
    }
    es_config={
        'hosts': [secrets['ELASTICSEARCH_URL']],
        'api_key':secrets['ELASTICSEARCH_API_KEY']
    }
    consumer=Consumer(consumer_config)
    es=Elasticsearch(**es_config)
    topic='billion_web_logs'
    consumer.subscribe([topic])
    
    try:
        index_name='billion_web_logs'
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logger.info(f'Created index: {index_name}')
    except Exception as e:
        logger.error(f'Failed to create index: {index_name}')
    
    logs=[]
    
    try: 
        messages_processed = 0
        start_time = datetime.now()
        while True:
            if (datetime.now() - start_time).total_seconds() > timeout_seconds:
                break
            msg=consumer.poll(timeout= 1.0) # request to get message
            if msg is None:
                continue 
            if msg.error():
                if msg.error().code()== KafkaException._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())
            
            log_entry=msg.value().decode('utf-8') #message receive 
            
            parsed_log=parse_log_entry(log_entry)
        
            if parsed_log:
                logs.append(parsed_log)
            #index when 15000 logs be collected
            # cứ mỗi 10000 log thì gửi đến es
            if len(logs)==10000:
                actions=[
                    {
                        '_op_type':'create',
                        '_index': index_name,
                        '_source' : log
                    }
                    for log in logs
                ]
                success, failed = helpers.bulk(es, actions, refresh=True)
                logger.info(f'Index {success} logs, {len(failed)} failed')
                logs=[] #reset logs
                
    except Exception as e:
        logger.error(f'Failed to index log: {e}')    
    #index any remaining logs
    try: 
        if logs: 
            actions=[
                    {
                        '_op_type':'create',
                        '_index': index_name,
                        '_source' : log
                    }
                    for log in logs
                ]
            helpers.bulk(es,actions,refresh=True)
    except Exception as e:
        logger.error(f'Log processing error:{e}')
    finally: 
        consumer.close()
        es.close()
    
default_args={
    'owner': 'hodeptrai',
    'email': 'toilahoadeptraine@gmail.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'log_consumer_pipeline',
    default_args= default_args,
    description= 'generate and produce log',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025,1,1),
    catchup= False,
    tags=['log','kafka','production']
) as dag:
    consume_log_task= PythonOperator(
        task_id='generate_and_produce_logs',
        python_callable=consume_index_logs
    )
