
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer
from elasticsearch import Elasticsearch, helpers
from datetime import datetime, timedelta
from faker import Faker
import logging, random
from utils import get_secret

fake = Faker()
logger=logging.getLogger(__name__)


def create_produce_kafka(config):
    return Producer(config)

def genarate_log():
    """Generate synthetic log"""
    methods=['GET','POST','PUT','DELETE']
    endpoints=['/api/users','/home','/about','/contact','/services']
    statuses=[200,301,302,400,404,500]
    user_agents=[
        'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)',
        'Mozilla/5.0 (X11; Linux x86_64)',
        'Mozilla 5.0 (Windows NT 10.0; Win64; x64)',
        'Mozilla 5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'ApleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
    ]
    referrers=['https://google.com','https://bing.com','https://twitter.com','https://yahhoo.com','https://www.facebook.com']
    ip = fake.ipv4()
    timestamp= datetime.now().strftime('%b %d %Y, %H:%M:%S')
    method=random.choice(methods) #random.choice(): random value from specific sequence
    endpoint=random.choice(endpoints)
    status=random.choice(statuses)
    size= random.randint(1000, 15000)
    referrer=random.choice(referrers)
    user_agent=random.choice(user_agents)
    log_entry=(
        f'{ip} - - [{timestamp}] "{method} {endpoint} HTTP/1.1" {status} {size} "{referrer}" {user_agent}'
    )
    return log_entry

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
       Triggered by poll() or flush()"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
                    
def produce_logs(**context):
    """produce log retries into kafka"""
    secrets=get_secret('MWAA_Seccrets_V2')
    producer_config={
        'bootstrap.servers':secrets['KAFKA_BOOTSTRAP_SERVER'], #list broker để consumer connect to cluster
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'session.timeout.ms':50000,
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD']
    }
    producer=create_produce_kafka(producer_config)
    topic = 'billion_web_logs'
    
    for _ in range(15000):
        log=genarate_log()
        try: 
            producer.produce(topic,log.encode('utf-8'),on_delivery=delivery_report)
            producer.flush()
        except Exception as e:
            logger.error(f'Error producing log: {e}')
            raise
    logger.info(f'Produced 15,000 logs to topic  {topic}')
default_args={
    'owner': 'hodeptrai',
    'email': 'toilahoadeptraine@gmail.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'log_generation_pipeline',
    default_args= default_args,
    description= 'generate and produce log',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025,1,1),
    catchup= False,
    tags=['log','kafka','production']
) as dag:
    generate_logs= PythonOperator(
        task_id='generate_and_produce_logs',
        python_callable=produce_logs
    )
