from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# on défini les arguments par défaut
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 1, 3, 10, 0),
    'retries': 1,
}

def get_data():
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] =  f"{location['street']['number']} {location['street']['name']}" \
                        f", {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    import json
    import time
    import logging
    from kafka import KafkaProducer
    
    # on initialise le producer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                                value_serializer=lambda res: json.dumps(res).encode('utf-8'))
    curr_time = time.time()
    
    # on stream les données pendant 1 minute
    while True:
        # on arrête le streaming après 1 minute
        if time.time() > curr_time + 60:
            break
        # on stream les données
        try:
            res = get_data()
            data = format_data(res)
            producer.send('users_created', data)
        except Exception as e:
            logging.error(f"Error while streaming data: {e}")
            continue


# on défini le DAG
with DAG('user_automation',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:
    
    # Tâche pour démarrer le streaming
    start_streaming_task = PythonOperator(
        task_id='start_streaming-data_from_api',
        python_callable=stream_data
    )
