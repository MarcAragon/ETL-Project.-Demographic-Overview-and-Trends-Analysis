from kafka import KafkaConsumer
import json
from sqlalchemy import create_engine
import pandas as pd

Consumer = KafkaConsumer(
    'APITopic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer= lambda x: json.loads(x.decode('utf-8'))
)

Engine = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/airflow")

print('Waiting for messages')
MessageCounter = 0

for Message in Consumer:

    MessageCounter += 1
    print('Messages received', MessageCounter)
    print(Message.value)

    Data = Message.value
    Data['id'] = MessageCounter

    pd.DataFrame([Message.value]).to_sql(name='infoapi', con=Engine, if_exists='append', index=False)