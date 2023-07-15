from kafka import KafkaConsumer
from json import loads
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = 'mongodb+srv://farhansmg:qwerty1234@cluster-digitalskola.qvffazt.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print('Pinged your development. You successfully connected to MongoDB')
except Exception as e:
    print(e)

consumer = KafkaConsumer(
    'finnhub',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    group_id='consumer-id-1'
)

mydb = client["finnhub"]

mycol = mydb["tommy"]

while True :
    for message in consumer:
        message_raw = message.value
        msg = loads(message_raw)
        if msg['type'] == 'trade' :
            for res in msg['data'] :
                mycol.insert_one(res)