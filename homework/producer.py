#https://pypi.org/project/websocket_client/
import websocket
from kafka import KafkaProducer
import traceback
from json import dumps

def on_message(ws, message):
    try :
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x:
                                    dumps(x).encode('utf-8'))
        producer.send('finnhub',value=message)
    except Exception as e:
        print(traceback.format_exc())
    
def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=cip8vvhr01qrdahjvag0cip8vvhr01qrdahjvagg",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    
    