from confluent_kafka import Consumer, KafkaError
import time 
import socket

settings = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'group.id': 'lkc-qd096',
    'client.id': socket.gethostname(),
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'security.protocol':'SASL_SSL', 
    'sasl.mechanism':'PLAIN', 
    'sasl.username':'FL5OOZQ4OQPATRJZ', 
    'sasl.password':'v2Wbjn3yGNDlGyPwIO+2CRoswrIQC3Ej7d16HXwsrbFt02+9gieNsAt+CwUeImUD',
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

c = Consumer(settings)

c.subscribe(['test'])

try:
        while True:
            msg = c.poll()
            print(msg.value().decode('utf-8'))
            if not msg.error():
                Msg = msg.value().decode('utf-8').strip()
                try:
                    tm = time.strftime('%Y%m%d%H%M', time.localtime())
                    if Msg:
                        Msg = Msg.split()
                        if len(Msg) >= 17:
                            internet_access_minute = 'internet_access_minute_%s' % tm
                except Exception as e:
                    print(e)
                    continue
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                continue
finally:
    c.close()