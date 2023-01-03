from kafka import  KafkaProducer
from json import dumps
import csv

topicname='retailupdate'
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda x:dumps(x).encode('utf-8'))

with open("C:\kafka\Retail_result_op.csv",'r') as file:
 reader = csv.reader(file,delimiter='\t')
 for messages in reader:
  producer.send(topicname,messages)
  producer.flush()
