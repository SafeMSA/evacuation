import pika
import time

time.sleep(30)
connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq1"))
channel = connection.channel()
channel.queue_declare(queue='hello')
channel.basic_publish(exchange='', routing_key='hello', body='Hello World!')
connection.close()