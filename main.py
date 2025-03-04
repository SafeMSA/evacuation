import pika

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq1'
QUEUE_NAME = 'position_updates'

connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

channel.queue_declare(queue=QUEUE_NAME)

def callback(ch, method, properties, body):
    print(f"Received: {body}")

channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

print('Waiting for messages...')
channel.start_consuming()
print('Closing...')