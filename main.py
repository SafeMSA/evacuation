import pika
import socket
import time
import json
import os

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq1'
QUEUE_NAME = 'position_updates'
NAME = os.environ.get('NAME')

def connect_to_rabbitmq():
    #Attempts to connect to RabbitMQ, retrying until successful.
    credentials = pika.PlainCredentials('myuser', 'mypassword')
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(QUEUE_NAME, passive=True)
            print("Connected to RabbitMQ")
            return connection, channel
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker):
            print("RabbitMQ not available, retrying in 5 seconds...")
            time.sleep(5)

def start_server(host='0.0.0.0', port=9092):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Listening on {host}:{port}...")
        
        while True:
            client_socket, client_address = server_socket.accept()
            with client_socket:
                print(f"Connection from {client_address}")
                request = client_socket.recv(1024).decode('utf-8')
                print(f"Received request:\n{request}")

                # Build JSON response
                response_data = {
                    "name": NAME,
                    "body": "Hello World",
                    "code": 200
                }
                response_json = json.dumps(response_data, indent=2)

                response = (
                    "HTTP/1.1 200 OK\r\n"
                    "Content-Type: application/json\r\n"
                    f"Content-Length: {len(response_json)}\r\n"
                    "\r\n"
                    f"{response_json}"
                )

                client_socket.sendall(response.encode('utf-8'))


start_server()

# Attempt to connect to RabbitMQ
#connection, channel = connect_to_rabbitmq()

#def callback(ch, method, properties, body):
#    print(f"Received: {body}")

#channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

#print('Waiting for messages...')
#channel.start_consuming()
#print('Closing...')