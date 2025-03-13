import pika
import socket
import time

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq1'
QUEUE_NAME = 'position_updates'

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
                
                response = "HTTP/1.1 200 OK\r\n" \
                           "Content-Type: text/plain\r\n" \
                           "Content-Length: 13\r\n" \
                           "\r\n" \
                           "Hello, World!"
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