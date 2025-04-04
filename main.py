import pika
import socket
import time
import json
import os
import sys
import random

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'  # Connect via Envoy sidecar
RABBITMQ_PORT = 9093           # Envoy upstream port
QUEUE_NAME = 'position_updates'
NAME = os.environ.get('NAME')
DEGRADATION_RATE = float(os.environ.get('DEGRADATION_RATE', '0'))
CRASH_RATE = float(os.environ.get('CRASH_RATE', '0'))
RESTART_TIME = 120
DEGRADATION_TIME = 10


def connect_to_rabbitmq():
    #Attempts to connect to RabbitMQ, retrying until successful.
    credentials = pika.PlainCredentials('myuser', 'mypassword')
    
    while True:
        
        try:
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST, 
                port=RABBITMQ_PORT,
                credentials=credentials, 
                blocked_connection_timeout=1
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare(exchange='notifications', exchange_type='fanout', durable=True)
            print("Connected to RabbitMQ")
            return connection, channel
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker):
            print("RabbitMQ not available, retrying in 5 seconds...")
            time.sleep(5)

def start_server(host='0.0.0.0', port=9092):
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(100)
        print(f"Listening on {host}:{port}...")
        
        while True:
            try:
                client_socket, client_address = server_socket.accept()
                with client_socket:
                    print(f"Connection from {client_address}")
                    request = client_socket.recv(1024).decode('utf-8')
                    print(request)

                    # Filter out health checks
                    if (len(request) < 10):
                        continue

                    # CRASH
                    if (random.random() < CRASH_RATE):
                        time.sleep(RESTART_TIME)
                        os.execv(sys.executable, ['python'] + sys.argv)
                        
                    # DEGRADATION
                    if (random.random() < DEGRADATION_RATE):
                        time.sleep(DEGRADATION_TIME)

                    # SUCCESS
                    # Extract JSON body from HTTP request
                    headers, body = request.split("\r\n\r\n", 1)

                    # Validate JSON
                    json_data = json.loads(body)

                    response_data = {
                        "id": json_data.get('id'),
                        "name": NAME
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
                    # Publish the message
                    print("Sending out message")
                    channel.basic_publish(exchange='notifications', routing_key='', body=json.dumps(json_data),
                        properties=pika.BasicProperties(delivery_mode=2))  # Make message persistent
            except Exception as e:
                print(e)
                break

                
while True:
    try:
        # Attempt to connect to RabbitMQ
        connection, channel = connect_to_rabbitmq()

        # Start listening for HTTP Requests
        start_server()
        connection.close()

    except Exception as e:
        print(e)