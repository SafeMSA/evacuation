import pika
import socket
import time
import json
import os

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'  # Connect via Envoy sidecar
RABBITMQ_PORT = 9093           # Envoy upstream port
QUEUE_NAME = 'position_updates'
NAME = os.environ.get('NAME')

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
            print("RabbitMQ not available, retrying in 1 seconds...")
            time.sleep(1)

def start_server(host='0.0.0.0', port=9092):
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Listening on {host}:{port}...")
        
        while True:
            try:
                client_socket, client_address = server_socket.accept()
                with client_socket:
                    print(f"Connection from {client_address}")
                    request = client_socket.recv(1024).decode('utf-8')

                    # Try to parse the incoming request as JSON
                    if (len(request) > 10):
                        # Build JSON response
                        response_data = {
                            "name": NAME,
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

                        # Extract JSON body from HTTP request
                        headers, body = request.split("\r\n\r\n", 1)

                        # Validate JSON
                        json_data = json.loads(body)

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

# Attempt to connect to RabbitMQ
#connection, channel = connect_to_rabbitmq()

#def callback(ch, method, properties, body):
#    print(f"Received: {body}")

#channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

#print('Waiting for messages...')
#channel.start_consuming()
#print('Closing...')