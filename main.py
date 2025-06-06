import pika
import socket
import time
import json
import os
import sys
import random
from random import randrange
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import numpy as np
import hashlib
from threading import Lock

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'  # Connect via Envoy sidecar
RABBITMQ_PORT = 9093           # Envoy upstream port
QUEUE_NAME = 'position_updates'
NAME = os.environ.get('NAME')
DEGRADATION_RATE = float(os.environ.get('DEGRADATION_RATE', '0'))
CRASH_RATE = float(os.environ.get('CRASH_RATE', '0'))
RESTART_TIME = float(os.environ.get('RESTART_TIME', '0'))
GET_TIME = 2
DEGRADED_STATE = False
DEGRADED_START_TIME = None
DEGRADED_DURATION = None
COLLECTOR_QUEUE = 'evac_info_queue'

channel_lock = Lock()

salt = str(time.time())[-4:]
seed = int(hashlib.sha256((socket.gethostname() + salt).encode()).hexdigest(), 16) % (2**32)
rng = np.random.default_rng(seed)

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
            channel.queue_declare(queue=COLLECTOR_QUEUE, durable=True)
            return connection, channel
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker):
            print("RabbitMQ not available, retrying in 5 seconds...")
            time.sleep(5)

def handle_post(client_socket, request):
    global channel_lock
    try:
        with client_socket:

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
            with channel_lock:
                print(f"DEBUG: Posting message with id {json_data.get('id')} to exchange...")
                channel.basic_publish(exchange='notifications', routing_key='', body=json.dumps(json_data),
                    properties=pika.BasicProperties(delivery_mode=2))  # Make message persistent
            
    except Exception as e:
        print(e)

def handle_get(client_socket):
    global DEGRADED_STATE, GET_TIME
    with client_socket:
        before = time.time()
        if DEGRADED_STATE:
            time.sleep(GET_TIME)
        else:
            time.sleep(0.1)
        print(f"DEBUG: Handling GET request took {time.time()-before} seconds...")
        client_socket.sendall("HTTP/1.1 200 OK\r\n".encode('utf-8'))
        return
    
def send_state(channel, message):
    channel.basic_publish(
        exchange='',  # No exchange, direct to queue
        routing_key=COLLECTOR_QUEUE,  # Directly to the collector queue
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make the message persistent
        )
    )


def start_server(host='0.0.0.0', port=9092):
    global DEGRADED_STATE, DEGRADATION_RATE, DEGRADED_START_TIME, DEGRADED_DURATION
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(10)
    print(f"Listening on {host}:{port}...")

    with ThreadPoolExecutor(max_workers=20) as executor:
        global DEGRADED_STATE, DEGRADATION_RATE
        while True:
            client_socket, client_address = server_socket.accept()
            request = client_socket.recv(1024).decode('utf-8')

            if not request: # Hearth beat
                continue

            # CRASH
            rand_val = rng.random()
            if (rand_val < CRASH_RATE):
                try:
                    print("DEBUG: Chrashing...")
                    data = {
                        "name": NAME,
                        "state": "Down",
                        "time_sent": datetime.now().isoformat()
                    }
                    send_state(channel, json.dumps(data))
                    print(f"DEBUG: Random value for crash = {rand_val}")
                    exit(1)
                except subprocess.CalledProcessError as e:
                    print(f"Failed to restart containers: {e}")
                
            # DEGRADATION
            if (rng.random() < DEGRADATION_RATE and not DEGRADED_STATE):
                DEGRADED_START_TIME = time.time()
                DEGRADED_STATE = True
                DEGRADED_DURATION = randrange(10, 30)

                data = {
                    "name": NAME,
                    "state": "Degraded",
                    "time_sent": datetime.now().isoformat()
                }
                send_state(channel, json.dumps(data))
                print("DEBUG: Entering degraded state...")

            if DEGRADED_STATE and time.time() - DEGRADED_START_TIME >= DEGRADED_DURATION:
                DEGRADED_STATE = False

                data = {
                    "name": NAME,
                    "state": "Normal",
                    "time_sent": datetime.now().isoformat()
                }
                send_state(channel, json.dumps(data))
                print("DEBUG: Exiting degraded state...")
            
                

            method = request.splitlines()[0].split()[0]
            if method == "GET":
                executor.submit(handle_get, client_socket)
            else:
                executor.submit(handle_post, client_socket, request)

                    


print("Starting with settings...")
print(f"Chance of crashing:{CRASH_RATE*100}%")
print(f"Chance of degrading:{DEGRADATION_RATE*100}%")

time.sleep(RESTART_TIME)
subprocess.run(["docker", "restart", f"{NAME[:-1]}-proxy{NAME[-1]}"], check=True)


while True:
    
    try:
        # Attempt to connect to RabbitMQ
        connection, channel = connect_to_rabbitmq()

        data = {
            "name": NAME,
            "state": "Up",
            "time_sent": datetime.now().isoformat()
        }
        send_state(channel, json.dumps(data))

        # Start listening for HTTP Requests
        start_server()
        connection.close()

    except Exception as e:
        print(e)