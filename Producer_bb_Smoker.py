"""
The project is to create a producer that reads sensor data from the CSV file 
The CSV file contains temperature readings for a smoker and two foods (Food A and Food B).

Modified By : Sowdamini Nandigama
Original Source : Denise Case
Date: 09/20/2023
"""

import pika
import sys
import webbrowser
import csv
import socket
import time

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_temp(host: str, queue_name1: str, queue_name2: str, queue_name3: str, message: str):
    """
    Creates and sends a message to 3 queues each execution.

    """
    host = "localhost"
    port = 9999
    address = (host, port)

    # use the socket constructor to create a socket object we'll call sock
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 

     # read from a file to get smoker data
    input_file = open("smoker-temps.csv", "r")

     # create a csv reader 
    reader = csv.reader(input_file, delimiter=",")


    for row in reader:
        # read a row from the file
        Time, Channel1, Channel2, Channel3 = row

        try:
            # create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))
            # use the connection to create a communication channel
            ch = conn.channel()
            ch.queue_declare(queue=queue_name1, durable=True)
            ch.queue_declare(queue=queue_name2, durable=True)
            ch.queue_declare(queue=queue_name3, durable=True)
        

            try:
                Smoker = round(float(Channel1),1)
                # create a message from our data
                smoker = f"{Time}, {Smoker}"
                # prepare a binary (1s and 0s) message to stream
                MESSAGE = smoker.encode()
                # use the socket sendto() method to send the message
                sock.sendto(MESSAGE, address)
                ch.basic_publish(exchange="", routing_key=queue_name1, body=MESSAGE)
                # print a message to the console for the user
                print(f" [x] Sent {MESSAGE} from Smoker")
            except ValueError:
                pass
            
            try:
                FoodA = round(float(Channel2),1)
                # use an fstring to create a message from our data
                Food_A = f"{Time}, {FoodA}"
                # prepare a binary (1s and 0s) message to stream
                MESSAGE1 = Food_A.encode()
                # use the socket sendto() method to send the message
                sock.sendto(MESSAGE1, address)
                ch.basic_publish(exchange="", routing_key=queue_name2, body=MESSAGE1)
                # print a message to the console for the user
                print(f" [x] Sent {MESSAGE1} from FoodA")
            except ValueError:
                pass

            try:
                FoodB = round(float(Channel3),1)
                # use an fstring to create a message from our data
                # notice the f before the opening quote for our string?
                Food_B = f"{Time}, {FoodB}"
                # prepare a binary (1s and 0s) message to stream
                MESSAGE2 = Food_B.encode()
                # use the socket sendto() method to send the message
                sock.sendto(MESSAGE2, address)
                ch.basic_publish(exchange="", routing_key=queue_name3, body=MESSAGE2)
                # print a message to the console for the user
                print(f" [x] Sent {MESSAGE2} from FoodB")
            except ValueError:
                pass



        except pika.exceptions.AMQPConnectionError as e:
                print(f"Error: Connection to RabbitMQ server failed: {e}")
                sys.exit(1)

        finally:
            # close the connection to the server
            conn.close()

if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    message = " ".join(sys.argv[1:]) or '{MESSAGE}'
    # send the message to the queue
    send_temp("localhost","smoker", "FoodA", "FoodB", message)