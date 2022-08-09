from time import sleep
from kafka import KafkaConsumer
import mysql.connector
import json
import sys

def main():

    # con = mysql.connector.connect(user='team4', password='team4pswd', host='team4DB', database='names')
    # print("Database opened successfully")

    # cur = con.cursor()

    # # Creates tables
    # cur.execute('''CREATE TABLE ROVER_NAMES
    #     (NAME VARCHAR(50) PRIMARY KEY NOT NULL,
    #     UID VARCHAR(50) UNIQUE NOT NULL);''')

    # cur.execute('''CREATE TABLE OUTPOST_NAMES
    #     (NAME VARCHAR(50) PRIMARY KEY NOT NULL,
    #     UID VARCHAR(50) UNIQUE NOT NULL);''')

    # cur.execute('''CREATE TABLE CHECKPOINT_NAMES
    #     (NAME VARCHAR(50) PRIMARY KEY NOT NULL,
    #     UID VARCHAR(50) UNIQUE NOT NULL);''')

    # print("Tables created successfully")

    # Listens to user input
    while True:
        try:
            choice = input('Please select option:\n[1] To add new name or rename existing rover\n[2] To add new name or rename existing outpost\n[3] To add new name or rename existing checkpoint\n')
            if choice == '1':
                naming_rover(None)
        except:
            # cur.commit()
            # cur.close()
            sys.exit(0)


    cur.execute("INSERT INTO STUDENT (ADMISSION,NAME,AGE,COURSE,DEPARTMENT) VALUES (3420, 'John', 18, 'Computer Science', 'ICT')");


def naming_rover(cur):
    consumer = KafkaConsumer('rover-metrics', bootstrap_servers='rover-cluster-kafka-bootstrap:9092')
    for msg in consumer:
        decoded = msg.value.decode('utf-8')
        data = json.loads(decoded)
        print(data['driverId'])    
    

naming_rover(None)