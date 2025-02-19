from time import sleep
from kafka import KafkaConsumer
import mysql.connector
import json
import sys

def create_table(cur, name):
    cur.execute("SHOW TABLES LIKE %s;", (name, ))
    if not cur.fetchone():
        cur.execute('''CREATE TABLE %s
            (NAME VARCHAR(50) PRIMARY KEY NOT NULL,
            UID VARCHAR(50) UNIQUE NOT NULL);''', (name, ))

def main():

    con = mysql.connector.connect(user='team4', password='team4pswd', host='mysql', database='names')
    print("Database opened successfully")

    cur = con.cursor()

    create_table(cur, 'ROVER_NAMES')
    create_table(cur, 'OUTPOST_NAMES')
    create_table(cur, 'CHECKPOINT_NAMES')
    
    print("Tables created successfully")

    # Listens to user input
    while True:
        try:
            choice = input('Please select option:\n[1] To add new name or rename existing rover\n[2] To add new name or rename existing outpost\n[3] To add new name or rename existing checkpoint\n')
            if choice == '1':
                naming_rover(cur)

            con.commit()
        except:
            cur.close()
            con.commit()
            sys.exit(0)

def naming_rover(cur):
    # Fetch all rovers
    consumer = KafkaConsumer('rover-metrics', bootstrap_servers='rover-cluster-kafka-bootstrap:9092')
    index = 0

    name_dict = {}
    for msg in consumer:
        decoded = msg.value.decode('utf-8')
        data = json.loads(decoded)
        name_dict[index] = data['driverId']

        # Find existing name
        cur.execute('''SELECT *
            FROM ROVER_NAMES
            WHERE UID=%s;
            ''', name_dict[index])
        current_name = cur.fetchone()
        if current_name:
            current_name = ' - ' + current_name[0]
        else:
            current_name = ''

        print(str(index) + ' : ' +name_dict[index] + current_name)
        index+=1
    
    option = input('Select')
    
    if not option.strip().isdigit() or option not in range(0, index):
        print('Invalid index')
        return

    new_name = input('Insert new name for rover ID: ' + name_dict[index])

    # Check for existing entry of name
    cur.execute('''SELECT *
            FROM ROVER_NAMES
            WHERE NAME=%s;
            ''', new_name)

    # Non-empty response
    if cur.fetchall():
        print('This name is already in use')
        return

    # Inserts new name or updates the old one
    cur.execute('''INSERT INTO ROVER_NAMES
            (NAME, UID)
            VALUES(%s, %s)
            ON DUPLICATE KEY UPDATE
            UID=%s;
            ''', new_name, name_dict[index], name_dict[index])

    return
    

main()