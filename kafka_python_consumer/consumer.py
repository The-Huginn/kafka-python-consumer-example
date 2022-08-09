from time import sleep
from kafka import KafkaConsumer
import mysql.connector

consumer = KafkaConsumer('rover-metrics', bootstrap_servers='rover-cluster-kafka-bootstrap:9092')

# for msg in consumer:
#     print (msg)
#     sleep(1)

con = mysql.connector.connect(user='team4', password='team4pswd', host='team4DB', database='names')
print("Database opened successfully")

# cur = con.cursor()
# cur.execute('''CREATE TABLE STUDENT
#       (ADMISSION INT PRIMARY KEY     NOT NULL,
#       NAME           TEXT    NOT NULL,
#       AGE            INT     NOT NULL,
#       COURSE        CHAR(50),
#       DEPARTMENT        CHAR(50));''')
# print("Table created successfully")

# cur.execute("INSERT INTO STUDENT (ADMISSION,NAME,AGE,COURSE,DEPARTMENT) VALUES (3420, 'John', 18, 'Computer Science', 'ICT')");

# con.commit()
con.close()