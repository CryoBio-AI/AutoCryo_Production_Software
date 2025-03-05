# title: MQTT to SQL data transfer
# author: Christopher Evans
# date: November 15 2024
# output: data pushed to sql server
# -----

# This script was made to take the data sent from the Automated Cryocooler Tester and upload it to the engineering sql
# server.

# ----Imports-----------------------------------------------------------------------------------------------------------
import logging as logger
import datetime
import time
import paho.mqtt.client as mqtt
import pymysql
import os
import csv
# ----Imports-----------------------------------------------------------------------------------------------------------

# this is to allow the pi to boot up and connect to the internet before the below code is executed
time.sleep(60)

# configuring the log file
logger.basicConfig(filename=r"/home/pi/Automated_cryo_tester/automated_log.log",
                   level=logger.DEBUG,
                   format="%(asctime)s %(levelname)s, %(message)s",
                   datefmt="%Y-%m-%d %H:%M:%S")

log = logger.getLogger()

log.info("Starting the application")

# MQTT settings
mqtt_broker = "localhost"  # or the IP address of your MQTT broker
mqtt_port = 1883
mqtt_topics = ["DATE TIME", "STALL DATA", "HEAT LOAD DATA", "SN"] # These topics are setup by the publisher (the PLC)

# SQL Server settings
sql_ip = '10.0.0.7'  # IP address of SQL Server
sql_db = 'FusionLab'  # Database name
sql_un = 'fusion_daq'  # SQL Server username
sql_pw = 'jkeciooi839**#)@kljHUI379'  # SQL Server password

# Mating data outputs to topics
date_time = ["DateTime"]
stall_data = ["Stall_Temp", "Stall_Reject_Temp", "Stall_PWG_Temp", "Stall_Ambient_Temp", "Stall_Type_E", "Stall_Vacuum", "Stall_VARS"]
heat_load_data = ["Heat_Cooling_Power", "Heat_Reject_Temp", "Heat_PWG_Temp", "Heat_Ambient_Temp", "Heat_SiD", "Heat_Type_E",
             "Heat_Vacuum", "Heat_VARS"]
sn = ["Cryo_SN", "PWG_SN", "CH_SN", "Stand_SN"]

# Create an SQL Server connection
def create_sql_connection(ip, un, pw, db):
    z = 0
    while z < 5:
        try:
            conn = pymysql.connect(host=ip,
                                   user=un,
                                   password=pw,
                                   database=db,
                                   cursorclass=pymysql.cursors.DictCursor
                                   )
            return conn
        except Exception as e:
            z = z + 1
            log.ERROR("Could not connect to the SQL database.\n", e)
            time.sleep(1)

    return None

# Setting up an empty dictionary to dump the mqtt data into
mqtt_data = {}

# Callback when a message is received from the MQTT broker
def on_message(client, userdata, msg):
    global mqtt_data, y
    
    # Split the data coming in to it individual data points. It is comma-delimited.
    data = msg.payload.decode().replace('"',"").split(",")

    try: 
        # Determine the topic that was sent to the server and build the dictionary
        if msg.topic == "DATE TIME":
            
            # Combining the date and time into one entry replacing the date and time entries with this one entry
            date_time_object = data[0] + " " + data[1]
            date_time_object = datetime.datetime.strptime(date_time_object, "%m/%d/%y %H:%M")
            data = [date_time_object]
            mqtt_data[date_time[0]] = data[0]

        elif msg.topic == "STALL DATA":
        
            # Looping through the data entries with respect to the dictionary keys
            for y in range(0, len(data)):
                mqtt_data[stall_data[y]] = float(data[y])

        elif msg.topic == "HEAT LOAD DATA":
            # Looping through the data entries with respect to the dictionary keys
            for y in range(0, len(data)):
                mqtt_data[heat_load_data[y]] = float(data[y])
                
        elif msg.topic == "SN":
            for y in range(0, len(data)):
                mqtt_data[sn[y]] = str(data[y])

        # Determining if the data is complete and can move foward to querying the sql server
        if len(mqtt_data) == 20:
            cryo_bio_logs(mqtt_data)
            sql_upload(mqtt_data)
            mqtt_data = {}
            
        #print(mqtt_data)
        
    except:
        pass
        log.error(f"Error taking data and putting it in the dictionary: {mqtt_data}")

def cryo_bio_logs(data):
    file_path = r"/home/pi/Automated_cryo_tester/Data_logs_v2.0"
    file_name = "Automated_cryo_test_log.csv"
    files = os.listdir(file_path)
    if file_name not in files:
        with open(file_path+"/"+file_name, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=date_time+stall_data+heat_load_data+sn)
            writer.writeheader()
            writer.writerow(data)
            
    else:
        with open(file_path+"/"+file_name, mode="a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=date_time+stall_data+heat_load_data+sn)
            writer.writerow(data)


def sql_upload(data):
    # Insert message into SQL Server
    try:
        conn = create_sql_connection()

        if conn is None:
            pass
        else:
            with conn.cursor() as cursor:
                # Get the current timestamp
                timestamp = datetime.datetime.now()

                # SQL query to insert data
                columns = ", ".join(data.keys())
                values = ", ".join(['%s']*len(data))
                query = f"INSERT INTO AutoCryoTester ({columns}) VALUES ({values});"
                cursor.execute(query, list(data.values()))

                # Commit the transaction
                conn.commit()
                log.info("Uploaded to the SQL server")

            # Close the connection
            conn.close()

            print(f"Message inserted into SQL Server at {timestamp}")

    except Exception as e:
        print(f"Error inserting message into SQL Server: {e}")
        log.error(f"Error taking data and dumping it in SQL server: {mqtt_data}")
        # Empties the dictionary to remove all the old data
        mqtt = {}

x = 0 
while x==0:
    try:
        # Set up MQTT client
        client = mqtt.Client(clean_session=True)

        # Define the callback for receiving messages
        client.on_message = on_message

        # Connect to MQTT broker
        client.connect(mqtt_broker, mqtt_port, 60)
        log.info("Connected to MQTT")
        x=1
        
    except Exception as e:
        print(f"Connection failed: {e}. Retrying in 1 second")
        log.error("SQL Upload Execption", exc_info=True)
        time.sleep(1)

# Subscribe to the MQTT topic
for topic in mqtt_topics:
    client.subscribe(topic)
    

# Loop to process messages
try:
    print(f"Subscribed to topic '{mqtt_topics}' and waiting for messages...")
    client.loop_forever()  # This will keep the script running and listening for messages
except KeyboardInterrupt:
    print("Script terminated by user.")
    
