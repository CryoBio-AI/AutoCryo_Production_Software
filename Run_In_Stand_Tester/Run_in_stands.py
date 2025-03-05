# title: PLC to SQL server data transfer
# author: Christopher Evans
# date: February 21 2025
# output: data collected from the PLC to the pi and passed to the SQL server
# -----

# This script was made to take data from each run in stand an put it in its
# respective SQL table and local storage file.

#-Imports---------------------------------------------------------------------------------------------------------------
from pylogix import PLC
import logging
import csv
import numpy as np
import time
import pymysql
import datetime
from os import listdir
from os.path import join
import time
#-Imports---------------------------------------------------------------------------------------------------------------
# Setting up the logger and beginning to log
logging.basicConfig(filename=r"/home/pi/Run_in_stands/log/Run_in_stand.log",
				   level=logging.DEBUG,
					format="%(asctime)s %(levelname)s, %(message)s",
					datefmt="%Y-%m-%d %H:%M:%S")

log = logging.getLogger()

log.info("Starting the application")

# Stating all tags that will provide data to the program
tags = ['Current_Date_Time_Stamp[0]', 'Current_Date_Time_Stamp[1]', 'Current_Date_Time_Stamp[2]', 'Current_Date_Time_Stamp[3]', 'Current_Date_Time_Stamp[4]',
		'STATION_1_BC_DATA', 'Local:6:I.Ch01.Data', 'Local:6:I.Ch00.Data', 'Local:6:I.Ch02.Data', 'Local:4:I.Ch04.Data', 'Local:3:I.Ch00.Data', 'Local:3:I.Ch06.Data', 'STATION1_GHOUR',
		'STATION_2_BC_DATA', 'Local:7:I.Ch00.Data', 'Local:6:I.Ch03.Data', 'Local:7:I.Ch01.Data', 'Local:5:I.Ch07.Data', 'Local:3:I.Ch01.Data', 'Local:3:I.Ch07.Data', 'STATION2_GHOUR',
		'STATION_3_BC_DATA', 'Local:7:I.Ch03.Data', 'Local:7:I.Ch02.Data', 'Local:8:I.Ch00.Data', 'Local:4:I.Ch06.Data', 'Local:3:I.Ch02.Data', 'Local:4:I.Ch00.Data', 'STATION3_GHOUR',
		'STATION_4_BC_DATA', 'Local:8:I.Ch02.Data', 'Local:8:I.Ch01.Data', 'Local:8:I.Ch03.Data', 'Local:4:I.Ch07.Data', 'Local:3:I.Ch03.Data', 'Local:4:I.Ch01.Data', 'STATION4_GHOUR',
		'STATION_5_BC_DATA', 'Local:9:I.Ch01.Data', 'Local:9:I.Ch00.Data', 'Local:9:I.Ch02.Data', 'Local:5:I.Ch00.Data', 'Local:3:I.Ch04.Data', 'Local:4:I.Ch02.Data', 'STATION5_GHOUR',
		'STATION_6_BC_DATA', 'Local:10:I.Ch00.Data', 'Local:9:I.Ch03.Data', 'Local:10:I.Ch01.Data', 'Local:5:I.Ch01.Data', 'Local:3:I.Ch05.Data', 'Local:4:I.Ch03.Data', 'STATION6_GHOUR']

tables = ["Run_in_stand_1", "Run_in_stand_2", "Run_in_stand_3", "Run_in_stand_4", "Run_in_stand_5" , "Run_in_stand_6"]

columns = ["datetime", "cryo_sn", "pwg_temp", "reject_temp", "sid_temp", "vibration_sens", "voltage", "wattage", "run_time"]

#sql database details to get into the server
sql_ip = "10.0.0.7"
sql_db = "FusionLab"
sql_un = "fusion_daq"
sql_pw = "jkeciooi839**#)@kljHUI379"


# Takes the data and formats it to be saved locally and on the sql server
def data_organize(data):
	items_to_upload = []
	try:
		time_stamp = str(data[0].Value)+"/" + str(data[1].Value) + "/" + str(data[2].Value) + " " + str(data[3].Value) + ":" + str(data[4].Value)
		time_stamp = datetime.datetime.strptime(time_stamp,"%Y/%m/%d %H:%M")
		stand_data = {"Run_in_stand_1": [time_stamp, data[5].Value, data[6].Value, data[7].Value, data[8].Value,
										 data[9].Value, data[10].Value, data[11].Value, data[12].Value],
					  "Run_in_stand_2": [time_stamp, data[13].Value, data[14].Value, data[15].Value, data[16].Value,
										 data[17].Value, data[18].Value, data[19].Value, data[20].Value],
					  "Run_in_stand_3": [time_stamp, data[21].Value, data[22].Value, data[23].Value, data[24].Value,
										 data[25].Value, data[26].Value, data[27].Value, data[28].Value],
					  "Run_in_stand_4": [time_stamp, data[29].Value, data[30].Value, data[31].Value, data[32].Value,
										 data[33].Value, data[34].Value, data[35].Value, data[36].Value],
					  "Run_in_stand_5": [time_stamp, data[37].Value, data[38].Value, data[39].Value, data[40].Value,
										 data[41].Value, data[42].Value, data[43].Value, data[44].Value],
					  "Run_in_stand_6": [time_stamp, data[45].Value, data[46].Value, data[47].Value, data[48].Value,
										 data[49].Value, data[50].Value, data[51].Value, data[52].Value]
					  }

		for table in stand_data.keys():
			running_stand = False
			if stand_data[table][4]==1000 or stand_data[table][-2]<5:
				running_stand = True
			if running_stand is True:
				pass
			else:
				items_to_upload.append(table)
	except Exception as e:
		log.DEBUG("Data unable to be formatted \n", e)

	if len(items_to_upload)==0:
		print("Nothing to upload")

	return stand_data, items_to_upload


# Saves the collect data locally in 6 separate txt files
def local_save(tables, data):
	folder_path = r"/home/pi/Run_in_stands/stand_data_logs"
	try:
		files = listdir(folder_path)
		for table in tables:
			if table+".txt" not in files:
				for value in range(0,len(data[table])):
						if data[table][value] is None:
							data[table][value] = ""
						else:
							data[table][value] = str(data[table][value])
				with open(folder_path+"/"+table+".txt", "w") as file:
					line = ", ".join(map(str, data[table]))
					file.write(line + "\n")
			else:
				with open(folder_path+"/"+table+".txt", "a") as file:
					line = ", ".join(map(str, data[table]))
					file.write(line + "\n")
	except Exception as e:
		log.DEBUG("Data was not logged in its repesective local file \n", e)
		pass

# Takes collected data and uploads it into the sql server
def sql_upload(tables, columns, data, ip, db, un, pw):
	try:
		sql_conn = connect_to_sql(ip=ip, db=db, un=un, pw=pw)

		if sql_conn is None:
			pass
		else:
			with sql_conn.cursor() as cursor:
				for table in tables:
					sql_data = data[table]
					new_sql_data = []
					for value in sql_data:
						if value is None or value =="":
							new_sql_data.append(0)
						else:
							new_sql_data.append(value)

					table_number = str(table).split("_")[-1]
					station_number = "station_" + table_number + "_"
					new_columns = [station_number + column for column in columns]
					timestamp = datetime.datetime.now()
					new_columns = ", ".join(new_columns)
					values = ", ".join(['%s'] * len(new_sql_data))
					query = f"INSERT INTO {table} ({new_columns}) VALUES ({values});"
					cursor.execute(query, list(new_sql_data))
					sql_conn.commit()
					print(f"Data was sucessfully entered into SQL server at {timestamp}")
					#log.info(f"Data was sucessfully entered into SQL server at {timestamp}")

			sql_conn.close()

	except Exception as e:
		log.DEBUG("Program was not able to connect to sql server or upload data \n", e)
		pass


# Connect the raspberry pi to the sql server used to store engineering and manufacturing data
def connect_to_sql(ip, db, un, pw):
	x = 0
	while x < 5:
		try:
			sql_conn = pymysql.connect(host=ip,
									   user=un,
									   password=pw,
									   database=db,
									   cursorclass=pymysql.cursors.DictCursor
									   )

			return sql_conn
		except Exception as e:
			x = x+1
			log.ERROR("Error connecting to the SQL database. ", e)
			time.sleep(1.0)
	return None


# Connecting with the PLC to grab the data
with PLC() as comm:
	comm.ProcessorSlot = 0
	comm.IPAddress = "10.143.1.108"
	while 0==0:
		ret = comm.Read('Current_Date_Time_Stamp[5]')
		time.sleep(1.0)
		if ret.Value == 0:
			comm.IPAddress = '10.143.1.108'
			ret = comm.Read(tags)
			if len(ret)==53:
				stand_data, items_to_upload = data_organize(data=ret)
				local_save(tables=items_to_upload, data=stand_data)
				sql_upload(tables=items_to_upload, columns=columns, data=stand_data, ip=sql_ip, db=sql_db, pw=sql_pw, un=sql_un)
			time.sleep(1.0)
		else:
			pass
