# Create a sqlite database of all devices

import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
import sqlite_writer

timestamp_format = '%m/%d/%Y %I:%M:%S %p'

class DeviceRecord():

	def __init__(self):
		self.id = None
		self.num_trips = 0
		self.total_duration = 0
		self.earliest_ride = None
		self.latest_ride = None
		self.lifespan = None
		self.total_distance = 0
		self.revenue = 0

	def record_trip(self, trip):
		self.id = trip['Device ID']
		self.num_trips += 1
		self.total_duration += trip['Trip Duration']
		self.total_distance += trip['Trip Distance']
		global timestamp_format
		start_time = datetime.strptime(trip['Start Time'], timestamp_format)
		if self.latest_ride == None or start_time > self.latest_ride:
			self.latest_ride = start_time
		if self.earliest_ride == None or start_time < self.earliest_ride:
			self.earliest_ride = start_time

	def finalize(self):
		self.lifespan = (self.latest_ride - self.earliest_ride).days;
		self.total_duration = self.total_duration // 60
		self.revenue = self.total_duration * 15 + self.num_trips * 100

	def get_tuple(self):
		self.finalize()
		return (
			self.id,
			self.num_trips,
			self.total_duration,
			self.earliest_ride.isoformat(),
			self.latest_ride.isoformat(),
			self.lifespan,
			self.total_distance,
			self.revenue
		)




def main():

	sqlite_writer.init_db('data/scooters.sqlite', 'data/scooters_schema.sql', 'scooters', 8)
	
	devices = defaultdict(DeviceRecord)

	chunksize = 10 ** 6
	count = 0
	for chunk in pd.read_csv("data/Dockless_Vehicle_Trips.csv", chunksize=chunksize):
	# for chunk in pd.read_csv("data/1000_trips.csv", chunksize=chunksize):
		for index, row in chunk.iterrows():
			count += 1
			if type(row['Start Time']) == str and row['Vehicle Type'] == 'scooter':
				devices[row['Device ID']].record_trip(row)
				if count % 1000 == 0:
					print('Record #: %d' % (count))

	for device in devices:
		sqlite_writer.append_row(devices[device].get_tuple())

	sqlite_writer.commit_changes()
			

	


main()