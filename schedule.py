import os
import sys
import pandas as pd
import time
from pathlib import Path

def time_for_tasks(df):
	comp_time = 0
	# get the unique set of processes/tasks for the product
	tasks = df['Part of DWLG (Standard 7 port)'].unique()
	# calculate total time for each task
	for x in tasks:
		steps = df.loc[df['Part of DWLG (Standard 7 port)'] == x]
		totaltime = 0
		persons = []
		for index, row in steps.iterrows():
			totaltime += (int(row['Set up time']) + int(row['Process time']))
			if row['Labor associated'] not in persons:
				persons.append(row['Labor associated'])
		print(x,totaltime,'mins',persons)
		comp_time += totaltime
	print('completion time:',int(comp_time/(60)),'hours')

def time_per_person(df):
	# get the set of personnel
	persons = df['Labor associated'].unique()
	for x in persons:
		steps = df.loc[df['Labor associated'] == x]
		totaltime = 0
		tasks = []
		for index, row in steps.iterrows():
			totaltime += (int(row['Set up time']) + int(row['Process time']))
			task = row['Part of DWLG (Standard 7 port)'] + '-' + row['Name of Process']
			if task not in tasks:
				tasks.append(task)
		print(x,totaltime,'mins',len(tasks),'tasks','avg. time on task',int(totaltime/len(tasks))) 

if __name__ == '__main__':
	
	start = time.time()
	print('Execution started...')
	# read data into dataframe
	data = Path(Path(os.getcwd()) / 'data-dlg.csv')
	df = pd.read_csv(data)
	print('-------TASKS--------')
	time_for_tasks(df)
	print('--------LABOR--------')
	time_per_person(df)
	end = time.time()
	print('Execution finished in',str(round(end-start,2)),'secs')