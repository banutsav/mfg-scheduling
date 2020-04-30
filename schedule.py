import os
import sys
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
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

# construct list with each task and succeeding tasks
def get_successors(df):
	# extract just the task information
	tasks = df[['Task ID', 'Set up time', 'Process time', 'Predecessor']]
	tasks['Total Time'] = tasks['Set up time'] + tasks['Process time']
	tasks['Done'] = False
	#print(tasks.head())
	# last task ID
	last = len(tasks)
	# task : succesor list init
	successors = []
	# generate sequence of task starts
	for i in range(last):
		# predecessor step as a string
		pre = str(i)
		# check for no steps found in a phase
		flag = False
		# find tasks in that phase
		steps = []
		for index, row in tasks.iterrows():
			if pre in str(row['Predecessor']):
				steps.append(row['Task ID'])
				flag = True
		# steps found
		if flag:
			# construct obj = {task id = steps which can start when that task completes}
			obj = {'task': i, 'succesors': steps}
			successors.append(obj)
	
	return successors

if __name__ == '__main__':
	
	start = time.time()
	print('Execution started...')
	# read data into dataframe
	df = pd.read_excel('data/dlg-base-data.xlsx')
	# sequence of tasks
	successors = get_successors(df)
	print(successors)
	end = time.time()
	print('Execution finished in',str(round(end-start,2)),'secs')