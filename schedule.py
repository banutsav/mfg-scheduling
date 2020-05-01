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

# convert a comma separated task set string to a list
def taskset_to_list(tasks):
	tokens = str(tasks).split(',')
	result = []
	for i in range(len(tokens)):
		result.append(int(tokens[i]))
	return result

# construct list with each task and succeeding tasks
def get_successors(df):
	# extract just the task information
	tasks = df[['Task ID', 'Set up time', 'Process time', 'Predecessor']].dropna()
	# task -> succesors mapping init
	successors = {}
	# generate sequence of task starts
	for task_id in df.loc[:, 'Task ID']:
		# check for no steps found in a phase
		flag = False
		# find tasks in that phase
		steps = []
		for index, row in tasks.iterrows():
			# get set of predecessor tasks as a list
			pre = taskset_to_list(row['Predecessor'])
			# check if any other task
			if task_id in pre:
				steps.append(row['Task ID'])
				flag = True
		# steps found
		if flag:
			# construct obj = {task id = steps which can start when that task completes}
			successors[task_id] = steps
	
	# list of starting tasks
	start_tasks = []
	# add the starting tasks
	for index, row in tasks.iterrows():
		# get set of predecessor tasks as a list
		pre = taskset_to_list(row['Predecessor'])
		# check if it is a starting task
		if 0 in pre:
			start_tasks.append(row['Task ID'])
				
	successors[0] = start_tasks
	
	return successors

# check if a task has multiple predecessor tasks
def check_multiple_predecessor(parent, child, successors, df, curr_time):
	# loop through all tasks
	for task in successors:
		# ignore the current parent
		if task == parent:
			continue
		# nothing to do if child not a succesor of task
		if child not in successors[task]:
			continue
		# get end time of task
		end_time = int(df.loc[df['Task ID'] == task, 'End Time'])
		# nothing to do if task not started
		if end_time == -1:
			return False
		# child is a successor of a task which has started
		if end_time > curr_time:
			# that task has yet to end, child cannot start
			return False
	
	# child can start
	return True

# set start and end times for a list of tasks
def set_start_end(df, task_ts_df, end_tasks, successors, start):
	# iterate over the tasks which have ended at this time
	for end_task in end_tasks:
		# print task which has ended, only if not 0
		if end_task != 0:
			#print(end_task, 'ends at min', start - 1)
			# add entry to the time series dataframe object
			task_ts_df = task_ts_df.append({'Task ID': end_task
				, 'Main Task': df.loc[df['Task ID'] == end_task, 'Part of DWLG (Standard 7 port)'].values[0] 
				, 'Sub Task': df.loc[df['Task ID'] == end_task, 'Name of Process'].values[0]
				, 'Status': 'End', 'Minute Timestamp': start - 1}, ignore_index=True)
		
		# check if task has succeeding tasks
		if end_task not in successors:
			continue
		# find the followers of each task that has ended
		followers = successors[end_task]
		# kick off it's successor tasks
		for task in followers: 
			# check if task depends on multiple predecessor
			if check_multiple_predecessor(end_task, task, successors, df, start):
				# start the task
				df.loc[df['Task ID'] == task, 'Start Time'] = start
				df.loc[df['Task ID'] == task, 'End Time'] = start + int(df.loc[df['Task ID'] == task, 'Total Time'])
				#print(task, 'starts at min', start)
				# add entry to the time series dataframe object
				task_ts_df = task_ts_df.append({'Task ID': task
					, 'Main Task': df.loc[df['Task ID'] == task, 'Part of DWLG (Standard 7 port)'].values[0] 
					, 'Sub Task': df.loc[df['Task ID'] == task, 'Name of Process'].values[0]
					, 'Status': 'Start', 'Minute Timestamp': start}, ignore_index=True)
	
	return df, task_ts_df

# generate time series of task starts and ends
def get_task_timeseries(df, successors):
	# init the time series dataframe
	task_ts_df = pd.DataFrame(columns=['Task ID', 'Main Task', 'Sub Task', 'Status', 'Minute Timestamp'])
	# extract task set from data frame
	tasks = df
	# add additional columns
	tasks['Total Time'] = tasks['Set up time'] + tasks['Process time']
	tasks['Start Time'] = -1
	tasks['End Time'] = -1
	# calculate total time
	totaltime = tasks['Total Time'].sum()
	print('Totaltime is', totaltime, 'mins')
	# start the first batch of tasks which have no predecessors
	tasks, task_ts_df = set_start_end(tasks, task_ts_df, [0], successors, 0)
	#print(tasks.head(20))
	# iterate over each minute from start and end
	for mins in range(1, totaltime + 1):
		# see if any task ends at this minute
		end_tasks = tasks.loc[tasks['End Time'] == mins, 'Task ID'].tolist()
		# tasks which end at this minute
		if len(end_tasks) > 0:
			tasks, task_ts_df = set_start_end(tasks, task_ts_df, end_tasks, successors, mins + 1)
	
	return tasks, task_ts_df

if __name__ == '__main__':
	
	start = time.time()
	print('Execution started...')
	# read data into dataframe
	df = pd.read_excel('data/dlg-base-data.xlsx')
	# get successors for each task
	successors = get_successors(df)
	#print(successors)
	# generate timed sequence of tasks
	df, task_ts_df = get_task_timeseries(df, successors)
	# output the time time series dataframe as a CSV
	task_ts_df.to_excel('output/task-timeseries-dlg.xlsx', index=False)
	end = time.time()
	print('Execution finished in',str(round(end-start,2)),'secs')