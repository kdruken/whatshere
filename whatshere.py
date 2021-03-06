#!/usr/bin/env python
'''
'whatshere.py'
K.Druken (kelsey.druken@anu.edu.au)
NCI

Script to quickly find out what file types 
(number and capacity) are found within a 
specified directory. For specific file type, 
use the '--type' option. To output the full 
paths of all found files, use '--log'. 

Usage:
>> python whatshere.py <TOP-DIRECTORY> 
   [--<MB/GB/TB>] [--type <extension>] [--log]


Notes: 
- Only files you have permission to read 
  will be reflected in the totals (messages 
  will be displayed for any files the scan 
  is unable to read)


	
'''


from operator import itemgetter
import multiprocessing as mp
from datetime import datetime
import os, sys
import time


class WhatIsHere:
	def __init__(self):
		self.found = {}

	def add(self, fullpath): 
		try:

			size = os.stat(fullpath)[6]
			name = os.path.basename(fullpath)

			if name.count('.') != 0:
				ext = name.split('.')[-1].lower()
				try:
					i = int(ext)
					ext = '(numeric)'
				except ValueError:
					pass	
			else:
				ext = '(no_extension)'				

			if ext in self.found.keys():
				self.found[ext]['count'] += 1
				self.found[ext]['size'] += size
			else:
				self.found[ext] = {'count': 1, 'size': size}

		except:
			print "Problem reading: ", fullpath
			pass



def explore_path(path, fileType):
	directories = []
	nondirectories = []
	try:
		for filename in os.listdir(path):
			fullname = os.path.join(path, filename)
			if os.path.isdir(fullname):
				directories.append(fullname)
			elif os.path.isfile(fullname):
				if fileType and fullname.endswith('.'+fileType):
					nondirectories.append(fullname)
				elif fileType == None:
					nondirectories.append(fullname)
			else:
				print 'Permission denied or broken symlink: ', fullname
	except OSError:
		print 'Permission denied: ', path
		pass	
	except:
		print 'Unknown error: ', path
		pass

	return directories, nondirectories



def parallel_worker(i, unsearched, found, fileType, log):
	scan = WhatIsHere()
	while True:
		if unsearched.empty() == False:
			path = unsearched.get()
			dirs, files = explore_path(path, fileType)
			for file in files:
				scan.add(file)
				if log:
					print file
			for newdir in dirs:
				unsearched.put(newdir)
			unsearched.task_done()
		else:
			time.sleep(5)
			if unsearched.empty() == True:
				break
	
	found.put(scan.found)
 
def find_files(path, fileType, log):

	print 'Checking under directory: ', path

	m = mp.Manager()
	unsearched = mp.JoinableQueue()
	found = m.Queue()
	unsearched.put(path)

	try:
		np = 7
		jobs = []
		for i in range(np):	
			p = mp.Process(target=parallel_worker, args=(i, unsearched, found, fileType, log))
			jobs.append(p)
		for p in jobs:
			p.start()

		for p in jobs:
			p.join()
		unsearched.join()	

		print 'Getting results...'
		results = [found.get() for p in jobs]
	except:	
		for p in jobs:
			p.terminate()
		sys.exit('Opps something happened- ending processes and exiting...')

	return results
 


def main():
	## What capacity units (default = 'MB') ##
	units = {'KB': 1e3, 'MB': 1e6, 'GB': 1e9, 'TB': 1e12, 'PB': 1e15}

	path = log = fileType = None
	U = 'GB'
	for item in sys.argv:
		if item in ['--help', '-help', '-h', '--h']:
			print __doc__
			sys.exit()
		if os.path.exists(item):
			path = item
		if item.count('type') > 0:
			fileType = sys.argv[sys.argv.index(item) + 1]
		if item.count('log') > 0:
			log = True
		if item.strip('--').upper() in units.keys():
			U = item.strip('--').upper()
	u = units[U]

	print 'Specific file type(s): ', fileType

	results = find_files(path, fileType, log)


	totals = {}
	for result in results:
		for key in result.keys():
			if key not in totals.keys():
				totals[key] = result[key]
			else:
				totals[key]['count'] += result[key]['count']
				totals[key]['size']  += result[key]['size']

	out = []
	for key, value in totals.items():
		out.append((key, value['count'], value['size']/u))	

	print ""
	print "{:15} {:>15} {:>15}".format('Ext-Type', 'No.Files', 'Capacity ('+U+')')
	print "{:15} {:>15} {:>15}".format('--------', '-----', '-------------')	
	
	for item in sorted(out, key=itemgetter(2,1), reverse=True):
		print "{:15} {:15} {:15.1f}".format(item[0], item[1], item[2])
	
	print ""
	totcount = sum([item[1] for item in out])
	totsize  = sum([item[2] for item in out])
	print "{:15} {:15} {:15.1f}".format('TOTALS:', totcount, totsize)



if __name__ == '__main__':

	start_time = datetime.now()

	main()

	end_time = datetime.now()
	print('Duration: {}'.format(end_time - start_time))
