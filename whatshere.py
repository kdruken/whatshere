#!/usr/bin/env python

from __future__ import print_function
import os, sys
from operator import itemgetter



class What_is_here:

	def __init__(self, inputs=None, topdir=None):
		
		if not topdir:
			if len(inputs) < 2:
				sys.exit()

			if os.path.isdir(inputs[1]):
				self.topdir = inputs[1]
		else:
			self.topdir = topdir
                
		self.filetypes = {'*no_ext*': 0}
		self.filesizes = {'*no_ext*': 0}


	def scan(self): 
		## To think about: how to predict the 'v20150212' type format for .nc in ua6
		## as well as whether to follow symlinks or not...
		for root, dirs, files in os.walk(self.topdir, topdown=False, followlinks=True):
			for name in files:
				fullpath = os.path.join(root, name)
				size = os.stat(fullpath)[6]
				if name.count('.') != 0:
					ext = name.split('.')[-1].lower()
					try:
						i = int(ext)
						ext = '*numeric*'
					except ValueError:
						pass					
						
					if ext in self.filetypes.keys():
						self.filetypes[ext] += 1
						self.filesizes[ext] += size
					else:
						self.filetypes[ext] = 1
						self.filesizes[ext] = size
				else:
					self.filetypes['*no_ext*'] += 1
					self.filesizes['*no_ext*'] += size

if __name__ == "__main__":
	
	run = What_is_here(inputs=sys.argv)
	run.scan()
	

	result = []
	for key, item in run.filetypes.items():
		result.append((key, item, run.filesizes[key]/1e6))
	
	print()
	print("{:15} {:>15} {:>15}".format('Ext-Type', 'Count', 'Capacity (Mb)'))
	print("{:15} {:>15} {:>15}".format('--------', '-----', '-------------'))	
	
	for item in sorted(result, key=itemgetter(2,1), reverse=True):
		print("{:15} {:15} {:15.1f}".format(item[0], item[1], item[2]))
	
	print()
	totcount = sum([item[1] for item in result])
	totsize  = sum([item[2] for item in result])
	print("{:15} {:15} {:15.1f}".format('TOTALS:', totcount, totsize))
