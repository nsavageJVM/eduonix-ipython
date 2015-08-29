#!/usr/bin/python
# -*- coding: utf-8 -*-

#  test code for pydoop mapreduce


def readAnParse(input_file ):

	dataset = []
	f = open(input_file, "r")
	lines = f.read().split("\n")

	for line in lines:
    		if line != "": # add other needed checks to skip titles
        		cols = line.split()
			# dataset LONGITUDE  LATITUDE  T_DAILY_MEAN  SUR_TEMP_DAILY_AVG  SOIL_MOISTURE_10_DAILY
			dataset.append('%s|%s|%s|%s|%s'%(cols[5], cols[6], cols[9], cols[16], cols[19]))
	f.close()
	return dataset

def generate_data(dataset, output_file):
	f = open(output_file, 'w')
	for line in dataset:
		 f.write('%s\n'%(line))


def main():
# /home/ubu/  --> location of sci_data_1.txt
	records = readAnParse("/home/ubu/sci_data_1.txt")
	generate_data(records, "/home/ubu/sci_data_out.txt")



main()



