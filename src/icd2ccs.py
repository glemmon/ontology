#!/usr/bin/env python3
from plugin import PhevorPlugin
from os.path import join, split

data_dir = "/opt/neo4j-community/3.1.1-ontos/data"
neo_in_dir = join(data_dir,'in')
neo_out_dir = join(data_dir,'out')

def parse_args():
	from argparse import ArgumentParser
	parser = ArgumentParser(description='Print icd9 -> ccs codes')
	return parser.parse_args()

def main():
	args = parse_args()
	plugin = PhevorPlugin('AddParents', 'add_parents_to_file')
	from shutil import copy
	import os
	import stat
	in_dir,in_file = split(args.in_file)
	out_dir,out_file = split(args.out_file)
	neo_in_path = join(neo_in_dir, in_file)
	neo_out_path = join(neo_out_dir, out_file)
	copy(args.in_file, neo_in_path)
	os.chmod(neo_in_path, os.stat(neo_in_path).st_mode | stat.S_IROTH)
	fxn_args = {'in_file':neo_in_path, 'out_file':neo_out_path}
	plugin.call(**fxn_args)
	copy(neo_out_path, args.out_file)

if __name__=='__main__':
	main()
