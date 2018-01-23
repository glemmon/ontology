#!/usr/bin/env python3
from plugin import PhevorPlugin
from os.path import join, split

data_dir = "/opt/neo4j-community/3.1.1-ontos/data"
neo_in_dir = join(data_dir,'in')

def parse_args():
	from argparse import ArgumentParser
	parser = ArgumentParser(description='RX tsv file -> Neo4j')
	parser.add_argument('in_file', help='File with medical orders')
	return parser.parse_args()

def main():
	args = parse_args()
	plugin = PhevorPlugin('RXparser', 'parse_rx')
	from shutil import copy
	import os
	import stat
	in_dir,in_file = split(args.in_file)
	neo_in_path = join(neo_in_dir, in_file)
	copy(args.in_file, neo_in_path)
	os.chmod(neo_in_path, os.stat(neo_in_path).st_mode | stat.S_IROTH)
	fxn_args = {'in_file':neo_in_path}
	plugin.call(**fxn_args)
	os.remove(neo_in_path)

if __name__=='__main__':
	main()
