#!/usr/bin/env python3
from plugin import PhevorPlugin

def parse_args():
	from argparse import ArgumentParser
	parser = ArgumentParser(description='Search Ontologies for this term')
	parser.add_argument('--search_by_id', action='store_true', default=False, help='Whether to search by id')
	parser.add_argument('query', help='Term or ID to serach for')
	return parser.parse_args()

def main():
	args = parse_args()
	plugin = PhevorPlugin('TextQuery', 'find_nodes')
	q = '(?ism).*'+args.query+".*"
	fxn_args = {'query':q, 'search_by_id':args.search_by_id, 'hp_only':True}
	print(plugin.call(**fxn_args))

if __name__=='__main__':
	main()
