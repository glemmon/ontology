#!/usr/bin/env python3
from plugin import PhevorPlugin

def parse_args():
	from argparse import ArgumentParser
	parser = ArgumentParser(description='Dump Neo4j DAGs as a tree')
	parser.add_argument('--pad', action='store_true', default=False, help='pad results with appropriate # of zeros')
	parser.add_argument('label', help='The Ontology we are dumping')
	return parser.parse_args()

def main():
	args = parse_args()
	plugin = PhevorPlugin('TreeDumper', 'dump_tree')
	fxn_args = {'label':args.label, 'pad':args.pad}
	print(plugin.call(**fxn_args))

if __name__=='__main__':
	main()
