#!/usr/bin/env python3
from plugin import PhevorPlugin

def main():
	from sys import argv
	plugin = PhevorPlugin('TreeDumper', 'dump_tree')
	args = {'label':argv[1]}
	print(plugin.call(**args))

if __name__=='__main__':
	main()
