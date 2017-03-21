#!/usr/bin/env python3
from py2neo.packages.httpstream import http
http.socket_timeout=9999
from py2neo import Graph,authenticate
from py2neo import ServerPlugin
from time import sleep
import json

headers={"Content-Type":"application/json"}
#TODO this should extend an abstract Plugin class
class UPDB_plugin():
	def __init__(self, plugin_name, fxn_name):
		authenticate("localhost:7477", "neo4j", "phevor")
		graph = Graph('http://localhost:7477/db/data') # the default location
		plugin = ServerPlugin(graph, plugin_name)
		self.fxn_resource =  plugin.resources[fxn_name]
	def call(self,**kwargs):
		body = json.dumps(kwargs)
		try:
			response = self.fxn_resource.post(body,headers)
			return response.content
		except Exception as err:
			print('ServerError. Args were:',body)
			raise(err)


def test():
	plugin = UPDB_plugin('TreeDumper', 'dump_tree')
	args = {'label':'ICD9dx'}
	print(plugin.call(**args))

if __name__=='__main__':
	test()
