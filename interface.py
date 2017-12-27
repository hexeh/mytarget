# -*- coding: utf-8 -*-

import pprint
import json
import os
import sys
import datetime
import argparse
from myapitarget import MTAgency

YESTERDAY = datetime.date.today() - datetime.timedelta(1)

if __name__ == '__main__':

	today = datetime.datetime.today()
	config_file = open('configs/sample_main.json')
	config = json.load(config_file)
	config_file.close()
	
	parser = argparse.ArgumentParser( description = 'Interface for myTarget API')
	parser.add_argument("task", help = "complete task")
	parser.add_argument("-l", "--show_log", action = "store_true", help = "print execution log")
	parser.add_argument("-t", "--with_threading", action = "store_true", help = "perform task in parallel mode")
	parser.add_argument("-cl", "--clients_list", nargs = '*', help = "produce clients list", default = [])
	parser.add_argument("-dr", "--date_range", nargs = '*', help = "produce date range", default = [str(YESTERDAY)])
	args = parser.parse_args()
	
	if len(args.date_range) != 1 and len(args.date_range) > 2:
		print('Date range can contain only one or two dates')
		sys.exit()
	
	pp = pprint.PrettyPrinter( indent = 4 )
	mt = MTAgency(config)
	
	# Update tokens in file
	with open('configs/sample_main.json', 'w') as jsonf:
		json.dump(mt.config, jsonf)
	current_task = args.task
	clients = {}

	# Check whether agency clients stored or not
	try:
		clients_file = open('configs/clients.json')
		current_clients = json.load(clients_file)
		clients_file.close()
	except:
		current_clients = {}
	
	# Reduce array of clients to defined ids
	if len(args.clients_list) > 0:
		for i in config['grants']:
			clients[i['client_id']] = [client for client in current_clients[i['client_id']] if str(client['client_id']) in args.clients_list]
	else:
		clients = current_clients
		
	if current_task == 'clients':
	
		if len(args.clients_list) > 0:
			# Set updateList to True for first run
			clients_new = mt.getClients(clients, updateList = False, doPar = args.with_threading)
		else:
			clients_new = mt.getClients(clients)
		with open('configs/clients_list.json', 'w') as cl:
			json.dump(clients_new, cl)
		log = mt.log
	
	if current_task == 'campaigns':

		campaigns = mt.getCampaigns(clients, clientsLimit = 0, doPar = args.with_threading)
		log = mt.log
			
	if current_task == 'stats':

		date_range = [datetime.datetime.strptime(x, '%Y-%m-%d').date() for x in args.date_range]
		if len(date_range) == 1:
			date_range.append(date_range[0])
		date_next = date_range[0]
		while date_next <= date_range[1]:
			stats_new = mt.getStats(clients, date_next.strftime('%d.%m.%Y'), date_next.strftime('%d.%m.%Y'), clientsLimit = 0, doPar = args.with_threading)
			# Dumping local results
			with open('dump/dump_{0!s}.json'.format(date_next.strftime('%Y%m%d')), 'w') as f:
				json.dump(stats_new, f)
			date_next = date_next + datetime.timedelta(1)
	
	if current_task == 'stats_v2':

		date_range = [datetime.datetime.strptime(x, '%Y-%m-%d').date() for x in args.date_range]
		if len(date_range) == 1:
			date_range.append(date_range[0])
		date_next = date_range[0]
		while date_next <= date_range[1]:
			stats_new = mt.getStatsV2(clients, date_next.strftime('%d.%m.%Y'), date_next.strftime('%d.%m.%Y'), clientsLimit = 0, doPar = args.with_threading)
			# Dumping local results
			with open('dump/dump_{0!s}.json'.format(date_next.strftime('%Y%m%d')), 'w') as f:
				json.dump(stats_new, f)
			date_next = date_next + datetime.timedelta(1)
	
	if current_task == 'counters':

		counters = mt.getCounters(clients)
		log = mt.log
		
	if args.show_log:
			pp.pprint(log)
