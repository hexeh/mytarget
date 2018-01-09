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
	parser.add_argument("task", choices = ['clients', 'campaigns', 'counters', 'stats', 'stats_v2'], help = "complete task")
	parser.add_argument("-l", "--show_log", action = "store_true", help = "print execution log")
	parser.add_argument("-i", "--import_db", action = "store_true", help = "import to database")
	parser.add_argument("-v", "--verbose", action = "store_true", help = "print execution info")
	parser.add_argument("-t", "--with_threading", action = "store_true", help = "perform task in parallel mode")
	parser.add_argument("-cl", "--clients_list", nargs = '*', help = "produce clients list", default = [])
	parser.add_argument("-dr", "--date_range", nargs = '*', help = "produce date range", default = [str(YESTERDAY)])
	args = parser.parse_args()
	
	if len(args.date_range) != 1 and len(args.date_range) > 2:
		print('Date range can contain only one or two dates')
		sys.exit()
	
	pp = pprint.PrettyPrinter( indent = 4 )
	loadable = args.import_db

	mt = MTAgency(config)
	
	# Update tokens in file
	with open('configs/sample_main.json', 'w') as jsonf:
		json.dump(mt.config, jsonf)
	current_task = args.task
	clients = {}

	# Check whether agency clients stored or not
	try:
		clients_file = open('configs/clients_list.json')
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
	
		log = []
		if len(args.clients_list) > 0:
			# Set updateList to True for first run
			clients_new = mt.getClients(clients, updateList = False, doPar = args.with_threading)
			if args.verbose:
				print('UPDATING FOR SELECTED CLIENTS')
		else:
			clients_new = mt.getClients(clients, doPar = args.with_threading)
			if args.verbose:
				print('UPDATING EXISTING CLIENTS')
		with open('configs/clients_list.json', 'w') as cl:
			json.dump(clients_new, cl)
		log += mt.log
	
	if current_task == 'campaigns':

		if args.verbose:
			print('GET CAMPAIGNS')
		campaigns = mt.getCampaigns(clients, clientsLimit = 0, doPar = args.with_threading)
		log = []
		log += mt.log
			
	if current_task == 'stats':

		date_range = [datetime.datetime.strptime(x, '%Y-%m-%d').date() for x in args.date_range]
		if len(date_range) == 1:
			date_range.append(date_range[0])
		if args.verbose:
			print('PREPARING TO FETCH STATS FOR PERIOD: \033[1;32;40m [{0!s};{1!s}] \033[0m'.format(date_range[0].strftime('%Y-%m-%d'), date_range[1].strftime('%Y-%m-%d')))
		date_next = date_range[0]
		log = []
		payload = []
		while date_next <= date_range[1]:
			stats_new = mt.getStats(clients, date_next.strftime('%d.%m.%Y'), date_next.strftime('%d.%m.%Y'), clientsLimit = 0, doPar = args.with_threading)
			if args.verbose:
				print('FETCHING FOR DATE: \033[1;32;40m {0!s}  \033[0m'.format(date_next.strftime('%Y-%m-%d')))
			del_s = h.diffStats('target', date_next.strftime('%Y-%m-%d'), untilEnd = False)
			# Dumping local results
			for i in config['grants']:
				for j,cc in enumerate(stats_new[i['client_id']]):
					payload.append(cc)
			with open('mt_dump/dump_{0!s}.json'.format(date_range[0].strftime('%Y%m%d')), 'w') as f:
				json.dump(payload, f)
			if args.verbose:
				print('SAVED FOR DATE: \033[1;32;40m {0!s}  \033[0m'.format(date_next.strftime('%Y-%m-%d')))
			date_next = date_next + datetime.timedelta(1)
		log += mt.log
	
	if current_task == 'stats_v2':

		date_range = [datetime.datetime.strptime(x, '%Y-%m-%d').date() for x in args.date_range]
		if len(date_range) == 1:
			date_range.append(date_range[0])
		if args.verbose:
			print('PREPARING TO FETCH STATS FOR PERIOD: \033[1;32;40m [{0!s};{1!s}] \033[0m'.format(date_range[0].strftime('%Y-%m-%d'), date_range[1].strftime('%Y-%m-%d')))
		date_next = date_range[0]
		log = []
		payload = []
		while date_next <= date_range[1]:
			stats_new = mt.getStatsV2(clients, date_next.strftime('%d.%m.%Y'), date_next.strftime('%d.%m.%Y'), clientsLimit = 0, doPar = args.with_threading)
			if args.verbose:
				print('FETCHING FOR DATE: \033[1;32;40m {0!s}  \033[0m'.format(date_next.strftime('%Y-%m-%d')))
			# Dumping local results
			for i in config['grants']:
				for j,cc in enumerate(stats_new[i['client_id']]):
					payload.append(cc)
			with open('mt_dump/dump_{0!s}.json'.format(date_range[0].strftime('%Y%m%d')), 'w') as f:
				json.dump(payload, f)
			if args.verbose:
				print('SAVED FOR DATE: \033[1;32;40m {0!s}  \033[0m'.format(date_next.strftime('%Y-%m-%d')))
			date_next = date_next + datetime.timedelta(1)
		log += mt.log
	
	if current_task == 'counters':

		counters = mt.getCounters(clients)
		log = mt.log
		
	if args.show_log:
		pp.pprint(log)
