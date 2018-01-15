# -*- coding: utf-8 -*-

import datetime
from dateutil import parser
import pprint
import time
import json
import threading
import requests
import urllib
import sys
from queue import Queue
from myapitarget import MTClient

class MTAgency:

	def __init__(self, config):

		self.config = config
		self.base = "https://target.my.com"
		self.log = []
		request_time = datetime.datetime.now() + datetime.timedelta(0, 60)
		for i,v in enumerate(self.config['grants']):
			if(request_time >= parser.parse(self.config['grants'][i]['token_info']['expired_at'])):
				token_query = {
					'grant_type': 'refresh_token',
					'refresh_token': self.config['grants'][i]['token_info']['refresh'],
					'client_id': self.config['grants'][i]['client_id'],
					'client_secret': self.config['grants'][i]['client_secret']
				}
				token = requests.post(
					self.base + '/api/v2/oauth2/token.json', 
					headers = {
						'Content-Type': 'application/x-www-form-urlencoded'
					},
					data = token_query
				)
				if token.status_code == 200:
					updated_token = json.loads(token.text)
					self.config['grants'][i]['token_info'] = {
						'expired_at': str(datetime.datetime.now() + datetime.timedelta(0, updated_token['expires_in'])),
						'access': updated_token['access_token'],
						'refresh': updated_token['refresh_token']
					}
					self.log.append({
						'source': 'target',
						'date': str(datetime.datetime.now()),
						'action': 'init',
						'state': 'OK',
						'details': {
							'id': self.config['grants'][i]['client_id'],
							'step': 'token_refresh',
							'reason': '200',
							'text': ''
						}
					})			
				if token.status_code == 401:
					new_query = {
						'grant_type': 'client_credentials',
						'client_id': self.config['grants'][i]['client_id'],
						'client_secret': self.config['grants'][i]['client_secret']
					}
					new_token = requests.post(
						self.base + '/api/v2/oauth2/token.json', 
						headers = {
							'Content-Type': 'application/x-www-form-urlencoded'
						},
						data = new_query
					)
					if new_token.status_code == 200:
						self.config['grants'][i]['token_info'] = {
							'expired_at': str(datetime.datetime.now() + datetime.timedelta(0, json.loads(new_token.text)['expires_in'])),
							'access': json.loads(new_token.text)['access_token'],
							'refresh': json.loads(new_token.text)['refresh_token']
						}
						self.log.append({
							'source': 'target',
							'date': str(datetime.datetime.now()),
							'action': 'init',
							'state': 'OK',
							'details': {
								'id': self.config['grants'][i]['client_id'],
								'step': 'token_access',
								'reason': '200',
								'text': ''
							}
						})
					else:
						self.log.append({
							'source': 'target',
							'date': str(datetime.datetime.now()),
							'action': 'init',
							'state': 'ERROR',
							'details': {
								'id': self.config['grants'][i]['client_id'],
								'step': 'token_access',
								'reason': str(new_token.status_code),
								'text': str(cli_grants.text)
							}
						})
				if token.status_code not in [200,401]:
					self.log.append({
						'source': 'target',
						'date': str(datetime.datetime.now()),
						'action': 'init',
						'state': 'ERROR',
						'details': {
							'id': self.config['grants'][i]['client_id'],
							'step': 'token_refresh',
							'reason': str(cli_grants.status_code),
							'text': str(cli_grants.text)
						}
					})
			else:
				self.log.append({
						'source': 'target',
						'date': str(datetime.datetime.now()),
						'action': 'init',
						'state': 'OK',
						'details': {
							'id': self.config['grants'][i]['client_id'],
							'step': 'token_refresh',
							'reason': 'NOT_MODIFIED',
							'text': ''
						}
					})	

		# do it after init, not in class
		#with open('configs/mt.json', 'w') as jsonf:
		#	json.dump(self.config, jsonf)

	def getClients(self, existed, updateList = True, doPar = True):

		result = {}

		if doPar:
			q = Queue(maxsize = 0)
			def threadingClients(q,result):
				while not q.empty():
					work = q.get()
					client_inst = MTClient(work[1],work[2])
					result.append({'log': client_inst.log, 'configs': client_inst.config})
					q.task_done()
				return True

			if updateList:
				for i in self.config['grants']:
					parent = {'id': i['client_id'], 'secret': i['client_secret']}
					clients = requests.get( 
						self.base + '/api/v1/clients.json',
						headers = {
								'Content-Type': 'application/x-www-form-urlencoded',
								'Authorization': 'Bearer ' + str(i['token_info']['access'])
						}
					)
					if(clients.status_code == 200):
						resulted_list = []
						client_full_list = json.loads(clients.text)
						client_existed_ids = [e['client_id'] for e in existed[i['client_id']]]
						client_new_list = [c for c in client_full_list if c['id'] not in client_existed_ids]
						load = [cc for cc in existed[i['client_id']]]
						num_threads = min(50, len(load))
						results = []

						for l in range(len(load)):
							q.put((l, parent, load[l]))

						for t in range(num_threads):
							t = threading.Thread(target = threadingClients, args = (q,results))
							t.start()

						q.join()
						with open('configs/new.json', 'w') as jsonf:
							json.dump(results, jsonf)
						self.log += [item for sublist in [cl['log'] for cl in results] for item in sublist]
						resulted_list = [cl['configs'] for cl in results]
						if len(client_new_list) > 0:
							client_new_configs = [{
									'client_id': cn['id'],
									'client_name': cn['additional_info']['client_name'],
									'client_email': cn['username'],
									'client_status': cn['status'],
									'client_access': '_blank',
									'client_refresh': '_blank',
									'expiration': '-1'
								} for cn in client_new_list]
							new_load = [nc for nc in client_new_configs]
							num_threads = min(50, len(new_load))
							results_new = [{} for x in new_load]

							with q.mutex:
								q.queue.clear()

							for l in range(len(new_load)):
								q.put((l, parent, new_load[l]))

							for t in range(num_threads):
								t = threading.Thread(target = threadingClients, args = (q,results_new))
								t.start()
							q.join()
							self.log += [item for sublist in [cl['log'] for cl in results_new] for item in sublist]
							resulted_list += [cl['configs'] for cl in results_new]
					else:
						self.log.append({
							'source': 'target',
							'date': str(datetime.datetime.now()),
							'action': 'clients',
							'state': 'ERROR',
							'details': {
								'id': i['client_id'],
								'step': 'list',
								'reason': str(clients.status_code),
								'text': str(clients.text)
							}
						})
					result[i['client_id']] = resulted_list
			else:
				for i in self.config['grants']:
					parent = {'id': i['client_id'], 'secret': i['client_secret']}
					load = [cc for cc in existed[i['client_id']]]

					num_threads = min(50, len(load))
					results = [{} for x in load]
					for l in range(len(load)):
						q.put((l, parent, load[l]))

					for t in range(num_threads):
						t = threading.Thread(target = threadingClients, args = (q,results))
						t.start()
					q.join()
					self.log += [item for sublist in [cl['log'] for cl in results] for item in sublist]
					result[i['client_id']] += [item for sublist in [cl['configs'] for cl in results] for item in sublist]
		else:
			if updateList:
				for i in self.config['grants']:
					clients = requests.get( 
						self.base + '/api/v1/clients.json',
						headers = {
								'Content-Type': 'application/x-www-form-urlencoded',
								'Authorization': 'Bearer ' + str(i['token_info']['access'])
						}
					)
					if(clients.status_code == 200):
						resulted_list = []
						client_full_list = json.loads(clients.text)
						client_existed_ids = [e['client_id'] for e in existed[i['client_id']]]
						client_new_list = [c for c in client_full_list if c['id'] not in client_existed_ids]
						for ei,ev in enumerate(existed[i['client_id']]):
							client_account = MTClient(ev['client_id'], i['client_id'], i['client_secret'], ev)
							existed[i['client_id']][ei] = client_account.config
							self.log += client_account.log
						if len(client_new_list) > 0:
							client_new_configs = []
							for cn in client_new_list:
								client_tmp_config = {
									'client_id': cn['id'],
									'client_name': cn['additional_info']['client_name'],
									'client_email': cn['username'],
									'client_status': cn['status'],
									'client_access': '_blank',
									'client_refresh': '_blank',
									'expiration': '-1'
								}
								client_account = MTClient(cn['id'], i['client_id'], i['client_secret'], client_tmp_config)
								client_new_configs.append(client_account.config)
								self.log += client_account.log
							resulted_list = existed[i['client_id']] + client_new_configs
						else:
							resulted_list = existed[i['client_id']]
					else:
						self.log.append({
							'source': 'target',
							'date': str(datetime.datetime.now()),
							'action': 'clients',
							'state': 'ERROR',
							'details': {
								'id': i['client_id'],
								'step': 'list',
								'reason': str(clients.status_code),
								'text': str(clients.text)
							}
						})
					result[i['client_id']] = resulted_list
			else:
				for i in self.config['grants']:
					for ei,ev in enumerate(existed[i['client_id']]):
						client_account = MTClient(ev['client_id'], i['client_id'], i['client_secret'], ev)
						existed[i['client_id']][ei] = client_account.config
						self.log += client_account.log
					result[i['client_id']] = existed[i['client_id']]
		
		return(result)

	def getCampaigns(self, clients, clientsLimit = 0, doPar = True):

		result = {}

		if doPar:
			q = Queue(maxsize = 0)
			def threadingCampaigns(q,result):
				while not q.empty():
					work = q.get()
					client_inst = MTClient(work[1],work[2])
					camps = client_inst.getCampaigns()
					result[work[0]] = {'log': client_inst.log, 'campaigns': camps}
					q.task_done()
				return True

			for i in self.config['grants']:

				parent = {'id': i['client_id'], 'secret': i['client_secret']}
				if clientsLimit > 0:
					load = [cc for ic,cc in enumerate(clients[i['client_id']][:clientsLimit])]
				else:
					load = [cc for ic,cc in enumerate(clients[i['client_id']])]

				num_threads = min(50, len(load))
				results = [{} for x in load]
				for l in range(len(load)):
					q.put((l, parent, load[l]))
				for t in range(num_threads):
					t = threading.Thread(target = threadingCampaigns, args = (q,results))
					t.start()
				q.join()
				self.log += [item for sublist in [cl['log'] for cl in results] for item in sublist]
				result[i['client_id']] = [item for sublist in [cl['campaigns'] for cl in results] for item in sublist]
		else:
			for i in self.config['grants']:
				resulted_list = []
				clients_list = clients[i['client_id']]
				for ci,cc in enumerate(clients_list):
					if clientsLimit > 0 and ci == clientsLimit:
						break
					client_account = MTClient(cc['client_id'], i['client_id'], i['client_secret'], cc)
					self.log += client_account.log
					resulted_list += client_account.getCampaigns()
					self.log += client_account.log
				result[i['client_id']] = resulted_list

		return(result)

	def getStats(self, clients, date_start, date_end, clientsLimit = 0, doPar = True):

		result = {}

		if doPar:
			q = Queue(maxsize = 0)
			def threadingStats(q,result):
				while not q.empty():
					work = q.get()
					client_inst = MTClient(work[1],work[2])
					stats = client_inst.getStats(date_start, date_end)
					result[work[0]] = {'log': client_inst.log, 'stats': stats}
					q.task_done()
				return True

			for i in self.config['grants']:
				parent = {'id': i['client_id'], 'secret': i['client_secret']}
				if clientsLimit > 0:
					load = [cc for ic,cc in enumerate(clients[i['client_id']][:clientsLimit])]
				else:
					load = [cc for ic,cc in enumerate(clients[i['client_id']])]

				num_threads = min(50, len(load))
				results = [{} for x in load]

				for l in range(len(load)):
					q.put((l, parent, load[l]))
				for t in range(num_threads):
					t = threading.Thread(target = threadingStats, args = (q,results))
					t.start()
				q.join()
				self.log += [item for sublist in [cl['log'] for cl in results] for item in sublist]
				result[i['client_id']] = [item for sublist in [cl['stats'] for cl in results] for item in sublist]
		else:
			for i in self.config['grants']:
				resulted_list = []
				clients_list = clients[i['client_id']]
				for ci,cc in enumerate(clients_list):
					if clientsLimit > 0 and ci == clientsLimit:
						break
					client_account = MTClient(cc['client_id'], i['client_id'], i['client_secret'], cc)
					self.log += client_account.log
					resulted_list += client_account.getStats(date_start, date_end)
					self.log += client_account.log
				result[i['client_id']] = resulted_list

		return(result)

	def getStatsV2(self, clients, date_start, date_end, clientsLimit = 0, doPar = True):

		result = {}

		if doPar:
			q = Queue(maxsize = 0)
			def threadingStats(q,result):
				while not q.empty():
					work = q.get()
					client_inst = MTClient(work[1],work[2])
					stats = client_inst.getStatsV2(date_start, date_end)
					result[work[0]] = {'log': client_inst.log, 'stats': stats}
					q.task_done()
				return True

			for i in self.config['grants']:
				parent = {'id': i['client_id'], 'secret': i['client_secret']}
				if clientsLimit > 0:
					load = [cc for ic,cc in enumerate(clients[i['client_id']][:clientsLimit])]
				else:
					load = [cc for ic,cc in enumerate(clients[i['client_id']])]

				num_threads = min(50, len(load))
				results = [{} for x in load]

				for l in range(len(load)):
					q.put((l, parent, load[l]))
				for t in range(num_threads):
					t = threading.Thread(target = threadingStats, args = (q,results))
					t.start()
				q.join()
				self.log += [item for sublist in [cl['log'] for cl in results] for item in sublist]
				result[i['client_id']] = [item for sublist in [cl['stats'] for cl in results] for item in sublist]
		else:
			for i in self.config['grants']:
				resulted_list = []
				clients_list = clients[i['client_id']]
				for ci,cc in enumerate(clients_list):
					if clientsLimit > 0 and ci == clientsLimit:
						break
					client_account = MTClient(cc['client_id'], i['client_id'], i['client_secret'], cc)
					self.log += client_account.log
					resulted_list += client_account.getStatsV2(date_start, date_end)
					self.log += client_account.log
				result[i['client_id']] = resulted_list

		return(result)

	def getCounters(self, clients, clientsLimit = 0):

		result = {}
		q = Queue(maxsize = 0)
		def threadingCounters(q,result):
			while not q.empty():
				work = q.get()
				client_inst = MTClient(work[1],work[2])
				camps = client_inst.getCounters()
				result[work[0]] = {'log': client_inst.log, 'campaigns': camps}
				q.task_done()
			return True

		for i in self.config['grants']:
			parent = {'id': i['client_id'], 'secret': i['client_secret']}
			if clientsLimit > 0:
				load = [cc for ic,cc in enumerate(clients[i['client_id']][:clientsLimit])]
			else:
				load = [cc for ic,cc in enumerate(clients[i['client_id']])]

			num_threads = min(50, len(load))
			results = [{} for x in load]
			for l in range(len(load)):
				q.put((l, parent, load[l]))
			for t in range(num_threads):
				t = threading.Thread(target = threadingCounters, args = (q,results))
				t.start()
			q.join()
			self.log += [item for sublist in [cl['log'] for cl in results] for item in sublist]
			result[i['client_id']] = [item for sublist in [cl['campaigns'] for cl in results] for item in sublist]

		return(result)
