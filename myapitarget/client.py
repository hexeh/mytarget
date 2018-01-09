# -*- coding: utf-8 -*-

import datetime
from dateutil import parser
import pprint
import time
import math
import json
import requests
import urllib
import sys

class MTClient:

	def __init__(self, parent, client_config):

		self.config = client_config
		self.base = 'https://target.my.com'

		self.parent = parent
		self.id = client_config['client_id']
		self.log = []
		self.errors_cnt = 0
		# Check configs
		if type(self.config) is not dict:
			self.log.append({
				'source': 'target',
				'date': str(datetime.datetime.now()),
				'action': 'init',
				'state': 'ERROR',
				'details': {
					'id': self.id,
					'step': 'config_check',
					'reason': 'NOT_DICT',
					'text': ''
				}
			})
			self.errors_cnt += 1
		else:
			valid_keys = set(['client_email', 'client_status', 'expiration', 'client_id', 'client_refresh', 'client_access', 'client_name'])
			existed_keys = set(self.config.keys())
			if existed_keys != valid_keys:
				self.log.append({
					'source': 'target',
					'date': str(datetime.datetime.now()),
					'action': 'init',
					'state': 'ERROR',
					'details': {
						'id': self.id,
						'step': 'config_check',
						'reason': 'KEYS_MISSMATCH',
						'text': str(existed_keys)
					}
				})
				self.errors_cnt += 1
		# Check tokens
		if self.errors_cnt == 0:
			clients_time = datetime.datetime.now() + datetime.timedelta(0, 5)
			if self.config['expiration'] != '-1':
				if clients_time >= parser.parse(self.config['expiration']):
					cli_query = {
						'grant_type': 'refresh_token',
						'refresh_token': self.config['client_refresh'],
						'agency_client_name': self.config['client_email'],
						'client_id': self.parent['id'],
						'client_secret': self.parent['secret']
					}
					cli_grants = requests.post(
						self.base + '/api/v2/oauth2/token.json', 
						headers = {
							'Content-Type': 'application/x-www-form-urlencoded'
						},
						data = cli_query
					)
					if cli_grants.status_code == 403:
						self.log.append({
							'source': 'target',
							'date': str(datetime.datetime.now()),
							'action': 'init',
							'state': 'ERROR',
							'details': {
								'id': self.id,
								'step': 'token_refresh',
								'reason': '403',
								'text': str(cli_grants.text)
							}
						})
					if cli_grants.status_code == 200:
						cli_data = json.loads(cli_grants.text)
						self.config['client_access'] = cli_data['access_token']
						self.config['expiration'] = str(clients_time + datetime.timedelta(0, cli_data['expires_in']))
						self.log.append({
							'source': 'target',
							'date': str(datetime.datetime.now()),
							'action': 'init',
							'state': 'OK',
							'details': {
								'id': self.id,
								'step': 'token_refresh',
								'reason': '200',
								'text': ''
							}
						})
					if cli_grants.status_code == 401:
						new_query = {
							'grant_type': 'agency_client_credentials',
							'agency_client_name': self.config['client_email'],
							'client_id': self.parent['id'],
							'client_secret': self.parent['secret']
						}
						new_token = requests.post(
							self.base + '/api/v2/oauth2/token.json', 
							headers = {
								'Content-Type': 'application/x-www-form-urlencoded'
							},
							data = new_query
						)
						if new_token.status_code == 200:
							cli_data = json.loads(new_token.text)
							self.config['client_access'] = cli_data['access_token']
							self.config['expiration'] = str(clients_time + datetime.timedelta(0, cli_data['expires_in']))
							self.config['client_refresh'] = cli_data['refresh_token']
							self.log.append({
								'source': 'target',
								'date': str(datetime.datetime.now()),
								'action': 'init',
								'state': 'OK',
								'details': {
									'id': self.id,
									'step': 'token_refresh',
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
									'id': self.id,
									'step': 'token_refresh',
									'reason': str(new_token.status_code),
									'text': str(new_token.text)
								}
							})
				else:
					self.log.append({
						'source': 'target',
						'date': str(datetime.datetime.now()),
						'action': 'init',
						'state': 'OK',
						'details': {
							'id': self.id,
							'step': 'token_refresh',
							'reason': 'NOT_MODIFIED',
							'text': ''
						}
					})
			else:
				new_query = {
					'grant_type': 'agency_client_credentials',
					'agency_client_name': self.config['client_email'],
					'client_id': self.parent['id'],
					'client_secret': self.parent['secret']
				}
				new_token = requests.post(
					self.base + '/api/v2/oauth2/token.json', 
					headers = {
						'Content-Type': 'application/x-www-form-urlencoded'
					},
					data = new_query
				)
				if new_token.status_code == 200:
					cli_data = json.loads(new_token.text)
					self.config['client_access'] = cli_data['access_token']
					self.config['expiration'] = str(clients_time + datetime.timedelta(0, cli_data['expires_in']))
					self.config['client_refresh'] = cli_data['refresh_token']
					self.log.append({
						'source': 'target',
						'date': str(datetime.datetime.now()),
						'action': 'init',
						'state': 'OK',
						'details': {
							'id': self.id,
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
							'id': self.id,
							'step': 'token_access',
							'reason': str(new_token.status_code),
							'text': str(new_token.text)
						}
					})
		else:
			self.log.append({
					'source': 'target',
					'date': str(datetime.datetime.now()),
					'action': 'init',
					'state': 'ERROR'
				})

	def getCampaigns(self):

		result = []
		time_sense = datetime.date.today() - datetime.timedelta(90)
		if self.errors_cnt == 0:
			# reasonable_status = ['active', 'blocked', 'deleted']
			#for rs in reasonable_status:
			camps_query = {
				'fields': 'id,name,status,last_stats_updated'
			}
			camps_r = requests.get(
				self.base + '/api/v1/campaigns.json?' + urllib.parse.urlencode(camps_query), 
				headers = {
					'Content-Type': 'application/x-www-form-urlencoded',
					'Authorization': 'Bearer ' + str(self.config['client_access'])
				}
			)
			if camps_r.status_code == 200:
				camps = json.loads(camps_r.text)
				for ic,mc in enumerate(camps):
					camps[ic]['agency'] = self.parent
					camps[ic]['client_id'] = self.id
					if 'last_stats_updated' in mc.keys():
						last_time = datetime.datetime.strptime(mc['last_stats_updated'], '%Y-%m-%d %H:%M:%S').date()
						if last_time >= time_sense:
							result += camps
					self.log.append({
						'source': 'target',
						'date': str(datetime.datetime.now()),
						'action': 'dict',
						'state': 'OK',
						'details': {
							'id': self.id,
							'step': 'campaigns',
							'reason': '200',
							'text': ''
						}
					})
			else:
				self.log.append({
						'source': 'target',
						'date': str(datetime.datetime.now()),
						'action': 'dict',
						'state': 'ERROR',
						'details': {
							'id': self.id,
							'step': 'campaigns',
							'reason': str(camps_r.status_code),
							'text': str(camps_r.text)
						}
					})
		return(result)

	def getStats(self, date_start, date_end):

		stats_total = []
		if self.errors_cnt == 0:
			stats_query = {
				'date_from': str(date_start),
				'date_to': str(date_end)
			}
			stats_r = requests.get(
				self.base + '/api/v1/campaigns/statistics.json?' + urllib.parse.urlencode(stats_query), 
				headers = {
					'Content-Type': 'application/x-www-form-urlencoded',
					'Authorization': 'Bearer ' + str(self.config['client_access'])
				}
			)
			if stats_r.status_code == 200:
				stats_chunk = json.loads(stats_r.text)['campaigns']
				for sc in stats_chunk:
					stats_total.append({
						'client_id': self.id,
						'campaign_id': sc['campaign_id'],
						'date': sc['date'],
						'impressions': sc['general']['shows'],
						'clicks': sc['general']['clicks'],
						'cost': sc['general']['amount']
					})
				self.log.append({
					'source': 'target',
					'date': str(datetime.datetime.now()),
					'action': 'stats',
					'state': 'OK',
					'details': {
						'id': self.id,
						'step': 'stats',
						'reason': '200',
						'text': ''
					}
				})
			else:
				self.log.append({
					'source': 'target',
					'date': str(datetime.datetime.now()),
					'action': 'stats',
					'state': 'OK',
					'details': {
						'id': self.id,
						'step': 'stats',
						'reason': str(stats_r.status_code),
						'text': str(stats_r.text)
					}
				})
		return(stats_total)

	def getStatsV2(self, date_start, date_end):

		stats_total = []
		if self.errors_cnt == 0:

			# reasonable_status = ['active', 'blocked', 'deleted']
			camp_ids = []
			time.sleep(0.3)
			
			#for rs in reasonable_status:
			camps_query = {
				'fields': 'id,last_stats_updated'
			}
			camps_r = requests.get(
				self.base + '/api/v1/campaigns.json?' + urllib.parse.urlencode(camps_query), 
				headers = {
					'Content-Type': 'application/x-www-form-urlencoded',
					'Authorization': 'Bearer ' + str(self.config['client_access'])
				}
			)
			if camps_r.status_code == 200:
				camps = json.loads(camps_r.text)
				camp_ids += [str(cid['id']) for cid in camps if 'last_stats_updated' in cid.keys() and datetime.datetime.strptime(cid['last_stats_updated'], '%Y-%m-%d %H:%M:%S').date() >= datetime.datetime.strptime(date_start, '%d.%m.%Y').date()]
			else:
				self.log.append({
					'source': 'target',
					'date': str(datetime.datetime.now()),
					'action': 'dict',
					'state': 'ERROR',
					'details': {
						'id': self.id,
						'step': 'campaigns',
						'reason': str(camps_r.status_code),
						'text': str(camps_r.text)
					}
				})
			if len(camp_ids) > 0:
				chunks_count = math.ceil(len(camp_ids) / 152)
				if chunks_count <= 1:
					time.sleep(1)
					stats_query = {
						'date_from': str(date_start),
						'date_to': str(date_end),
						'id': ','.join(camp_ids),
						'metrics': 'base'
					}
					stats_r = requests.get(
						self.base + '/api/v2/statistics/campaigns/day.json?' + urllib.parse.urlencode(stats_query), 
						headers = {
							'Content-Type': 'application/x-www-form-urlencoded',
							'Authorization': 'Bearer ' + str(self.config['client_access'])
						}
					)
					if stats_r.status_code == 200:
						stats_chunk = json.loads(stats_r.text)['items']
						for sc in stats_chunk:
							for scs in sc['rows']:
								stats_total.append({
									'client_id': self.id,
									'campaign_id': sc['id'],
									'date': scs['date'],
									'impressions': scs['base']['shows'],
									'clicks': scs['base']['clicks'],
									'cost': scs['base']['spent']
								})
						self.log.append({
							'source': 'target',
							'date': str(datetime.datetime.now()),
							'action': 'stats',
							'state': 'OK',
							'details': {
								'id': self.id,
								'step': 'stats',
								'reason': '200',
								'text': ''
							}
						})
					else:
						self.log.append({
							'source': 'target',
							'date': str(datetime.datetime.now()),
							'action': 'stats',
							'state': 'OK',
							'details': {
								'id': self.id,
								'step': 'stats',
								'reason': str(stats_r.status_code),
								'text': str(stats_r.text)
							}
						})
				else:
					def split(arr, size):
						arrs = []
						while len(arr) > size:
							pice = arr[:size]
							arrs.append(pice)
							arr   = arr[size:]
						arrs.append(arr)
						return arrs
					camps_chunks = split(camp_ids, chunks_count)
					for cch in camps_chunks:
						time.sleep(1)
						stats_query = {
							'date_from': str(date_start),
							'date_to': str(date_end),
							'id': ','.join(cch),
							'metrics': 'base'
						}
						stats_r = requests.get(
							self.base + '/api/v2/statistics/campaigns/day.json?' + urllib.parse.urlencode(stats_query), 
							headers = {
								'Content-Type': 'application/x-www-form-urlencoded',
								'Authorization': 'Bearer ' + str(self.config['client_access'])
							}
						)
						if stats_r.status_code == 200:
							stats_chunk = json.loads(stats_r.text)['items']
							for sc in stats_chunk:
								for scs in sc['rows']:
									stats_total.append({
										'client_id': self.id,
										'campaign_id': sc['id'],
										'date': scs['date'],
										'impressions': scs['base']['shows'],
										'clicks': scs['base']['clicks'],
										'cost': scs['base']['spent']
									})
							self.log.append({
								'source': 'target',
								'date': str(datetime.datetime.now()),
								'action': 'stats',
								'state': 'OK',
								'details': {
									'id': self.id,
									'step': 'stats',
									'reason': '200',
									'text': ''
								}
							})
						else:
							self.log.append({
								'source': 'target',
								'date': str(datetime.datetime.now()),
								'action': 'stats',
								'state': 'OK',
								'details': {
									'id': self.id,
									'step': 'stats',
									'reason': str(stats_r.status_code),
									'text': str(stats_r.text)
								}
							})
			else:
				self.log.append({
					'source': 'target',
					'date': str(datetime.datetime.now()),
					'action': 'dict',
					'state': 'ERROR',
					'details': {
						'id': self.id,
						'step': 'campaigns',
						'reason': 'EMPTY',
						'text': ''
					}
				})
		return(stats_total)

	def getCounters(self):

		c_request = requests.get(
			self.base + '/api/v2/remarketing/counters.json',
			headers = {
				'Content-Type': 'application/x-www-form-urlencoded',
				'Authorization': 'Bearer ' + str(self.config['client_access'])
				}
			)
		if c_request.status_code == 200:
			c_response = json.loads(c_request.text)['items']
			for ci,cc in enumerate(c_response):
				c_response[ci]['client_id'] = self.id
				c_response[ci]['client_name'] = self.config['client_name']
				c_response[ci]['client_email'] = self.config['client_email']
			self.log.append({
				'source': 'target',
				'date': str(datetime.datetime.now()),
				'action': 'dict',
				'state': 'OK',
				'details': {
					'id': self.id,
					'step': 'counters',
					'reason': '200',
					'text': ''
					}
				})
		else:
			self.log.append({
				'source': 'target',
				'date': str(datetime.datetime.now()),
				'action': 'dict',
				'state': 'OK',
				'details': {
					'id': self.id,
					'step': 'counters',
					'reason': str(c_request.status_code),
					'text': str(c_request.text)
					}
				})
			c_response = []

		return(c_response)
