#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()

import csv
import gevent
import requests
from collections import defaultdict

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

site = 'https://tesera.ru'
tmpdir = 'tmp'
pagedir = 'page_cache'
csvfile = 'games.csv'

api = "https://api.tesera.ru"
batch_limit = 100

def get_games(user):
	offset = 0
	while True:
		res = requests.get(
			'/'.join((api, 'collections', 'base', 'own', user)),
			params={'limit': batch_limit, 'offset': offset},
			verify=False)
		res.raise_for_status()
		if not res.json():
			break
		for game in res.json():
			yield game['game']['title'], game['game']['teseraUrl']
		if offset == int(res.headers['x-total-pages']) - 1: break
		offset += 1


if __name__ == '__main__':
	with open('users') as f:
		users = f.read().splitlines()

	# for user in users:
	# 	print("Verifying user %s" % user)
	# 	res = requests.get('%s/user/%s' % (site, user))
	# 	res.raise_for_status()
	# 	if not res.headers.get('Last-Modified'):
	# 		print("User %s is missing" % user)

	games = defaultdict(list)

	def scrape_job(user):
		print("Scraping user", user)
		return user, list(get_games(user))

	jobs = [gevent.spawn(scrape_job, user) for user in users]
	gevent.joinall(jobs)

	for job in jobs:
		user, user_games = job.value
		for game in user_games:
			games[game].append(user)

	with open(csvfile, 'w') as f:
		writer = csv.writer(f)
		for game, users in sorted(games.items(), key=lambda game: game[0]):
			writer.writerow([f'=HYPERLINK("{game[1]}"; "{game[0]}")', ','.join(users)])

	print("Successfully created file", csvfile)
