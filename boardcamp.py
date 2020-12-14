#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()

import os
import csv
import json
import errno
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
		for game in res.json():
			yield '=HYPERLINK("%s", "%s")' % (game['game']['teseraUrl'], game['game']['title'])
		if offset == int(res.headers['x-total-pages']) - 1: break
		offset += 1


def get_user_games(user):
	tmpfile = '%s/%s.tmp' % (tmpdir, user)
	userfile = '%s/%s.games' % (tmpdir, user)
	try:
		with open(userfile) as f:
			print("Found saved game list for user", user)
			for line in f.readlines():
				yield line.strip()
	except FileNotFoundError:
		with open(tmpfile, 'w') as f:
			for game in get_games(user):
				f.write("%s\n" % game)
				yield game
		os.rename(tmpfile, userfile)


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

	# sequential user scan; slow
	# for user in users:
	#  	print('Scraping user %s' % user)
	#  	for game in get_user_games(user):
	#  		games[game].append(user)

	def scrape_job(user):
		print("Scraping user", user)
		return user, list(get_user_games(user))

	jobs = [gevent.spawn(scrape_job, user) for user in users]
	gevent.joinall(jobs)

	for job in jobs:
		user, user_games = job.value
		for game in user_games:
			games[game].append(user)

	# try:
	# 	with open('oldgames.csv') as f:
	# 		oldgames = dict(((row[1], row) for row in csv.reader(f)))
	# except FileNotFoundError:
	# 	oldgames = {}

	# for game, owners in games.items():
	# 	if game in oldgames:
	# 		oldowners = set(oldgames[game][0].split(','))
	# 		oldowners.update(owners)
	# 		oldgames[game][0] = ','.join(oldowners)
	# 	else:
	# 		oldgames[game] = [','.join(owners), game, '', '', '', '', '', '', '', '', '', '', '', '', '']

	# with open('newgames.csv', 'w') as f:
	# 	writer = csv.writer(f)
	# 	for row in oldgames.values():
	# 		writer.writerow(row)

	with open(csvfile, 'w') as f:
		writer = csv.writer(f)
		for game, users in sorted(games.items()):
			writer.writerow([game, ','.join(users)])

	print("Successfully created file", csvfile)
