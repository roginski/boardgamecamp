#!/usr/bin/env python3
from gevent import monkey; monkey.patch_all()

import os
import csv
import json
import errno
import gevent
import requests
from bs4 import BeautifulSoup
from collections import defaultdict

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

site = 'https://tesera.ru'
tmpdir = 'tmp'
pagedir = 'page_cache'
csvfile = 'games.csv'

game_name_cache_file = 'game_name_cache.json'

def read_file_if_exists(filename):
	try:
		with open(filename) as f:
			return f.read()
	except IOError as e:
		if e.errno == errno.ENOENT:
			return None
		else:
			raise

data = read_file_if_exists(game_name_cache_file)
game_name_cache = json.loads(data) if data else {}

def get_html(path):
	data = read_file_if_exists('%s/%s' % (pagedir, path.replace('/', '\\')))
	if not data:
		print("Reading page", path)
		res = requests.get('%s%s' % (site, path))
		res.raise_for_status()
		data = res.text
		with open('%s/%s' % (pagedir, path.replace('/', '\\')), 'w') as f:
			f.write(data)
	return BeautifulSoup(data, 'html.parser')

def extract_game_title(path):
	name = game_name_cache.get(path)
	if name:
		print("Game %s found in cache" % name)
		return name
	print("Extracting game name from %s" % path)
	html = get_html(path)
	name = html.find('h1', id='game_title').span.getText().strip()
	game_name_cache[path] = name
	return name

def find_next_page(html):
	pagination = html.find('div', class_='pagination')
	if not pagination: return
	arrow = pagination.find('img')
	if not arrow or arrow['src'] != '/img/br.gif': return
	ref = arrow.parent
	if ref.name != 'a':
		print("Something went wrong, arrow's name isn't a")
		return
	return ref['href']

def extract_games(html):
	for div in html.find_all('div', class_='gameslinked'):
		game_ref = div.find('div', class_='text').find('a')['href']
		yield extract_game_title(game_ref)

def get_games(html):
	yield from extract_games(html)
	while True:
		next_page = find_next_page(html)
		if next_page:
			html = get_html(next_page)
			yield from extract_games(html)
		else:
			break

def get_user_games(user):
	tmpfile = '%s/%s.tmp' % (tmpdir, user)
	userfile = '%s/%s.games' % (tmpdir, user)
	try:
		with open(userfile) as f:
			print("Found saved game list for user", user)
			for line in f.readlines():
				yield line.strip()
	except IOError as e:
		if e.errno != errno.ENOENT: raise
		html = get_html('/user/%s/games/owns' % user)
		with open(tmpfile, 'w') as f:
			for game in get_games(html):
				f.write("%s\n" % game)
				yield game
		os.rename(tmpfile, userfile)

##################################################################################

api = "https://api.tesera.ru"
batch_limit = 100

def get_games2(user):
	offset = 0
	while True:
		res = requests.get(
			'/'.join((api, 'collections', 'base', 'own', user)),
			params={'limit': batch_limit, 'offset': offset},
			verify=False)
		res.raise_for_status()
		for game in res.json():
			yield '=HYPERLINK("%s", "%s")' % (game['game']['teseraUrl'], game['game']['title'])
		if offset == int(res.headers['x-total-pages'] )- 1: break
		offset += 1

def get_user_games2(user):
	tmpfile = '%s/%s.tmp' % (tmpdir, user)
	userfile = '%s/%s.games' % (tmpdir, user)
	try:
		with open(userfile) as f:
			print("Found saved game list for user", user)
			for line in f.readlines():
				yield line.strip()
	except FileNotFoundError:
		with open(tmpfile, 'w') as f:
			for game in get_games2(user):
				f.write("%s\n" % game)
				yield game
		os.rename(tmpfile, userfile)


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
#  	for game in get_user_games2(user):
#  		games[game].append(user)

def scrape_job(user):
	print("Scraping user",user)
	return user, list(get_user_games2(user))

try:
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
finally:
	with open('%s/%s' % (tmpdir, game_name_cache_file), 'w') as f:
		json.dump(game_name_cache, f)

print("Successfully created file", csvfile)
