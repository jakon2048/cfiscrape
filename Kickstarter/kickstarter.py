#!/usr/bin/python
# -*- coding: utf-8 -*-

import gevent.monkey
gevent.monkey.patch_socket()

import os
import re
import json
import errno
import urllib2
import socket
import hashlib
import csv
import gevent
import gevent.pool
import time
import argparse
from httplib import BadStatusLine
import lxml.html
from lxml.cssselect import CSSSelector
BASE_URL = 'http://www.kickstarter.com'
URL_FILE = 'project_url_initial.csv'
THREADS = 5

def decode_htmlentities(string):
    def substitute_entity(match):
        ent = match.group(3)
        if match.group(1) == "#":
            # decoding by number
            if match.group(2) == '':
                # number is in decimal
                return unichr(int(ent))
            elif match.group(2) == 'x':
                # number is in hex
                return unichr(int('0x'+ent, 16))
        else:
            # they were using a name
            cp = n2cp.get(ent)
            if cp: return unichr(cp)
            else: return match.group()

    entity_re = re.compile(r'&(#?)(x?)(\w+);')
    return entity_re.subn(substitute_entity, string)[0]

def _fetch(url, retry=False):
    headers = { 'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:9.0.1) Gecko/20100101 Firefox/9.0.1' }
    if retry:
        time.sleep(0.5)
    try:
        request = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(request)
    except urllib2.HTTPError as e:
        if e.code == 500:
            return '500 Error'
        if e.code == 404:
            return '404 Error'
        print 'Refetching page ' + url + ' due to ' + str(e.code)
        print e
        return _fetch(url, retry=True)
    except urllib2.URLError as e:
        print 'Refetching page ' + url + ' due to URLError'
        print e
        return _fetch(url, retry=True)
    except BadStatusLine as e:
        print 'Refetching page ' + url + ' due to BadStatusLine'
        print e
        return _fetch(url, retry=True)
    except socket.error as e:
        print 'Refetching page ' + url + ' due to socket.error'
        print e
        return _fetch(url, retry=True)

    return response.read()

def parse_project_for_founders(proj_html):
    project = lxml.html.fromstring(proj_html)

    retobj = { 'founder_name': '',
               'founder_profile': '',
               'founder_location': '',
               'founder_website': '',
               'founder_facebook_link': '',
               'founder_facebook_name': '',
               'start_date': '',
               'end_date': '' }

    deadline_r = project.xpath('.//*[@id="meta"]/p/text()[3]', smart_strings=False)
    for deadline in deadline_r:
        deadline_m = re.search('(?P<startdate>.+) - (?P<enddate>.+)\(', deadline.replace('\n', '')) #need days left
        if deadline_m:
            retobj['start_date'] = deadline_m.groupdict()['startdate']
            retobj['end_date'] = deadline_m.groupdict()['enddate']

    founder_r = project.xpath('.//*[@id="creator-name"]/h5/a', smart_strings=False)
    for founder in founder_r:
        retobj['founder_name'] = founder.text
        retobj['founder_profile'] = BASE_URL + founder.get('href')

    facebook_r = project.xpath('.//*[@id="creator-details"]/ul/li[2]/span[2]/a', smart_strings=False)
    for facebook in facebook_r:
        retobj['founder_facebook_name'] = facebook.text
        retobj['founder_facebook_link'] = facebook.get('href')

    website_r = project.xpath('.//*[@id="creator-details"]/ul/li[3]/a', smart_strings=False)
    for website in website_r:
        retobj['founder_website'] = website.get('href')

    founderloc_r = project.xpath('.//*[@id="creator-name"]/p/span[1]/a', smart_strings=False)
    for founderloc in founderloc_r:
        retobj['founder_location'] = founderloc.text

    return retobj

def parse_projects():
    info = {}
    def fetch_project(url):
        html = _fetch(url)
        info[url] = {}
        more_info = parse_project_for_founders(html)
        info[url].update(more_info)        

    with open('project_url_initial.csv', 'rb') as f:
        reader = csv.reader(f)
        pool = gevent.pool.Pool(THREADS)
        threads = []
        counter = 0
        for proj_url in reader:
            if counter == 2000:
                break
            threads.append(pool.spawn(fetch_project, proj_url[0]))
            counter += 1
        pool.join()

    with open('kickstarter.json', 'w') as f:
        json.dump(info, f, sort_keys=True, indent=4, separators=(',', ': '))

def convert_to_csv():
    with open('kickstarter.json', 'r') as f:
        projects_info = json.load(f)

    with open('kickstarterfounder.csv', 'wb') as f:
        csv_file = csv.writer(f)
        f.write(u'\ufeff'.encode('utf8')) # BOM for opening in excel
        csv_file.writerow([  u'Link to Campaign',
                             u'Founder Name', 
                             u'Founder Location', 
                             u'Founder Facebook URL', 
                             u'Founder Facebook Name',
                             u'Founder Website',
                             u'Founder Kickstarter Bio',
                             u'Start Date', 
                             u'End Date' ])

        for url, info in projects_info.iteritems():
            row = [ decode_htmlentities(url).encode("utf8"),
                    decode_htmlentities(info['founder_name']).encode("utf8"),
                    decode_htmlentities(info['founder_location']).encode("utf8"),
                    decode_htmlentities(info['founder_facebook_link']).encode("utf8"),
                    decode_htmlentities(info['founder_facebook_name']).encode("utf8"),
                    decode_htmlentities(info['founder_website']).encode("utf8"),
                    decode_htmlentities(info['founder_profile']).encode("utf8"),
                    decode_htmlentities(info['start_date']).encode("utf8"),
                    decode_htmlentities(info['end_date']).encode("utf8") ]

            csv_file.writerow(row)

convert_to_csv()