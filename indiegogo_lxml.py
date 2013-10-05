#!/usr/bin/python
# -*- coding: utf-8 -*-

#ISSUE
#Caching -> all to memory? Skip disk? RAMDISK?
#Doesn't show which year it ended
#Conditions for money

#TODO: perhaps parse Individual data (perhaps in different spreadsheet) <----- ***this too
#TODO: SCRAPE INDIEGOGO WEEKLY FOR EVERYTHING FOR NEXT 2/3 WEEKS, 24th July
#TODO: individual contributions

#TRY TO SCRAPE KICKSTARTER
#BUILD DATABASE MODEL
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
from BeautifulSoup import BeautifulSoup
from htmlentitydefs import name2codepoint as n2cp
from httplib import BadStatusLine
import lxml.html
from lxml.cssselect import CSSSelector
INDIEGOGO_BASE_URL = 'http://www.indiegogo.com'
THREADS = 50

def decode_htmlentities(string):
    """
    Decode HTML entities–hex, decimal, or named–in a string
    @see http://snippets.dzone.com/posts/show/4569

    >>> u = u'E tu vivrai nel terrore - L&#x27;aldil&#xE0; (1981)'
    >>> print decode_htmlentities(u).encode('UTF-8')
    E tu vivrai nel terrore - L'aldilà (1981)
    >>> print decode_htmlentities("l&#39;eau")
    l'eau
    >>> print decode_htmlentities("foo &lt; bar")                
    foo < bar
    """
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

def create_dir_if_not_exist(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
def parse_project_summary(proj, page_num):
    '''
    Parses the summary html to extract needed info, 
    then returns a dictionary of it with the project 
    link as the key
    '''    
    retval = {}    
    campaign_title_r = proj.xpath('.//a[contains(@class, "name")]', smart_strings=False)
    if campaign_title_r:
        campaign_link = INDIEGOGO_BASE_URL + campaign_title_r[0].get('href')
        retval[campaign_link] = {}
        retval[campaign_link]['page_num'] = page_num
        retval[campaign_link]['campaign_title'] = ''
        retval[campaign_link]['category'] = ''      
        retval[campaign_link]['campaign_eta'] = ''
        retval[campaign_link]['num_funders'] = ''
        retval[campaign_link]['amount_raised'] = ''
        retval[campaign_link]['currency_code'] = ''

        if campaign_title_r[0].text:
            retval[campaign_link]['campaign_title'] = campaign_title_r[0].text

        category_r = proj.xpath('.//div[2]/text()', smart_strings=False)
        for category in category_r:
            retval[campaign_link]['category'] = category.strip()

        eta_r = proj.xpath('.//*[@id="time_left_number"]/text()', smart_strings=False)
        for eta in eta_r:
            retval[campaign_link]['campaign_eta'] = eta

        funders_r = proj.xpath('.//*[@id="funders"]/text()', smart_strings=False)
        if funders_r:
            retval[campaign_link]['num_funders'] = funders_r[0].strip()

        raised_r = proj.xpath('.//*[@id="project-stats-funding-amt"]/span[1]/text()', smart_strings=False)
        for raised in raised_r:
            retval[campaign_link]['amount_raised'] = re.sub('[^\d]', '', raised)

        currency_r = proj.xpath('.//*[@id="currency_code"]/text()', smart_strings=False)
        for currency in currency_r:
            retval[campaign_link]['currency_code'] = currency

    return retval

def parse_project(proj_html):
    project = lxml.html.fromstring(proj_html)

    retobj = { 'location': '',
               'start_date': '',
               'end_date': '',
               'target_amount': '',
               'team_info': [],
               'perk_info': [] }

    locations_r = project.xpath('//span[contains(@class, "location")]/a/text()', smart_strings=False)
    for location in locations_r:
        retobj['location'] = location

    deadline_r = project.xpath('//p[contains(@class, "funding-info")]/text()', smart_strings=False)
    for deadline in deadline_r:
        deadline_m = re.search('Funding duration: (?P<startdate>.+) - (?P<enddate>.+)\.', deadline)
        if deadline_m:
            retobj['start_date'] = deadline_m.groupdict()['startdate']
            retobj['end_date'] = deadline_m.groupdict()['enddate']

    target_selector = CSSSelector('.money-raised.goal')
    for goal in target_selector(project):
        for amount in goal.xpath('.//text()', smart_strings=False):
            target_m = re.match('Raised of (?P<target>.+) Goal', amount.strip())
            if target_m:
                retobj['target_amount'] = re.sub('[^\d]', '', target_m.groupdict()['target'])

    teaminfo_selector = CSSSelector('.name.bold')
    for member in teaminfo_selector(project):
        memberobj = (member.text, INDIEGOGO_BASE_URL + member.get('href'))
        retobj['team_info'].append(memberobj)

    perk_selector = CSSSelector('.perk.rounded.shadow')
    for perk in perk_selector(project):
        perk_amount = ''
        perk_claimed = ''
        max_claimed = 'None'
        for amount in perk.xpath('.//div[contains(@class, "amount")]/text()', smart_strings=False):
            perk_amount = re.sub('[^\d]', '', amount.strip())

        for claims in perk.xpath('.//p[contains(@class, "claimed")]/text()'):
            if 'out of' in claims: #has max number of claims
                claimed_m = re.match('(?P<claimed>.+) out of (?P<max>.+) claimed', claims.strip())
                if claimed_m:
                    perk_claimed = claimed_m.groupdict()['claimed']
                    max_claimed = claimed_m.groupdict()['max']
            else: #unlimited number of claims
                claimed_m = re.match('(?P<claimed>.+) claimed', claims.strip())
                if claimed_m:
                    perk_claimed = claimed_m.groupdict()['claimed']
        perk_obj = (perk_amount, perk_claimed, max_claimed)
        retobj['perk_info'].append(perk_obj)

    return retobj

# def parse_individual(ind_html):
#     soup = BeautifulSoup(ind_html, convertEntities=BeautifulSoup.HTML_ENTITIES)
#     info_raw = soup.find(attrs={'class': 'info-boxes'}).findAll('h1')

#     location_parent_raw = soup.find(text='Location: ')
#     gender_parent_raw = soup.find(text='Gender: ')

#     if location_parent_raw:
#         location_raw = location_parent_raw.parent.findNext(attrs={'class': 'notranslate'})
#     else:
#         location_raw = None

#     if gender_parent_raw:
#         gender_raw = gender_parent_raw.parent.findNext(attrs={'class': 'notranslate'})
#     else:
#         gender_raw = None

#     retobj = { 'referrals': '',
#                'contributions_made': '',
#                'campaigns_on': '',
#                'comments_written': '',
#                'location': '',
#                'gender': '' }

#     if info_raw:        
#         retobj['referrals'] = info_raw[0].text
#         retobj['contributions_made'] = info_raw[1].text
#         retobj['campaigns_on'] = info_raw[2].text
#         retobj['comments_written'] = info_raw[3].text

#     if location_raw:
#         retobj['location'] = location_raw.text
        
#     if gender_raw:
#         retobj['gender'] = gender_raw.text

#     return retobj
#     #campaigns_on_raw = ''
#     '''Perhaps think about tradeoff of multithreading, and don't write stuff to disk'''

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

def create_index_cache(cat, start, end):
    """
    PHASE 1A:
    Creates a snapshot of the index by downloading 
    them all into a local cache directory.

    Input:  None
    Output: {page_num}.html in the cache_path directory
    """
    print 'Creating index cache'

    if cat != 'All':
        BASE_INDEX_URL = 'http://www.indiegogo.com/projects?filter_category={category}&filter_country=&pg_num='.format(category=cat)
    else:
        BASE_INDEX_URL = 'http://www.indiegogo.com/projects?&pg_num='

    def fetch_index(page_num):
        #print 'Caching ' + str(page_num)
        html = _fetch(BASE_INDEX_URL + str(page_num))
        filename = str(page_num) + '.html'
        with open(os.path.join(cache_path, filename), 'w') as f:
            f.write(html)

    pool = gevent.pool.Pool(THREADS)
    threads = []
    for i in range(start, end + 1):
        threads.append(pool.spawn(fetch_index, i))
    #gevent.joinall(threads)
    pool.join()
    return

def parse_index(start, end):
    """
    PHASE 1B
    Input:  None, but requires cache_path directory to exist
    Output: phase1_outfile
    """    
    print 'Parsing index'
    info = {}
    for i in range(start, end + 1):
        #print 'Parsing index page ' + str(i)
        filename = str(i) + '.html'
        with open(os.path.join(cache_path, filename), 'r') as page:
            lxml_tree = lxml.html.parse(page)
            project_selector = CSSSelector('.fl.badge.rounded.shadow')
            for project in project_selector(lxml_tree):
                page_info = parse_project_summary(project, str(i))
                info.update(page_info)

    with open(phase1_outfile, 'w') as f:
        json.dump(info, f, sort_keys=True, indent=4, separators=(',', ': '))

def create_project_cache():
    """
    PHASE 3    
    Creates a cache of all the individual projects in {category}/proj
    """
    print 'Creating project cache'
    with open(phase1_outfile, 'r') as f:
        projects_info = json.load(f)

    def fetch_project(url):
        #print 'Fetching project at ' + url
        filename = hashlib.md5(url).hexdigest()        
        html = _fetch(url)
        #print 'Caching ' + filename
        projects_info[url]['cache_file_name'] = filename
        with open(os.path.join(proj_cache_path, filename), 'w') as f:
            f.write(html)        

    pool = gevent.pool.Pool(THREADS)
    threads = []
    for proj_url, info in projects_info.iteritems():
        threads.append(pool.spawn(fetch_project, proj_url))
    pool.join()
    with open(phase1_outfile, 'w') as f:
         json.dump(projects_info, f, sort_keys=True, indent=4, separators=(',', ': '))

def parse_projects():
    """
    Parses the individual cached projects
    PHASE 4
    """
    print 'Parsing project cache'
    with open(phase1_outfile, 'r') as f:
        projects_info = json.load(f)

    for url, info in projects_info.iteritems():
        #print "Parsing: " + info['cache_file_name']
        with open(os.path.join(proj_cache_path, info['cache_file_name']), 'r') as cache:
            more_info = parse_project(cache.read())
            info.update(more_info)

    with open(phase2_outfile, 'w') as f:
        json.dump(projects_info, f, sort_keys=True, indent=4, separators=(',', ': '))

# def parse_all_individuals():
#     """
#     PHASE 5 
#     """    
#     with open('test.json', 'r') as f:
#         projects_info = json.load(f)

#     all_info = {}

#     for projects, data in projects_info.iteritems():
#         for member in data['team_info']:            
#             url = member[1]
#             print url
#             individual_obj = {url: { 'name': member[0]}}
#             individual_data = parse_individual(_fetch(url))
#             individual_obj[url].update(individual_data)
#             all_info.update(individual_obj)

#     individual_outfile = 'individual.txt'
#     with open(individual_outfile, 'w') as f:
#         json.dump(all_info, f, sort_keys=True, indent=4, separators=(',', ': '))
    # def fetch_project(url):
    #     #print 'Fetching project at ' + url
    #     filename = hashlib.md5(url).hexdigest()        
    #     html = _fetch(url)
    #     #print 'Caching ' + filename
    #     projects_info[url]['cache_file_name'] = filename
    #     with open(os.path.join(proj_cache_path, filename), 'w') as f:
    #         f.write(html)        

    # pool = gevent.pool.Pool(THREADS)
    # threads = []
    # for proj_url, info in projects_info.iteritems():
    #     threads.append(pool.spawn(fetch_project, proj_url))
    # pool.join()
    # with open(phase1_outfile, 'w') as f:
    #      json.dump(projects_info, f, sort_keys=True, indent=4, separators=(',', ': '))

def convert_to_csv():
    with open(phase2_outfile, 'r') as f:
        projects_info = json.load(f)

    with open(phase2_outfile + '.csv', 'wb') as f:
        csv_file = csv.writer(f)
        f.write(u'\ufeff'.encode('utf8')) # BOM for opening in excel
        csv_file.writerow([u'Category', 
                    u'Campaign Name', 
                    u'Link to Campaign',
                    u'Amount Raised', 
                    u'Target Amount', 
                    u'Currency Code', 
                    u'Number of Funders', 
                    u'ETA (days)', 
                    u'Deadline', 
                    u'Location',
                    u'Team Member(s)',
                    u'Perk(s)'])
        for url, info in projects_info.iteritems():
            print url
            row = [ decode_htmlentities(info['category']).encode("utf8"),
                    decode_htmlentities(info['campaign_title']).encode("utf8"),
                    decode_htmlentities(url).encode("utf8"),
                    decode_htmlentities(info['amount_raised']).encode("utf8"),
                    decode_htmlentities(info['target_amount']).encode("utf8"),
                    decode_htmlentities(info['currency_code']).encode("utf8"),
                    decode_htmlentities(info['num_funders']).encode("utf8"),
                    decode_htmlentities(info['campaign_eta']).encode("utf8"),
                    decode_htmlentities(info['deadline']).encode("utf8"),
                    decode_htmlentities(info['location']).encode("utf8") ]

            for person in info['team_info']:
                row.append(decode_htmlentities(person[0]).encode("utf8"))
                row.append(decode_htmlentities(person[1]).encode("utf8"))
            
            csv_file.writerow(row)

#print parse_individual(_fetch('http://www.indiegogo.com/individuals/3023401'))
#parse_all_individuals()

if __name__ == '__main__':
    """
    Phase 1: cache the index
    Phase 2: parse index, flush to JSON
    Phase 3: cache individual project, update JSON
    Phase 4: parse individual project cache, update JSON
    """

    CATEGORIES = [ 'Art',
                   'Comic',
                   'Dance',
                   'Design',
                   'Fashion',
                   'Film',
                   'Gaming',
                   'Music',
                   'Photography',
                   'Theatre',
                   'Transmedia',
                   'Video+/+Web',
                   'Writing',
                   'Animals',
                   'Community',
                   'Education',
                   'Environment',
                   'Health',
                   'Politics',
                   'Religion',
                   'NONPROFIT<TODO>',
                   'Food',
                   'SmallBiz',
                   'Sports',
                   'Technology' ]

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--category',
                        action='store', choices=['SmallBiz', 'Food', 'Sports', 'Technology', 'All'], required=True,
                        help='Scrapes the selected category')
    parser.add_argument('-s', '--start',
                        action='store', type=int, required=True,
                        help='First page to start scraping from')
    parser.add_argument('-e', '--end',
                        action='store', type=int, required=True,
                        help='Last page to scrape from')


    args = parser.parse_args()

    if args.category == 'SmallBiz':
        category = 'Small+Business'
    else:
        category = args.category


    global cache_path, proj_cache_path, phase1_outfile, phase2_outfile
    cache_path = './cache/' + category
    proj_cache_path = cache_path + '/proj'
    create_dir_if_not_exist(cache_path)
    create_dir_if_not_exist(proj_cache_path)
    timestamp = time.strftime('%Y-%m-%d_%I.%M%p', time.localtime())
    phase1_outfile = category + '_phase1.txt'
    #phase2_outfile = category + '_final_12am_' + timestamp + '.txt'
    phase2_outfile = 'output_test.json'
    '''START'''
    START = args.start
    END = args.end
    create_index_cache(category, START, END)
    parse_index(START, END)
    create_project_cache()
    parse_projects()
    convert_to_csv()