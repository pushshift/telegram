from telethon import TelegramClient, events, sync
import ujson as json
import time
import sys
from telethon import functions, types
import logging
import urllib.parse
from requests_html import HTMLSession
from bs4 import BeautifulSoup
from urlparse import urlparse


TELEGAGO_BASE_URL = 'https://cse.google.com/cse?q=+&cx=006368593537057042503:efxu7xprihg#gsc.tab=0&gsc.ref=more%3Apublic&gsc.q='
LYZEM_BASE_URL = 'https://lyzem.com/search?f=channels&l=%3Aen&per-page=100&q='


# extracts the html from a URL using the requests_html library (supports JS)
def extract_html(url, javascript_enabled=False):
    print("Requesting HTML for " + url)
    session = HTMLSession()
    response = session.get(url)
    if javascript_enabled:
        response.html.render()
        source_html = response.html.html
        return source_html
    else:
        return response.html.html


# method to parse the HTML from the Lyzem page
def parse_lyzem_page(html):
    print(html)
    soup = BeautifulSoup(html, "lxml")
    links = soup.find_all('li', attrs={'class', 'result'})
    channels = []
    for link in links:
        try:
            print(link['data-url'])
            print(type(link['data-url']))
            element_classes = link['class']
            # if they have this element this means the result is an advertisement
            # we dont want these
            if 'ann' in element_classes:
                continue
            path_url = link['data-url']
            channel_name = path_url.split('?')[0].split('/')[-1]
            if channel_name not in channels:
                channels.append(channel_name)
        except KeyError:
            continue
    return channels

def search_channels_lyzem(query, limit=100):
    all_channels = []
    initial_request_url = LYZEM_BASE_URL + urllib.parse.quote(query)
    logging.debug("Lyzem request url {}".format(initial_request_url))

    # extract channels from initial page
    source_html = extract_html(initial_request_url, javascript_enabled=False)
    print(source_html)
    page_channels = parse_lyzem_page(source_html)
    all_channels = page_channels
    
    # if reached limit return the channels
    if len(all_channels)>=limit:
        return all_channels[:limit]

    # otherwise we need to go to next pages
    # find the number of pages from the html
    soup = BeautifulSoup(source_html, "lxml")
    cursor_div = soup.find_all('nav', {'class': 'pages'})
    num_pages = len(cursor_div[0].find_all('li'))
    
    # then iterate over all pages to extract all channels
    i=1
    for i in range(num_pages):
        request_url = initial_request_url + '&p=' + str(i+1)
        logging.debug("Lyzem request url {}".format(request_url))
        source_html = extract_html(request_url, javascript_enabled=False)
        page_channels = parse_lyzem_page(source_html)
        for channel in page_channels:
            if channel not in all_channels:
                all_channels.append(channel)
        i+=1
        if len(all_channels)>=limit:
            return all_channels[:limit]
    return all_channels


# method to parse the HTML from the telegago page
def parse_telegago_page(html):
    soup = BeautifulSoup(html, "lxml")
    links = soup.find_all('a', attrs={'class', 'gs-title'})

    channels = []

    for link in links:
        try:
            print(link['href'])
            print(type(link['href']))

            path_url = urlparse(link['href']).path
            if path_url.startswith('/s/'):
                if path_url.count('/')==2:
                    channel_name = path_url.split('/')[-1]
                else:
                    channel_name = path_url.split('/')[-2]
            else:
                channel_name = path_url.split('/')[1]
            if channel_name not in channels:
                channels.append(channel_name)
        except KeyError:
            continue
    return channels

def search_channels_telegago(query, limit=100):
    all_channels = []
    initial_request_url = TELEGAGO_BASE_URL + urllib.parse.quote(query)
    logging.debug("Telegago request url {}".format(initial_request_url))

    # extract channels from initial page
    source_html = extract_html(initial_request_url, javascript_enabled=True)
    page_channels = parse_telegago_page(source_html)
    all_channels = page_channels
    
    # if reached limit return the channels
    if len(all_channels)>=limit:
        return all_channels[:limit]

    # otherwise we need to go to next pages
    # find the number of pages from the html
    soup = BeautifulSoup(source_html, "lxml")
    cursor_div = soup.find_all('div', {'class': 'gsc-cursor'})
    num_pages = len(cursor_div[0].find_all('div'))
    
    # then iterate over all pages to extract all channels
    i=1
    for i in range(num_pages):
        request_url = initial_request_url + '&gsc.page=' + str(i+1)
        logging.debug("Telegago request url {}".format(request_url))
        source_html = extract_html(request_url, javascript_enabled=True)
        page_channels = parse_telegago_page(source_html)
        for channel in page_channels:
            if channel not in all_channels:
                all_channels.append(channel)
        i+=1
        if len(all_channels)>=limit:
            return all_channels[:limit]
    return all_channels



# example: get_channel_info(client, 'followchris')
# parameters:
# client: the TelegramClient from Telethon
# channel: the channel_id or the channel_username
# output: Dictionary with the channel info. 
# In case that the channel parameter is not valid we return an error dict
def get_channel_info(client, channel):
    try:
        return client(functions.channels.GetFullChannelRequest(channel=channel)).to_json()
    except ValueError as e:
        return {'error': str(e)}

# method to get participants from channel (we might not have priviledges to get this data)
# getting some errors about permissions
def get_channel_users(client, channel, limit=1000):
    try:
        participants = client.get_participants(channel, limit)
        return participants
    except ValueError as e:
        return {'error': str(e)}