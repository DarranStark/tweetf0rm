#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
from random import randint

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

import argparse, json, os, sys, time

sys.path.append("../..")
from tweetf0rm.twitterapi.streaming_api import Streamer
from tweetf0rm.utils import full_stack


def track_keywords(apikeys, keywords, output):
    stream = Streamer(apikeys=apikeys, output=os.path.abspath(output))

    try:
        # stream.statuses.filter(track=keywords)
        stream.statuses.filter(track=keywords, language="ar")
    except:
        raise
    finally:
        stream.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--apikeys',
                        help="config file for twitter api keys (json format); twitter requires you to have an account to crawl;",
                        required=True)
    # parser.add_argument('-c', '--crawler', help="the crawler identifier; you can have multiple crawler accounts set in the apikeys.json; pick one", required = True)
    parser.add_argument('-k', '--keywords', help="define the location of the keywords.json file", required=True)
    parser.add_argument('-o', '--output',
                        help="define the location of the output; crawled tweets are organized by timestamp in this folder;",
                        required=True)
    args = parser.parse_args()

    with open(os.path.abspath(args.apikeys), 'rb') as apikeys_f, open(os.path.abspath(args.keywords), 'rb') as keywords_f:
        apikeys_config = json.load(apikeys_f)
        # apikeys = apikeys_config.get(args.crawler, None)
        apikeys = apikeys_config.get(apikeys_config.keys()[randint(0, len(apikeys_config) - 1)], None)

        if not apikeys:
            raise Exception("what's the point? Make sure you have all the api keys set in the config file...")

        keywords = json.load(keywords_f)

        keywords = keywords['keywords']

        logger.info('tracking %d keywords' % (len(keywords)))

        while True:
            try:
                apikeys = apikeys_config.get(apikeys_config.keys()[randint(0, len(apikeys_config) - 1)], None)
                track_keywords(apikeys, keywords, args.output)
            except KeyboardInterrupt:
                logger.error('You pressed Ctrl+C!')
                sys.exit(0)
            except:
                logger.error(full_stack())
                logger.info('failed, retry')
                time.sleep(10)
