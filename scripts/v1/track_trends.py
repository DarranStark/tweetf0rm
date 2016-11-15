#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from random import randint

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

import argparse, json, os, sys, time

sys.path.append("../..")
from tweetf0rm.utils import full_stack
from tweetf0rm.twitterapi.streaming_api import Streamer


def track_keywords(apikeys, keywords, output, language):
    stream = Streamer(apikeys=apikeys, output=os.path.abspath(output))

    try:
        # stream.statuses.filter(track=keywords)
        if language is None:
            stream.statuses.filter(track=keywords)
        else:
            stream.statuses.filter(track=keywords, language=language)
    except:
        raise
    finally:
        stream.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--apikeys',
                        help="config file for twitter api keys (json format); twitter requires you to have an account to crawl;",
                        required=True)
    parser.add_argument('-o', '--output',
                        help="define the location of the output; crawled tweets are organized by timestamp in this folder;",
                        required=True)
    list_of_choices = ["ar"]
    parser.add_argument('-l', '--language',
                        help="",
                        required=False,
                        choices=list_of_choices)
    args = parser.parse_args()

    with open(os.path.abspath(args.apikeys), 'rb') as apikeys_f:
        apikeys_config = json.load(apikeys_f)
        # apikeys = apikeys_config.get(args.crawler, None)
        apikeys = apikeys_config.get(apikeys_config.keys()[randint(0, len(apikeys_config) - 1)], None)

        if not apikeys:
            raise Exception("what's the point? Make sure you have all the api keys set in the config file...")
        language = args.language
        while True:
            from tweetf0rm.twitterapi.twitter_api import TwitterAPI

            twitter_api = TwitterAPI(apikeys=apikeys)

            keywords = []
            if language == 'ar':
                # Algiers 1253079  ==> town
                # Algeria 23424740 ==> country
                town_trend = twitter_api.get_place_trends(id="1253079")
                country_trend = twitter_api.get_place_trends(id="23424740")
                print(json.dumps(town_trend))
                print(json.dumps(country_trend))
                for trend in town_trend[0]['trends']:
                    keywords.append(trend['name'])
                for trend in country_trend[0]['trends']:
                    keywords.append(trend['name'])
            if language is None:
                worldwide_trend = twitter_api.get_place_trends(id="1")
                print(json.dumps(worldwide_trend))
                for trend in worldwide_trend[0]['trends']:
                    keywords.append(trend['name'])
            keywords_set = set(keywords)
            keywords = list(keywords_set)

            print("keywords: ")
            print(json.dumps(keywords))

            try:
                apikeys = apikeys_config.get(apikeys_config.keys()[randint(0, len(apikeys_config) - 1)], None)
                track_keywords(apikeys, keywords, args.output, language)
            except KeyboardInterrupt:
                logger.error('You pressed Ctrl+C!')
                sys.exit(0)
            except:
                logger.error(full_stack())
                logger.info('failed, retry')
                time.sleep(10)
