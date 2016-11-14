#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
stream.py: 

KeywordsStreamer: straightforward class that tracks a list of keywords; most of the jobs are done by TwythonStreamer; the only thing this is just attach a WriteToHandler so results will be saved

'''

import logging

logger = logging.getLogger(__name__)

from twython import TwythonStreamer
from tweetf0rm.exceptions import MissingArgs
import os, copy, datetime, json


class Streamer(TwythonStreamer):
    def __init__(self, *args, **kwargs):
        """
        Constructor with apikeys, and output folder

        * apikeys: apikeys
        """
        logger.info(kwargs)
        import copy

        apikeys = copy.copy(kwargs.pop('apikeys', None))
        output = copy.copy(kwargs.pop('output', None))

        if not apikeys:
            raise MissingArgs('apikeys is missing')
        if not output:
            raise MissingArgs('output is missing')

        self.apikeys = copy.copy(apikeys)  # keep a copy
        self.output = open(output, 'a')
        self.counter = 0

        kwargs.update(apikeys)

        super(Streamer, self).__init__(*args, **kwargs)

    def on_success(self, data):
        if 'text' in data:
            self.counter += 1
            if self.counter % 1000 == 0:
                logger.info("received: %d" % self.counter)
            # logger.debug(data['text'].encode('utf-8'))
            # self.write_to_handler.append(json.dumps(data))
            json.dump(data, self.output)
            self.output.write('\n')

    def on_error(self, status_code, data):
        logger.warn(status_code)

    def close(self):
        self.disconnect()
        # self.write_to_handler.close()
        self.output.close()
