#!/usr/bin/python
# -*- coding: utf8 -*-
import sys
import logging.config

reload(sys)
sys.setdefaultencoding("utf-8")


__author__ = 'caoweidong'


class Logger(object):
    def __init__(self):
        self.file = 'resource/logger.conf'

    def get_logger(self, name="root"):
        """
        获得Logger对象
        """
        logging.config.fileConfig(self.file)
        return logging.getLogger(name)

logger = Logger().get_logger()
