import json
import logging
from pathlib import Path
from functools import wraps
from datetime import datetime

import pymongo
from pymongo.errors import ServerSelectionTimeoutError


class Dataset:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.__configJsonFilePath = Path('config.json')
        # check if config.json exists
        if self.__configJsonFilePath.is_file():
            logging.warning(f'{self.__class__.__name__}: Config.json is found')
            self.__configJsonFile = open(self.__configJsonFilePath)
            # try to read json
            try:
                self.__configJson = json.loads(self.__configJsonFile.read())
            except ValueError:
                logging.error(
                    f'{self.__class__.__name__}: Config.json format error')
                logging.info(
                    f'{self.__class__.__name__}: The application closed')
                print('Config.json format error')
                print('Quitting...')
                exit()
            # read db connection string
            try:
                self.__dbstring = self.__configJson['dbstring']
            except KeyError:
                logging.error(
                    f'{self.__class__.__name__}: "dbstring" key is not found in Config.json')
                logging.info(
                    f'{self.__class__.__name__}: The application closed')
                print('Config.json format error')
                print('Quitting...')
                exit()
            # try to connect
            try:
                # Set server Selection Timeout in ms. The default value is 30s.
                maxSevSelDelay = 3
                self.dbserver = pymongo.MongoClient(
                    self.__dbstring, serverSelectionTimeoutMS=maxSevSelDelay)
                self.dbserver.server_info()  # force connection on a request
            except ServerSelectionTimeoutError:
                logging.error(f'{self.__class__.__name__}: Connection error')
                logging.info(
                    f'{self.__class__.__name__}: The application closed')
                print('Connection error')
                print('Quitting...')
                exit()
            else:
                self.db = self.dbserver['searchmydata']
                self.serviceCol = self.db['ServiceCollection']
        # if config.json does not exists
        else:
            logging.error(
                f'{self.__class__.__name__}: Config.json is not found')
            logging.info(f'{self.__class__.__name__}: The application closed')
            print('Config.json is not found')
            print('Quitting...')
            exit()

    def get_dataset(self):
        pass

    def save_dataset(self):
        pass

    def clear_collection(self):
        pass

    def __create_service_json(self):
        pass

    def __update_service_json(self):
        pass

    def update_metadata(self):
        pass

    def delete_collection_index(self):
        pass

    def create_collection_index(self):
        pass

    def search_into_collection(self):
        pass

    def setup_dataset(self):
        pass

    def measure_execution_time(func):
        @wraps(func)
        def log_time(*args, **kwargs):
            start_time = datetime.now()
            try:
                return func(*args, **kwargs)
            finally:
                end_time = datetime.now()
                logging.info(
                    f'Total execution time {args[0].__class__.__name__}.{func.__name__}: {end_time - start_time}')
        return log_time
