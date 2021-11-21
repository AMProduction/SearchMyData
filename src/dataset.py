import json
import logging
from pathlib import Path
from functools import wraps
from datetime import datetime

import pymongo
from pymongo.errors import ServerSelectionTimeoutError


class Dataset:
    """"Base parent class for all datasets

    --------
    Methods:
    --------
        get_dataset():
            Get the link to the dataset. Return the link to the dataset's source file
        save_dataset():
            Save the dataset into the Database. Input parameter - the link to the dataset's source file.
        clear_collection():
            Purge the collection
        __create_service_json():
            Create and save a JSON with service information about a dataset
        __update_service_json():
            Update and save a JSON with service information about a dataset
        update_metadata():
            Call __create_service_json() if a dataset is first time saved. Or call __update_service_json() if a dataset refreshed
        delete_collection_index():
            Drop a database full-text search index
        create_collection_index():
            Create a database full-text search index
        search_into_collection():
            Search, show and save search results
        setup_dataset():
            A sequence of class methods to setup a dataset
        measure_execution_time():
            A service function / a decorator to measure up execution time
        is_collection_exists():
            Check if a collection exists. Input parameter - a collection name
    """

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

    def __get_dataset(self):
        """Get the link to the dataset.
        Return the link to the dataset's source file
        """
        pass

    def __save_dataset(self):
        """Save the dataset into the Database.
        Input parameter - the link to the dataset's source file.
        """
        pass

    def __clear_collection(self):
        """Purge the collection
        """
        pass

    def __create_service_json(self):
        """Create and save a JSON with service information about a dataset
        """
        pass

    def __update_service_json(self):
        """Update and save a JSON with service information about a dataset
        """
        pass

    def __update_metadata(self):
        """Call __create_service_json() if a dataset is first time saved. Or call __update_service_json() if a dataset refreshed
        """
        pass

    def __delete_collection_index(self):
        """Drop a database full-text search index
        """
        pass

    def __create_collection_index(self):
        """Create a database full-text search index
        """
        pass

    def search_into_collection(self):
        """Search, show and save search results
        """
        pass

    def setup_dataset(self):
        """A sequence of class methods to setup a dataset
        """
        pass

    def measure_execution_time(func):
        """A service function / a decorator to measure up execution time
        """
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

    def is_collection_exists(self, collection_name):
        """Check if a collection exists.

        :param collection_name: str/a collection name
        :return: True - if a collection exists; False - if not
        """
        collections_list = self.db.list_collection_names()
        if collection_name in collections_list:
            return True
        else:
            return False
