import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pymongo
from prettytable import PrettyTable
from pymongo.errors import ServerSelectionTimeoutError


class ServiceTools:
    """Class for service purpose

    --------
    Methods:
    --------
          get_registers_info():
            Show the list of available registers, count of documents, and the last update date
          clear_results_dir():
            Clean up the folder with saved search results
          clear_console():
            Clear the console output
          check_is_expired():
            Check if a dataset last updated date is older than 2 days ago
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
                self.__dbserver = pymongo.MongoClient(
                    self.__dbstring, serverSelectionTimeoutMS=maxSevSelDelay)
                self.__dbserver.server_info()  # force connection on a request
            except ServerSelectionTimeoutError:
                logging.error(f'{self.__class__.__name__}: Connection error')
                logging.info(
                    f'{self.__class__.__name__}: The application closed')
                print('Connection error')
                print('Quitting...')
                exit()
            else:
                self.__db = self.__dbserver["searchmydata"]
                self.__serviceCol = self.__db['ServiceCollection']
        # if config.json does not exists
        else:
            logging.error(
                f'{self.__class__.__name__}: Config.json is not found')
            logging.info(f'{self.__class__.__name__}: The application closed')
            print('Config.json is not found')
            print('Quitting...')
            exit()

    def get_registers_info(self):
        """Show the list of available registers, count of documents, and the last update date
        """
        registers_info_table = PrettyTable(
            ['#', 'Description', 'Documents count', 'Last modified date'])
        for info in self.__serviceCol.find({}, {'_id': 1, 'Description': 1, 'DocumentsCount': 1,
                                                'LastModifiedDate': 1}).sort([('_id', 1)]):
            registers_info_table.add_row([str(info['_id']), info['Description'], str(
                info['DocumentsCount']), '{:.19}'.format(info['LastModifiedDate'])])
        print(registers_info_table.get_string(title='Registers info'))

    def clear_results_dir(self):
        """Clean up the folder with saved search results
        """
        for filename in os.listdir('results'):
            os.remove('results/' + filename)
        logging.info(f'{self.__class__.__name__}: "Results" folder is cleaned')

    def clear_console(self):
        """Clear the console output
        """
        command = 'clear'
        if os.name in ('nt', 'dos'):  # If Machine is running on Windows, use cls
            command = 'cls'
        os.system(command)

    def check_is_expired(self):
        """Check if a dataset last updated date is older than 2 days ago
        """
        is_expired = False
        expired_time = datetime.now() - timedelta(days=2)
        for record in self.__serviceCol.find():
            last_modified_date = datetime.strptime(
                record['LastModifiedDate'], '%Y-%m-%d %H:%M:%S.%f')
            if last_modified_date < expired_time:
                logging.warning(record['Description'] + ' is out of date')
                is_expired = True
        if is_expired:
            print('Warning! One or more datasets are out of date. Please, refresh!')
