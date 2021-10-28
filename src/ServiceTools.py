import json
import logging
import os
from pathlib import Path

import pymongo
from prettytable import PrettyTable
from pymongo.errors import ServerSelectionTimeoutError


class ServiceTools:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.__configJsonFilePath = Path('config.json')
        # check if config.json exists
        if self.__configJsonFilePath.is_file():
            logging.warning('Config.json is found')
            self.__configJsonFile = open(self.__configJsonFilePath)
            # try to read json
            try:
                self.__configJson = json.loads(self.__configJsonFile.read())
            except ValueError:
                logging.error('Config.json format error')
                logging.info('The application closed')
                print('Config.json format error')
                print('Quitting...')
                exit()
            # read db connection string
            try:
                self.__dbstring = self.__configJson['dbstring']
            except KeyError:
                logging.error('"dbstring" key is not found in Config.json')
                logging.info('The application closed')
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
                logging.error('Connection error')
                logging.info('The application closed')
                print('Connection error')
                print('Quitting...')
                exit()
            else:
                self.__db = self.__dbserver["searchmydata"]
                self.__serviceCol = self.__db['ServiceCollection']
        # if config.json does not exists
        else:
            logging.error('Config.json is not found')
            logging.info('The application closed')
            print('Config.json is not found')
            print('Quitting...')
            exit()

    def getRegistersInfo(self):
        registersInfoTable = PrettyTable(
            ['#', 'Description', 'Documents count', 'Last modified date'])
        for info in self.__serviceCol.find({}, {'_id': 1, 'Description': 1, 'DocumentsCount': 1, 'LastModifiedDate': 1}).sort([('_id', 1)]):
            registersInfoTable.add_row([str(info['_id']), info['Description'], str(
                info['DocumentsCount']), '{:.19}'.format(info['LastModifiedDate'])])
        print(registersInfoTable.get_string(title='Registers info'))

    def clearResultsDir(self):
        for filename in os.listdir('results'):
            os.remove('results/'+filename)
        logging.info('"Results" folder is cleaned')

    def clearConsole(self):
        command = 'clear'
        if os.name in ('nt', 'dos'):  # If Machine is running on Windows, use cls
            command = 'cls'
        os.system(command)