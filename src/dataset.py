import json
import logging
from pathlib import Path

import pymongo
from pymongo.errors import ServerSelectionTimeoutError


class Dataset:

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
                self.dbserver = pymongo.MongoClient(
                    self.__dbstring, serverSelectionTimeoutMS=maxSevSelDelay)
                self.dbserver.server_info()  # force connection on a request
            except ServerSelectionTimeoutError:
                logging.error('Connection error')
                logging.info('The application closed')
                print('Connection error')
                print('Quitting...')
                exit()
            else:
                self.db = self.dbserver['searchmydata']
                self.serviceCol = self.db['ServiceCollection']
        # if config.json does not exists
        else:
            logging.error('Config.json is not found')
            logging.info('The application closed')
            print('Config.json is not found')
            print('Quitting...')
            exit()

    def getDataset(self):
        pass

    def saveDataset(self):
        pass

    def clearCollection(self):
        pass

    def __createServiceJson(self):
        pass

    def __updateServiceJson(self):
        pass

    def updateMetadata(self):
        pass

    def deleteCollectionIndex(self):
        pass

    def createCollectionIndex(self):
        pass

    def searchIntoCollection(self):
        pass

    def setupDataset(self):
        pass