import json
import pymongo
import logging
from pathlib import Path

from pymongo.errors import ServerSelectionTimeoutError

class DBTools:
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.__configJsonFilePath = Path('config.json')
        #check if config.json exists
        if self.__configJsonFilePath.is_file():
            logging.warning('Config.json is found')
            self.__configJsonFile = open(self.__configJsonFilePath)
            #try to read json
            try:
                self.__configJson = json.loads(self.__configJsonFile.read())
            except ValueError:
                logging.error('Config.json format error')
                logging.info('The application closed')
                print('Config.json format error')
                print('Quitting...')
                exit()
            #read db connection string
            try:
                self.__dbstring = self.__configJson['dbstring']
            except KeyError:
                logging.error('"dbstring" key is not found in Config.json')
                logging.info('The application closed')
                print('Config.json format error')
                print('Quitting...')
                exit()
            #try to connect
            try:
                maxSevSelDelay = 3 #Set server Selection Timeout in ms. The default value is 30s.
                self.__dbserver = pymongo.MongoClient(self.__dbstring, serverSelectionTimeoutMS = maxSevSelDelay)
                self.__dbserver.server_info() #force connection on a request
            except ServerSelectionTimeoutError:
                logging.error('Connection error')
                logging.info('The application closed')
                print('Connection error')
                print('Quitting...')
                exit()
            else:
                self.__db = self.__dbserver["searchmydata"]
        #if config.json does not exists
        else:
            logging.error('Config.json is not found')
            logging.info('The application closed')
            print('Config.json is not found')
            print('Quitting...')
            exit()

    def saveMissingPersonsRegister(self, json):
        missingPersonsCol = self.__db['MissingPersons']
        countDeletedDocuments = missingPersonsCol.delete_many({})
        logging.warning('%s documents deleted. The missing persons collection is empty.', str(countDeletedDocuments.deleted_count))
        if ('full_text' in missingPersonsCol.index_information()):
            missingPersonsCol.drop_index('full_text')
            logging.warning('Missing persons Text index deleted')
        missingPersonsCol.insert(json)
        logging.info('Missing persons dataset was saved into the database')
        missingPersonsCol.create_index([('FIRST_NAME_U','text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text'), ('FIRST_NAME_R', 'text'), ('LAST_NAME_R', 'text'), ('MIDDLE_NAME_R', 'text'), ('FIRST_NAME_E', 'text'), ('LAST_NAME_E', 'text'), ('MIDDLE_NAME_E','text')], name = 'full_text')
        logging.info('Missing persons Text Index created')
        
    def saveWantedPersonsRegister(self, json):
        wantedPersonsCol = self.__db['WantedPersons']
        countDeletedDocuments = wantedPersonsCol.delete_many({})
        logging.warning('%s documents deleted. The wanted persons collection is empty.', str(countDeletedDocuments.deleted_count))
        if ('full_text' in wantedPersonsCol.index_information()):
            wantedPersonsCol.drop_index('full_text')
            logging.warning('WantedPersons Text index deleted')
        wantedPersonsCol.insert(json)
        logging.info('Wanted persons dataset was saved into the database')
        wantedPersonsCol.create_index([('FIRST_NAME_U','text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text'), ('FIRST_NAME_R', 'text'), ('LAST_NAME_R', 'text'), ('MIDDLE_NAME_R', 'text'), ('FIRST_NAME_E', 'text'), ('LAST_NAME_E', 'text'), ('MIDDLE_NAME_E','text')], name = 'full_text')
        logging.info('WantedPersons Text Index created')                 