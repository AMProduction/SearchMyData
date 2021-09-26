from datetime import datetime
import json
from prettytable import PrettyTable
from pathlib import Path
import pymongo
import logging

from pymongo.errors import ServerSelectionTimeoutError

class ServiceTools:
    
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
                self.__serviceCol = self.__db['ServiceCollection']
        #if config.json does not exists
        else:
            logging.error('Config.json is not found')
            logging.info('The application closed')
            print('Config.json is not found')
            print('Quitting...')
            exit()        
        
    def getRegistersInfo(self):
        registersInfoTable = PrettyTable(['#', 'Description', 'Documents count', 'Last modified date'])
        for info in self.__serviceCol.find({}, {'_id': 1, 'Description': 1, 'DocumentsCount': 1, 'LastModifiedDate': 1}):
            registersInfoTable.add_row([str(info['_id']), info['Description'], str(info['DocumentsCount']), '{:.19}'.format(info['LastModifiedDate'])])
        print(registersInfoTable.get_string(title = 'Registers info'))

    def refreshMetadata(self):
        collectionsList = self.__db.list_collection_names()
        #update or create MissingPersonsRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 1}, limit = 1) !=0):
            self.__updateMissingPersonsRegisterServiceJson()
            logging.info('MissingPersonsRegisterServiceJson updated')
        else:
            self.__createMissingPersonsRegisterServiceJson()
            logging.info('MissingPersonsRegisterServiceJson created')
        #update or create WantedgPersonsRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 2}, limit = 1) !=0):
            self.__updateWantedPersonsRegisterServiceJson()
            logging.info('WantedPersonsRegisterServiceJson updated')
        else:
            self.__createWantedPersonsRegisterServiceJson()
            logging.info('WantedPersonsRegisterServiceJson created')    
    
    def __createMissingPersonsRegisterServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        missingPersonsCol = self.__db['MissingPersons']
        documentsCount = missingPersonsCol.count_documents({})
        missingPersonsRegisterServiceJson = {
            '_id': 1,
            'Description': 'Інформація про безвісно зниклих громадян',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.__serviceCol.insert_one(missingPersonsRegisterServiceJson)

    def __updateMissingPersonsRegisterServiceJson(self):
        lastModifiedDate = datetime.now()
        missingPersonsCol = self.__db['MissingPersons']
        documentsCount = missingPersonsCol.count_documents({})
        self.__serviceCol.update_one(
            {'_id': 1},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )
        
    def __createWantedPersonsRegisterServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        wantedPersonsCol = self.__db['WantedPersons']
        documentsCount = wantedPersonsCol.count_documents({})
        wantedPersonsRegisterServiceJson = {
            '_id': 2,
            'Description': 'Інформація про осіб, які переховуються від органів влади',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.__serviceCol.insert_one(wantedPersonsRegisterServiceJson)
        
    def __updateWantedPersonsRegisterServiceJson(self):
        lastModifiedDate = datetime.now()
        wantedPersonsCol = self.__db['WantedPersons']
        documentsCount = wantedPersonsCol.count_documents({})
        self.__serviceCol.update_one(
            {'_id': 2},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )