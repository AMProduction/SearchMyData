from datetime import datetime
import json
import os
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
        for info in self.__serviceCol.find({}, {'_id': 1, 'Description': 1, 'DocumentsCount': 1, 'LastModifiedDate': 1}).sort([('_id', 1)]):
            registersInfoTable.add_row([str(info['_id']), info['Description'], str(info['DocumentsCount']), '{:.19}'.format(info['LastModifiedDate'])])
        print(registersInfoTable.get_string(title = 'Registers info'))

    #deprecated
    def refreshMetadata(self):
    #deprecated
        print('Updating metadata...')
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
        #update or create DebtorsRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 3}, limit = 1) !=0):
            self.__updateDebtorsRegisterServiceJson()
            logging.info('DebtorsRegisterServiceJson updated')
        else:
            self.__createDebtorsRegisterServiceJson()
            logging.info('DebtorsRegisterServiceJson created')
        #update or create LegalEntitiesRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 4}, limit = 1) !=0):
            self.__updateLegalEntitiesRegisterServiceJson()
            logging.info('LegalEntitiesRegisterServiceJson updated')
        else:
            self.__createLegalEntitiesRegisterServiceJson()
            logging.info('LegalEntitiesRegisterServiceJson created')
        #update or create EntrepreneursRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 5}, limit = 1) !=0):
            self.__updateEntrepreneursRegisterServiceJson()
            logging.info('EntrepreneursRegisterServiceJson updated')
        else:
            self.__createEntrepreneursRegisterServiceJson()
            logging.info('EntrepreneursRegisterServiceJson created')
        print('Metadata updated')    
    
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
        
    def __createDebtorsRegisterServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        debtorsCol = self.__db['Debtors']
        documentsCount = debtorsCol.count_documents({})
        debtorsRegisterServiceJson = {
            '_id': 3,
            'Description': 'Єдиний реєстр боржників',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.__serviceCol.insert_one(debtorsRegisterServiceJson)
        
    def __updateDebtorsRegisterServiceJson(self):
        lastModifiedDate = datetime.now()
        debtorsCol = self.__db['Debtors']
        documentsCount = debtorsCol.count_documents({})
        self.__serviceCol.update_one(
            {'_id': 3},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )
        
    def __createLegalEntitiesRegisterServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        legalEntitiesCol = self.__db['LegalEntities']
        documentsCount = legalEntitiesCol.count_documents({})
        legalEntitiesRegisterServiceJson = {
            '_id': 4,
            'Description': 'Єдиний державний реєстр юридичних осіб та громадських формувань',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.__serviceCol.insert_one(legalEntitiesRegisterServiceJson)
        
    def __updateLegalEntitiesRegisterServiceJson(self):
        lastModifiedDate = datetime.now()
        legalEntitiesCol = self.__db['LegalEntities']
        documentsCount = legalEntitiesCol.count_documents({})
        self.__serviceCol.update_one(
            {'_id': 4},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )
        
    def __createEntrepreneursRegisterServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        entrepreneursCol = self.__db['Entrepreneurs']
        documentsCount = entrepreneursCol.count_documents({})
        entrepreneursRegisterServiceJson = {
            '_id': 5,
            'Description': 'Єдиний державний реєстр фізичних осіб – підприємців',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.__serviceCol.insert_one(entrepreneursRegisterServiceJson)
        
    def __updateEntrepreneursRegisterServiceJson(self):
        lastModifiedDate = datetime.now()
        entrepreneursCol = self.__db['Entrepreneurs']
        documentsCount = entrepreneursCol.count_documents({})
        self.__serviceCol.update_one(
            {'_id': 5},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )
        
    def clearResultsDir(self):
        for filename in os.listdir('results'):
            os.remove('results/'+filename)
        logging.info('"Results" folder is cleaned')
        
    def refreshMissingPersonsRegisterMetadata(self):
        collectionsList = self.__db.list_collection_names()
        #update or create MissingPersonsRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 1}, limit = 1) !=0):
            self.__updateMissingPersonsRegisterServiceJson()
            logging.info('MissingPersonsRegisterServiceJson updated')
        else:
            self.__createMissingPersonsRegisterServiceJson()
            logging.info('MissingPersonsRegisterServiceJson created')
    
    def refreshWantedPersonsRegisterMetadata(self):
        collectionsList = self.__db.list_collection_names()
        #update or create WantedgPersonsRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 2}, limit = 1) !=0):
            self.__updateWantedPersonsRegisterServiceJson()
            logging.info('WantedPersonsRegisterServiceJson updated')
        else:
            self.__createWantedPersonsRegisterServiceJson()
            logging.info('WantedPersonsRegisterServiceJson created')
            
    def refreshDebtorsRegisterMetadata(self):
        collectionsList = self.__db.list_collection_names()
        #update or create DebtorsRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 3}, limit = 1) !=0):
            self.__updateDebtorsRegisterServiceJson()
            logging.info('DebtorsRegisterServiceJson updated')
        else:
            self.__createDebtorsRegisterServiceJson()
            logging.info('DebtorsRegisterServiceJson created')
            
    def refreshLegalEntitiesRegisterMetadata(self):
        collectionsList = self.__db.list_collection_names()
        #update or create LegalEntitiesRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 4}, limit = 1) !=0):
            self.__updateLegalEntitiesRegisterServiceJson()
            logging.info('LegalEntitiesRegisterServiceJson updated')
        else:
            self.__createLegalEntitiesRegisterServiceJson()
            logging.info('LegalEntitiesRegisterServiceJson created')
            
    def refreshEntrepreneursRegisterMetadata(self):
        collectionsList = self.__db.list_collection_names()
        #update or create EntrepreneursRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.__serviceCol.count_documents({'_id': 5}, limit = 1) !=0):
            self.__updateEntrepreneursRegisterServiceJson()
            logging.info('EntrepreneursRegisterServiceJson updated')
        else:
            self.__createEntrepreneursRegisterServiceJson()
            logging.info('EntrepreneursRegisterServiceJson created')
    
    def deleteMissingPersonsRegisterCollectionIndex(self):
        start_time = datetime.now()
        missingPersonsCol = self.__db['MissingPersons']
        if ('full_text' in missingPersonsCol.index_information()):
            missingPersonsCol.drop_index('full_text')
            logging.warning('Missing persons Text index deleted')
        end_time = datetime.now()
        logging.info('deleteMissingPersonsRegisterCollectionIndex: ' + str(end_time-start_time))
    
    def createMissingPersonsRegisterCollectionIndex(self):
        start_time = datetime.now()
        missingPersonsCol = self.__db['MissingPersons']
        missingPersonsCol.create_index([('FIRST_NAME_U','text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text'), ('FIRST_NAME_R', 'text'), ('LAST_NAME_R', 'text'), ('MIDDLE_NAME_R', 'text'), ('FIRST_NAME_E', 'text'), ('LAST_NAME_E', 'text'), ('MIDDLE_NAME_E','text')], name = 'full_text')
        logging.info('Missing persons Text Index created')
        end_time = datetime.now()
        logging.info('createMissingPersonsRegisterCollectionIndex: ' + str(end_time-start_time))
        
    def deleteWantedPersonsRegisterCollectionIndex(self):
        start_time = datetime.now()
        wantedPersonsCol = self.__db['WantedPersons']
        if ('full_text' in wantedPersonsCol.index_information()):
            wantedPersonsCol.drop_index('full_text')
            logging.warning('WantedPersons Text index deleted')
        end_time = datetime.now()
        logging.info('deleteWantedPersonsRegisterCollectionIndex: ' + str(end_time-start_time))
    
    def createWantedPersonsRegisterCollectionIndex(self):
        start_time = datetime.now()
        wantedPersonsCol = self.__db['WantedPersons']
        wantedPersonsCol.create_index([('FIRST_NAME_U','text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text'), ('FIRST_NAME_R', 'text'), ('LAST_NAME_R', 'text'), ('MIDDLE_NAME_R', 'text'), ('FIRST_NAME_E', 'text'), ('LAST_NAME_E', 'text'), ('MIDDLE_NAME_E','text')], name = 'full_text')
        logging.info('WantedPersons Text Index created')
        end_time = datetime.now()
        logging.info('createWantedPersonsRegisterCollectionIndex: ' + str(end_time-start_time))
        
    def deleteEntrepreneursRegisterCollectionIndex(self):
        start_time = datetime.now()
        entrepreneursCol = self.__db['Entrepreneurs']
        if ('full_text' in entrepreneursCol.index_information()):
            entrepreneursCol.drop_index('full_text')
            logging.warning('Entrepreneurs Text index deleted')
        end_time = datetime.now()
        logging.info('deleteEntrepreneursRegisterCollectionIndex: ' + str(end_time-start_time))
            
    def createEntrepreneursRegisterCollectionIndex(self):
        start_time = datetime.now()
        entrepreneursCol = self.__db['Entrepreneurs']
        entrepreneursCol.create_index([('fio','text')], name = 'full_text')
        logging.info('Entrepreneurs Text Index created')
        end_time = datetime.now()
        logging.info('createEntrepreneursRegisterCollectionIndex: ' + str(end_time-start_time))
        
    def deleteLegalEntitiesRegisterCollectionIndex(self):
        start_time = datetime.now()
        legalEntitiesCol = self.__db['LegalEntities']
        if ('full_text' in legalEntitiesCol.index_information()):
            legalEntitiesCol.drop_index('full_text')
            logging.warning('LegalEntities Text index deleted')
        end_time = datetime.now()
        logging.info('deleteLegalEntitiesRegisterCollectionIndex: ' + str(end_time-start_time))
    
    def createLegalEntitiesRegisterCollectionIndex(self):
        start_time = datetime.now()
        legalEntitiesCol = self.__db['LegalEntities']
        legalEntitiesCol.create_index([('short_name','text'), ('edrpou', 'text'), ('boss', 'text'), ('beneficiaries', 'text'), ('founders', 'text')], name = 'full_text')
        logging.info('LegalEntities Text Index created')
        end_time = datetime.now()
        logging.info('createLegalEntitiesRegisterCollectionIndex: ' + str(end_time-start_time))