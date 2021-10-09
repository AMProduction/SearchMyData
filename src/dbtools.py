import json
import pymongo
import logging
from pathlib import Path
import zipfile
from io import BytesIO
import xmltodict
import pandas as pd
from dask import dataframe as dd
import os
from pymongo.errors import ServerSelectionTimeoutError
import requests
import shutil
from datetime import datetime

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
        start_time = datetime.now()
        missingPersonsCol = self.__db['MissingPersons']
        countDeletedDocuments = missingPersonsCol.delete_many({})
        logging.warning('%s documents deleted. The missing persons collection is empty.', str(countDeletedDocuments.deleted_count))
        if ('full_text' in missingPersonsCol.index_information()):
            missingPersonsCol.drop_index('full_text')
            logging.warning('Missing persons Text index deleted')
        missingPersonsCol.insert_many(json)
        logging.info('Missing persons dataset was saved into the database')
        missingPersonsCol.create_index([('FIRST_NAME_U','text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text'), ('FIRST_NAME_R', 'text'), ('LAST_NAME_R', 'text'), ('MIDDLE_NAME_R', 'text'), ('FIRST_NAME_E', 'text'), ('LAST_NAME_E', 'text'), ('MIDDLE_NAME_E','text')], name = 'full_text')
        logging.info('Missing persons Text Index created')
        end_time = datetime.now()
        logging.info('Time to save into the missing person register: ' + str(end_time-start_time))
        
    def saveWantedPersonsRegister(self, json):
        start_time = datetime.now()
        wantedPersonsCol = self.__db['WantedPersons']
        countDeletedDocuments = wantedPersonsCol.delete_many({})
        logging.warning('%s documents deleted. The wanted persons collection is empty.', str(countDeletedDocuments.deleted_count))
        if ('full_text' in wantedPersonsCol.index_information()):
            wantedPersonsCol.drop_index('full_text')
            logging.warning('WantedPersons Text index deleted')
        wantedPersonsCol.insert_many(json)
        logging.info('Wanted persons dataset was saved into the database')
        wantedPersonsCol.create_index([('FIRST_NAME_U','text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text'), ('FIRST_NAME_R', 'text'), ('LAST_NAME_R', 'text'), ('MIDDLE_NAME_R', 'text'), ('FIRST_NAME_E', 'text'), ('LAST_NAME_E', 'text'), ('MIDDLE_NAME_E','text')], name = 'full_text')
        logging.info('WantedPersons Text Index created')
        end_time = datetime.now()
        logging.info('Time to save into the wanted person register: ' + str(end_time-start_time))
        
    #don't work
    def saveEntrepreneursRegister(self, zipUrl):
        entrepreneursCol = self.__db['Entrepreneurs']
        countDeletedDocuments = entrepreneursCol.delete_many({})
        #insert logging and index
        legalEntitiesCol = self.__db['LegalEntities']
        countDeletedDocuments = legalEntitiesCol.delete_many({})
        #insert logging and index
        try:
            #get ZIP file
            entrepreneursDatasetZIP = requests.get(zipUrl).content
        except:
            logging.error('Error during EntrepreneursRegister ZIP receiving occured')
            print('Error during ZIP file receiving occured!')
        else:
            logging.info('A EntrepreneursRegister dataset received')
            #get lists of file
            entrepreneursZip = zipfile.ZipFile(BytesIO(entrepreneursDatasetZIP), 'r' )
            #go inside ZIP
            for xmlFile in entrepreneursZip.namelist():
                #skip root folder
                if xmlFile.endswith('/'):
                    continue
                logging.warning('File in ZIP: ' + str(xmlFile))
                if xmlFile.find('_UO_') != -1:
                    #read the legal Entities Xml file
                    legalEntitiesXml = entrepreneursZip.open(xmlFile)
                    #convert xml to json
                    legalEntitiesJson = xmltodict.parse(legalEntitiesXml, encoding='windows-1251')
                    #save to the collection
                    legalEntitiesCol.insert_many(legalEntitiesJson)
                if xmlFile.find('_FOP_') != -1:
                    #read the entrepreneurs Xml file
                    entrepreneursXml = entrepreneursZip.open(xmlFile)
                    #convert xml to json
                    entrepreneursJson = xmltodict.parse(entrepreneursXml, encoding='windows-1251')
                    #save to the collection
                    entrepreneursCol.insert_many(entrepreneursJson)
                    
    def saveDebtorsRegister(self, zipUrl):
        start_time = datetime.now()
        debtorsCol = self.__db['Debtors']
        countDeletedDocuments = debtorsCol.delete_many({})
        logging.warning('%s documents deleted. The wanted persons collection is empty.', str(countDeletedDocuments.deleted_count))
        if ('full_text' in debtorsCol.index_information()):
            debtorsCol.drop_index('full_text')
            logging.warning('WantedPersons Text index deleted')
        try:
            #get ZIP file
            debtorsDatasetZIP = requests.get(zipUrl).content
        except:
            logging.error('Error during DebtorsRegisterZIP receiving occured')
            print('Error during ZIP file receiving occured!')
        else:
            logging.info('A DebtorsRegister dataset received')
            print('The Register "Єдиний реєстр боржників" refreshed')
            #get lists of file
            debtorsZip = zipfile.ZipFile(BytesIO(debtorsDatasetZIP), 'r' )
            #go inside ZIP
            for csvFile in debtorsZip.namelist():
                logging.warning('File in ZIP: ' + str(csvFile))
                debtorsCsvFileName = str(csvFile)
            debtorsZip.extractall()
            debtorsZip.close()
            #read CSV using Dask
            debtorsCsv = dd.read_csv(debtorsCsvFileName, encoding='windows-1251', header=None, skiprows=[0], dtype = {1:'object'}, names=['DEBTOR_NAME', 'DEBTOR_CODE', 'PUBLISHER', 'EMP_FULL_FIO', 'EMP_ORG', 'ORG_PHONE', 'EMAIL_ADDR', 'VP_ORDERNUM', 'VD_CAT'])
            #convert CSV to JSON using Dask
            debtorsCsv.to_json('debtorsJson')
            for file in os.listdir('debtorsJson'):
                #save to the collection
                for line in open('debtorsJson/'+file, 'r'):
                    debtorsJson = json.loads(line) 
                    debtorsCol.insert_one(debtorsJson)
            logging.info('Debtors dataset was saved into the database')
            debtorsCol.create_index([('DEBTOR_NAME','text'), ('DEBTOR_CODE', 'text'), ('EMP_FULL_FIO', 'text')], name = 'full_text')
            logging.info('Debtors Text Index created')
            os.remove(debtorsCsvFileName)
            shutil.rmtree('debtorsJson', ignore_errors=True)
        end_time = datetime.now()
        logging.info('Time to save into the debtors register: ' + str(end_time-start_time))