import json
import logging
import requests
import gc
import zipfile
import os
import shutil
import xml.etree.ElementTree as ET
from prettytable import PrettyTable
from datetime import datetime
from io import BytesIO

from src.dataset import Dataset

class EntrepreneursRegister(Dataset):
    def __init__(self):
        super().__init__()
        
    def getDataset(self):
        logging.info('EntrepreneursRegister getDataset call')
        
    def saveDataset(self):
        logging.info('EntrepreneursRegister saveDataset call')
        
    def clearCollection(self):
        start_time = datetime.now()
        entrepreneursCol = self.db['Entrepreneurs']
        countDeletedDocuments = entrepreneursCol.delete_many({})
        logging.warning('%s documents deleted. The entrepreneurs collection is empty.', str(countDeletedDocuments.deleted_count))
        end_time = datetime.now()
        logging.info('clearEntrepreneursRegisterCollection: ' + str(end_time-start_time))
        
    def __createServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        entrepreneursCol = self.db['Entrepreneurs']
        documentsCount = entrepreneursCol.count_documents({})
        entrepreneursRegisterServiceJson = {
            '_id': 5,
            'Description': 'Єдиний державний реєстр фізичних осіб – підприємців',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.serviceCol.insert_one(entrepreneursRegisterServiceJson)
        
    def __updateServiceJson(self):
        lastModifiedDate = datetime.now()
        entrepreneursCol = self.db['Entrepreneurs']
        documentsCount = entrepreneursCol.count_documents({})
        self.serviceCol.update_one(
            {'_id': 5},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )
        
    def updateMetadata(self):
        collectionsList = self.db.list_collection_names()
        #update or create EntrepreneursRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.serviceCol.count_documents({'_id': 5}, limit = 1) !=0):
            self.__updateServiceJson()
            logging.info('EntrepreneursRegisterServiceJson updated')
        else:
            self.__createServiceJson()
            logging.info('EntrepreneursRegisterServiceJson created')
            
    def deleteCollectionIndex(self):
        start_time = datetime.now()
        entrepreneursCol = self.db['Entrepreneurs']
        if ('full_text' in entrepreneursCol.index_information()):
            entrepreneursCol.drop_index('full_text')
            logging.warning('Entrepreneurs Text index deleted')
        end_time = datetime.now()
        logging.info('deleteEntrepreneursRegisterCollectionIndex: ' + str(end_time-start_time))
        
    def createCollectionIndex(self):
        start_time = datetime.now()
        entrepreneursCol = self.db['Entrepreneurs']
        entrepreneursCol.create_index([('fio','text')], name = 'full_text')
        logging.info('Entrepreneurs Text Index created')
        end_time = datetime.now()
        logging.info('createEntrepreneursRegisterCollectionIndex: ' + str(end_time-start_time))
        
    def searchIntoCollection(self, queryString):
        start_time = datetime.now()
        entrepreneursCol = self.db['Entrepreneurs']
        resultCount = entrepreneursCol.count_documents({'$text': {'$search': queryString}})
        if resultCount == 0:
            print('The Entrepreneurs register: No data found')
            logging.warning('The Entrepreneurs register: No data found')
        else:
            resultTable = PrettyTable(['NAME', 'ADDRESS', 'KVED', 'STATE'])
            resultTable.align = 'l'
            resultTable._max_width = {'NAME': 25, 'ADDRESS': 25, 'KVED': 30}
            #show only 10 first search results
            for result in entrepreneursCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'} }).sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                resultTable.add_row([result['fio'], result['address'], result['kved'], result['stan']])
            print(resultTable.get_string(title = 'The Entrepreneurs register: ' + str(resultCount) + ' records found'))
            logging.warning('The Entrepreneurs register: %s records found', str(resultCount))
            print('Only 10 first search results showed')
            #save all search results into HTML
            for result in entrepreneursCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'} }).sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                resultTable.add_row([result['fio'], result['address'], result['kved'], result['stan']])
            htmlResult = resultTable.get_html_string()
            f = open('results/Entrepreneurs.html', 'w', encoding='utf-8')
            f.write(htmlResult)
            f.close()
            print('All result dataset was saved into Entrepreneurs.html')
            logging.warning('All result dataset was saved into Entrepreneurs.html')
        end_time = datetime.now()
        logging.info('Search time into the Entrepreneurs register: ' + str(end_time-start_time))
        gc.collect()