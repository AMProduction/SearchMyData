import json
import logging
import requests
import gc
from prettytable import PrettyTable
from datetime import datetime

from src.dataset import Dataset

class WantedPersonsRegister(Dataset):
    def __init__(self):
        super().__init__()
        
    def getDataset(self):
        print('The register "Інформація про осіб, які переховуються від органів влади" is retrieving...')
        try:
            generalDataset = requests.get('https://data.gov.ua/api/3/action/package_show?id=7c51c4a0-104b-4540-a166-e9fc58485c1b').text
        except:
            logging.error('Error during general WantedPersons dataset JSON receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            generalDatasetJson = json.loads(generalDataset)
            logging.info('A general WantedPersons dataset JSON received')        
        #get dataset id
        wantedPersonsGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']
        try:
            #get resources JSON id
            wantedPersonsGeneralDatasetIdJson = requests.get('https://data.gov.ua/api/3/action/resource_show?id=' + wantedPersonsGeneralDatasetId).text
        except:
            logging.error('Error during WantedPersons resources JSON id receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            wantedPersonsGeneralDatasetJson = json.loads(wantedPersonsGeneralDatasetIdJson)
            logging.info('A WantedPersons resources JSON id received')
        #get dataset json url
        wantedPersonsDatasetJsonUrl = wantedPersonsGeneralDatasetJson['result']['url']
        try:
            #get dataset
            wantedPersonsDatasetJson = requests.get(wantedPersonsDatasetJsonUrl).text
        except:
            logging.error('Error during WantedPersons dataset receiving occured')
            print('Error during dataset receiving occured!')
        else:
            wantedPersonsDataset = json.loads(wantedPersonsDatasetJson)
            logging.info('A WantedPersons dataset received')          
        print('The Register "' + generalDatasetJson['result']['title'] + '" refreshed')
        return wantedPersonsDataset
    
    def saveDataset(self, json):
        start_time = datetime.now()
        wantedPersonsCol = self.db['WantedPersons']
        wantedPersonsCol.insert_many(json)
        logging.info('Wanted persons dataset was saved into the database')
        end_time = datetime.now()
        logging.info('Time to save into the wanted person register: ' + str(end_time-start_time))
        gc.collect()
        
    def clearCollection(self):
        start_time = datetime.now()
        wantedPersonsCol = self.db['WantedPersons']
        countDeletedDocuments = wantedPersonsCol.delete_many({})
        logging.warning('%s documents deleted. The wanted persons collection is empty.', str(countDeletedDocuments.deleted_count))
        end_time = datetime.now()
        logging.info('clearWantedPersonsRegisterCollection: ' + str(end_time-start_time))
        
    def __createServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        wantedPersonsCol = self.db['WantedPersons']
        documentsCount = wantedPersonsCol.count_documents({})
        wantedPersonsRegisterServiceJson = {
            '_id': 2,
            'Description': 'Інформація про осіб, які переховуються від органів влади',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.serviceCol.insert_one(wantedPersonsRegisterServiceJson)
        
    def __updateServiceJson(self):
        lastModifiedDate = datetime.now()
        wantedPersonsCol = self.db['WantedPersons']
        documentsCount = wantedPersonsCol.count_documents({})
        self.serviceCol.update_one(
            {'_id': 2},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )
        
    def updateMetadata(self):
        collectionsList = self.db.list_collection_names()
        #update or create WantedgPersonsRegisterServiceJson 
        if ('ServiceCollection' in collectionsList) and (self.serviceCol.count_documents({'_id': 2}, limit = 1) !=0):
            self.__updateServiceJson()
            logging.info('WantedPersonsRegisterServiceJson updated')
        else:
            self.__createServiceJson()
            logging.info('WantedPersonsRegisterServiceJson created')
            
    def deleteCollectionIndex(self):
        start_time = datetime.now()
        wantedPersonsCol = self.db['WantedPersons']
        if ('full_text' in wantedPersonsCol.index_information()):
            wantedPersonsCol.drop_index('full_text')
            logging.warning('WantedPersons Text index deleted')
        end_time = datetime.now()
        logging.info('deleteWantedPersonsRegisterCollectionIndex: ' + str(end_time-start_time))
        
    def createCollectionIndex(self):
        start_time = datetime.now()
        wantedPersonsCol = self.db['WantedPersons']
        wantedPersonsCol.create_index([('FIRST_NAME_U','text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text'), ('FIRST_NAME_R', 'text'), ('LAST_NAME_R', 'text'), ('MIDDLE_NAME_R', 'text'), ('FIRST_NAME_E', 'text'), ('LAST_NAME_E', 'text'), ('MIDDLE_NAME_E','text')], name = 'full_text')
        logging.info('WantedPersons Text Index created')
        end_time = datetime.now()
        logging.info('createWantedPersonsRegisterCollectionIndex: ' + str(end_time-start_time))
        
    def searchIntoCollection(self, queryString):
        start_time = datetime.now()
        wantedPersonsCol = self.db['WantedPersons']
        resultCount = wantedPersonsCol.count_documents({'$text': {'$search': queryString}})
        if resultCount == 0:
            print('The wanted persons register: No data found')
            logging.warning('The wanted persons register: No data found')
        else:
            resultTable = PrettyTable(['LAST NAME', 'FIRST NAME', 'MIDDLE NAME', 'BIRTH DATE', 'LOST PLACE', 'LOST DATE', 'CATEGORY', 'WHO IS SEARCHING', 'CRIME'])
            resultTable.align = 'l'
            resultTable._max_width = {'LOST PLACE': 20, 'CATEGORY': 25, 'WHO IS SEARCHING': 25, 'CRIME': 15}
            #show only 10 first search results
            for result in wantedPersonsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'} }).sort([('score', {'$meta': 'textScore'})]).limit(10):
                resultTable.add_row([result['LAST_NAME_E'], result['FIRST_NAME_E'], result['MIDDLE_NAME_E'], '{:.10}'.format(result['BIRTH_DATE']), result['LOST_PLACE'], '{:.10}'.format(result['LOST_DATE']), result['CATEGORY'], result['OVD'], result['ARTICLE_CRIM']])
            print(resultTable.get_string(title = 'The wanted persons register: ' + str(resultCount) + ' records found'))
            logging.warning('The wanted persons register: %s records found', str(resultCount))
            print('Only 10 first search results showed')
            #save all search results into HTML
            for result in wantedPersonsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'} }).sort([('score', {'$meta': 'textScore'})]):
                resultTable.add_row([result['LAST_NAME_E'], result['FIRST_NAME_E'], result['MIDDLE_NAME_E'], '{:.10}'.format(result['BIRTH_DATE']), result['LOST_PLACE'], '{:.10}'.format(result['LOST_DATE']), result['CATEGORY'], result['OVD'], result['ARTICLE_CRIM']])
            htmlResult = resultTable.get_html_string()
            f = open('results/WantedPersons.html', 'w', encoding='utf-8')
            f.write(htmlResult)
            f.close()
            print('All result dataset was saved into WantedPersons.html')
            logging.warning('All result dataset was saved into WantedPersons.html')
        end_time = datetime.now()
        logging.info('Search time into the wanted person register: ' + str(end_time-start_time))
        gc.collect()