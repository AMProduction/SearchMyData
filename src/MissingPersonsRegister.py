import json
import logging
import requests
import gc
from prettytable import PrettyTable
from datetime import datetime

from src.Dataset import Dataset


class MissingPersonsRegister(Dataset):
    def __init__(self):
        super().__init__()

    def __getDataset(self):
        print('The register "Інформація про безвісно зниклих громадян" is retrieving...')
        try:
            generalDataset = requests.get(
                'https://data.gov.ua/api/3/action/package_show?id=470196d3-4e7a-46b0-8c0c-883b74ac65f0').text
        except:
            logging.error(
                'Error during general MissingPersons dataset JSON receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            generalDatasetJson = json.loads(generalDataset)
            logging.info('A general MissingPersons dataset JSON received')
        # get dataset id
        missingPersonsGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']
        try:
            # get resources JSON id
            missingPersonsGeneralDatasetIdJson = requests.get(
                'https://data.gov.ua/api/3/action/resource_show?id=' + missingPersonsGeneralDatasetId).text
        except:
            logging.error(
                'Error during MissingPersons resources JSON id receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            missingPersonsGeneralDatasetJson = json.loads(
                missingPersonsGeneralDatasetIdJson)
            logging.info('A MissingPersons resources JSON id received')
        # get dataset json url
        missingPersonsDatasetJsonUrl = missingPersonsGeneralDatasetJson['result']['url']
        try:
            # get dataset
            missingPersonsDatasetJson = requests.get(
                missingPersonsDatasetJsonUrl).text
        except:
            logging.error(
                'Error during MissingPersons dataset receiving occured')
            print('Error during dataset receiving occured!')
        else:
            missingPersonsDataset = json.loads(missingPersonsDatasetJson)
            logging.info('A MissingPersons dataset received')
        print('The Register "' +
              generalDatasetJson['result']['title'] + '" refreshed')
        return missingPersonsDataset

    def __saveDataset(self, json):
        start_time = datetime.now()
        missingPersonsCol = self.db['MissingPersons']
        missingPersonsCol.insert_many(json)
        logging.info('Missing persons dataset was saved into the database')
        end_time = datetime.now()
        logging.info(
            'Time to save into the missing person register: ' + str(end_time-start_time))
        gc.collect()

    def __clearCollection(self):
        start_time = datetime.now()
        missingPersonsCol = self.db['MissingPersons']
        countDeletedDocuments = missingPersonsCol.delete_many({})
        logging.warning('%s documents deleted. The missing persons collection is empty.', str(
            countDeletedDocuments.deleted_count))
        end_time = datetime.now()
        logging.info('clearMissingPersonsRegisterCollection: ' +
                     str(end_time-start_time))

    def __createServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        missingPersonsCol = self.db['MissingPersons']
        documentsCount = missingPersonsCol.count_documents({})
        missingPersonsRegisterServiceJson = {
            '_id': 1,
            'Description': 'Інформація про безвісно зниклих громадян',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.serviceCol.insert_one(missingPersonsRegisterServiceJson)

    def __updateServiceJson(self):
        lastModifiedDate = datetime.now()
        missingPersonsCol = self.db['MissingPersons']
        documentsCount = missingPersonsCol.count_documents({})
        self.serviceCol.update_one(
            {'_id': 1},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )

    def __updateMetadata(self):
        collectionsList = self.db.list_collection_names()
        # update or create MissingPersonsRegisterServiceJson
        if ('ServiceCollection' in collectionsList) and (self.serviceCol.count_documents({'_id': 1}, limit=1) != 0):
            self.__updateServiceJson()
            logging.info('MissingPersonsRegisterServiceJson updated')
        else:
            self.__createServiceJson()
            logging.info('MissingPersonsRegisterServiceJson created')

    def __deleteCollectionIndex(self):
        start_time = datetime.now()
        missingPersonsCol = self.db['MissingPersons']
        if ('full_text' in missingPersonsCol.index_information()):
            missingPersonsCol.drop_index('full_text')
            logging.warning('Missing persons Text index deleted')
        end_time = datetime.now()
        logging.info(
            'deleteMissingPersonsRegisterCollectionIndex: ' + str(end_time-start_time))

    def __createCollectionIndex(self):
        start_time = datetime.now()
        missingPersonsCol = self.db['MissingPersons']
        missingPersonsCol.create_index([('FIRST_NAME_U', 'text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text'), ('FIRST_NAME_R', 'text'), (
            'LAST_NAME_R', 'text'), ('MIDDLE_NAME_R', 'text'), ('FIRST_NAME_E', 'text'), ('LAST_NAME_E', 'text'), ('MIDDLE_NAME_E', 'text')], name='full_text')
        logging.info('Missing persons Text Index created')
        end_time = datetime.now()
        logging.info(
            'createMissingPersonsRegisterCollectionIndex: ' + str(end_time-start_time))

    def searchIntoCollection(self, queryString):
        start_time = datetime.now()
        missingPersonsCol = self.db['MissingPersons']
        resultCount = missingPersonsCol.count_documents(
            {'$text': {'$search': queryString}})
        if resultCount == 0:
            print('The missing persons register: No data found')
            logging.warning('The missing persons register: No data found')
        else:
            resultTable = PrettyTable(
                ['LAST NAME', 'FIRST NAME', 'MIDDLE NAME', 'BIRTH DATE', 'LOST PLACE', 'LOST DATE'])
            resultTable.align = 'l'
            # show only 10 first search results
            for result in missingPersonsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'}}).sort([('score', {'$meta': 'textScore'})]).limit(10):
                resultTable.add_row([result['LAST_NAME_E'], result['FIRST_NAME_E'], result['MIDDLE_NAME_E'], '{:.10}'.format(
                    result['BIRTH_DATE']), result['LOST_PLACE'], '{:.10}'.format(result['LOST_DATE'])])
            print(resultTable.get_string(
                title='The missing persons register: ' + str(resultCount) + ' records found'))
            logging.warning(
                'The missing persons register: %s records found', str(resultCount))
            print('Only 10 first search results showed')
            # save all search results into HTML
            for result in missingPersonsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'}}).sort([('score', {'$meta': 'textScore'})]):
                resultTable.add_row([result['LAST_NAME_E'], result['FIRST_NAME_E'], result['MIDDLE_NAME_E'], '{:.10}'.format(
                    result['BIRTH_DATE']), result['LOST_PLACE'], '{:.10}'.format(result['LOST_DATE'])])
            htmlResult = resultTable.get_html_string()
            f = open('results/MissingPersons.html', 'w', encoding='utf-8')
            f.write(htmlResult)
            f.close()
            print('All result dataset was saved into MissingPersons.html')
            logging.warning(
                'All result dataset was saved into MissingPersons.html')
        end_time = datetime.now()
        logging.info(
            'Search time into the missing person register: ' + str(end_time-start_time))
        gc.collect()

    def setupDataset(self):
        self.__deleteCollectionIndex()
        self.__clearCollection()
        __dataset = self.__getDataset()
        self.__saveDataset(__dataset)
        self.__updateMetadata()
        self.__createCollectionIndex()