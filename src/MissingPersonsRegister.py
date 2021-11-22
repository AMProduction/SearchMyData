import gc
import json
import logging
from datetime import datetime

import requests
from prettytable import PrettyTable
from pymongo.errors import PyMongoError

from src.dataset import Dataset


class MissingPersonsRegister(Dataset):
    def __init__(self):
        super().__init__()

    @Dataset.measure_execution_time
    def __get_dataset(self):
        print('The register "Інформація про безвісно зниклих громадян" is retrieving...')
        try:
            general_dataset = requests.get(
                'https://data.gov.ua/api/3/action/package_show?id=470196d3-4e7a-46b0-8c0c-883b74ac65f0').text
        except ConnectionError:
            logging.error('Error during general MissingPersons dataset JSON receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            general_dataset_json = json.loads(general_dataset)
            logging.info('A general MissingPersons dataset JSON received')
        # get dataset id
        missing_persons_general_dataset_id = general_dataset_json['result']['resources'][0]['id']
        try:
            # get resources JSON id
            missing_persons_general_dataset_id_json = requests.get(
                'https://data.gov.ua/api/3/action/resource_show?id=' + missing_persons_general_dataset_id).text
        except ConnectionError:
            logging.error('Error during MissingPersons resources JSON id receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            missing_persons_general_dataset_json = json.loads(missing_persons_general_dataset_id_json)
            logging.info('A MissingPersons resources JSON id received')
        # get dataset json url
        missing_persons_dataset_json_url = missing_persons_general_dataset_json['result']['url']
        try:
            # get dataset
            missing_persons_dataset_json = requests.get(missing_persons_dataset_json_url).text
        except ConnectionError:
            logging.error('Error during MissingPersons dataset receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            missing_persons_dataset = json.loads(missing_persons_dataset_json)
            logging.info('A MissingPersons dataset received')
        print('The Register "' + general_dataset_json['result']['title'] + '" refreshed')
        return missing_persons_dataset

    @Dataset.measure_execution_time
    def __save_dataset(self, json):
        missing_persons_col = self.db['MissingPersons']
        try:
            missing_persons_col.insert_many(json)
        except PyMongoError:
            logging.error('Error during saving Missing Persons Register into Database')
            print('Error during saving Missing Persons Register into Database')
        else:
            logging.info('Missing persons dataset was saved into the database')
        gc.collect()

    @Dataset.measure_execution_time
    def __clear_collection(self):
        if self.is_collection_exists('MissingPersons'):
            missing_persons_col = self.db['MissingPersons']
            count_deleted_documents = missing_persons_col.delete_many({})
            logging.warning('%s documents deleted. The missing persons collection is empty.', str(
                count_deleted_documents.deleted_count))

    @Dataset.measure_execution_time
    def __create_service_json(self):
        created_date = datetime.now()
        last_modified_date = datetime.now()
        missing_persons_col = self.db['MissingPersons']
        documents_count = missing_persons_col.count_documents({})
        missing_persons_register_service_json = {
            '_id': 1,
            'Description': 'Інформація про безвісно зниклих громадян',
            'DocumentsCount': documents_count,
            'CreatedDate': str(created_date),
            'LastModifiedDate': str(last_modified_date)
        }
        self.serviceCol.insert_one(missing_persons_register_service_json)

    @Dataset.measure_execution_time
    def __update_service_json(self):
        last_modified_date = datetime.now()
        missing_persons_col = self.db['MissingPersons']
        documents_count = missing_persons_col.count_documents({})
        self.serviceCol.update_one(
            {'_id': 1},
            {'$set': {'LastModifiedDate': str(last_modified_date),
                      'DocumentsCount': documents_count}}
        )

    @Dataset.measure_execution_time
    def __update_metadata(self):
        # update or create MissingPersonsRegisterServiceJson
        if (self.is_collection_exists('ServiceCollection')) and (
                self.serviceCol.count_documents({'_id': 1}, limit=1) != 0):
            self.__update_service_json()
            logging.info('MissingPersonsRegisterServiceJson updated')
        else:
            self.__create_service_json()
            logging.info('MissingPersonsRegisterServiceJson created')

    @Dataset.measure_execution_time
    def __delete_collection_index(self):
        if self.is_collection_exists('MissingPersons'):
            missing_persons_col = self.db['MissingPersons']
            if 'full_text' in missing_persons_col.index_information():
                missing_persons_col.drop_index('full_text')
                logging.warning('Missing persons Text index deleted')

    @Dataset.measure_execution_time
    def __create_collection_index(self):
        missing_persons_col = self.db['MissingPersons']
        missing_persons_col.create_index([('FIRST_NAME_U', 'text'), ('LAST_NAME_U', 'text'), ('MIDDLE_NAME_U', 'text')],
                                         name='full_text')
        logging.info('Missing persons Text Index created')

    @Dataset.measure_execution_time
    def search_into_collection(self, query_string):
        missing_persons_col = self.db['MissingPersons']
        try:
            result_count = missing_persons_col.count_documents(
                {'$text': {'$search': query_string}})
        except PyMongoError:
            logging.error(
                'Error during search into Missing Persons Register')
            print('Error during search into Missing Persons Register')
        else:
            if result_count == 0:
                print('The missing persons register: No data found')
                logging.warning('The missing persons register: No data found')
            else:
                result_table = PrettyTable(
                    ['LAST NAME', 'FIRST NAME', 'MIDDLE NAME', 'BIRTH DATE', 'LOST PLACE', 'LOST DATE'])
                result_table.align = 'l'
                # show only 10 first search results
                for result in missing_persons_col.find({'$text': {'$search': query_string}},
                                                       {'score': {'$meta': 'textScore'}}) \
                        .sort([('score', {'$meta': 'textScore'})]).limit(10):
                    result_table.add_row([result['LAST_NAME_U'], result['FIRST_NAME_U'], result['MIDDLE_NAME_U'],
                                          '{:.10}'.format(result['BIRTH_DATE']), result['LOST_PLACE'],
                                          '{:.10}'.format(result['LOST_DATE'])])
                print(result_table.get_string(
                    title=f'The missing persons register: {result_count} records found'))
                logging.warning('The missing persons register: %s records found', str(result_count))
                print('Only 10 first search results showed')
                # save all search results into HTML
                for result in missing_persons_col.find({'$text': {'$search': query_string}},
                                                       {'score': {'$meta': 'textScore'}}) \
                        .sort([('score', {'$meta': 'textScore'})]):
                    result_table.add_row([result['LAST_NAME_U'], result['FIRST_NAME_U'], result['MIDDLE_NAME_U'],
                                          '{:.10}'.format(result['BIRTH_DATE']), result['LOST_PLACE'],
                                          '{:.10}'.format(result['LOST_DATE'])])
                html_result = result_table.get_html_string()
                f = open('results/MissingPersons.html', 'w', encoding='utf-8')
                f.write(html_result)
                f.close()
                print('All result dataset was saved into MissingPersons.html')
                logging.warning(
                    'All result dataset was saved into MissingPersons.html')
        gc.collect()

    @Dataset.measure_execution_time
    def setup_dataset(self):
        self.__delete_collection_index()
        self.__clear_collection()
        __dataset = self.__get_dataset()
        self.__save_dataset(__dataset)
        self.__update_metadata()
        self.__create_collection_index()
