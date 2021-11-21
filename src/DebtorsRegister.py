import gc
import json
import logging
import os
import shutil
import zipfile
import mmap
from datetime import datetime
from io import BytesIO
from pymongo.errors import PyMongoError

import requests
from dask import dataframe as dd
from prettytable import PrettyTable

from src.dataset import Dataset


class DebtorsRegister(Dataset):
    def __init__(self):
        super().__init__()

    @Dataset.measure_execution_time
    def __get_dataset(self):
        print('The register "Єдиний реєстр боржників" is retrieving...')
        try:
            general_dataset = requests.get(
                'https://data.gov.ua/api/3/action/package_show?id=506734bf-2480-448c-a2b4-90b6d06df11e').text
        except ConnectionError:
            logging.error('Error during general DebtorsRegister dataset JSON receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            general_dataset_json = json.loads(general_dataset)
            logging.info('A general DebtorsRegister dataset JSON received')
        # get dataset id
        debtors_general_dataset_id = general_dataset_json['result']['resources'][0]['id']
        try:
            # get resources JSON id
            debtors_general_dataset_id_json = requests.get(
                'https://data.gov.ua/api/3/action/resource_show?id=' + debtors_general_dataset_id).text
        except ConnectionError:
            logging.error('Error during DebtorsRegister resources JSON id receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            debtors_general_dataset_json = json.loads(debtors_general_dataset_id_json)
            logging.info('A DebtorsRegister resources JSON id received')
        # get ZIP url
        debtors_dataset_zip_url = debtors_general_dataset_json['result']['url']
        return debtors_dataset_zip_url

    @Dataset.measure_execution_time
    def __save_dataset(self, zip_url):
        debtors_col = self.db['Debtors']
        try:
            # get ZIP file
            debtors_dataset_zip = requests.get(zip_url).content
        except OSError:
            logging.error('Error during DebtorsRegisterZIP receiving occurred')
            print('Error during ZIP file receiving occurred!')
        else:
            logging.info('A DebtorsRegister dataset received')
            # get lists of file
            debtors_zip = zipfile.ZipFile(BytesIO(debtors_dataset_zip), 'r')
            # go inside ZIP
            for csvFile in debtors_zip.namelist():
                logging.warning('File in ZIP: ' + str(csvFile))
                debtors_csv_file_name = str(csvFile)
            debtors_zip.extractall()
            debtors_zip.close()
            # read CSV using Dask
            debtors_csv = dd.read_csv(debtors_csv_file_name, encoding='windows-1251', header=None, skiprows=[0],
                                      dtype={1: 'object'}, names=[
                    'DEBTOR_NAME', 'DEBTOR_CODE', 'PUBLISHER', 'EMP_FULL_FIO', 'EMP_ORG', 'ORG_PHONE', 'EMAIL_ADDR',
                    'VP_ORDERNUM', 'VD_CAT'])
            # convert CSV to JSON using Dask
            debtors_csv.to_json('debtorsJson')
            for file in os.listdir('debtorsJson'):
                file_object = open('debtorsJson/' + file, mode='r')
                # map the entire file into memory, size 0 means whole file, normally much faster than buffered i/o
                mm = mmap.mmap(file_object.fileno(), 0,
                               access=mmap.ACCESS_READ)
                # iterate over the block, until next newline
                for line in iter(mm.readline, b''):
                    debtors_json = json.loads(line)
                    try:
                        # save to the collection
                        debtors_col.insert_one(debtors_json)
                    except PyMongoError:
                        logging.error(
                            'Error during saving Debtors Register into Database')
                        print('Error during saving Debtors Register into Database')
                mm.close()
                file_object.close()
            logging.info('Debtors dataset was saved into the database')
            print('The Register "Єдиний реєстр боржників" refreshed')
        finally:
            # delete temp files
            os.remove(debtors_csv_file_name)
            shutil.rmtree('debtorsJson', ignore_errors=True)
        gc.collect()

    @Dataset.measure_execution_time
    def __clear_collection(self):
        if Dataset.is_collection_exists('Debtors'):
            debtors_col = self.db['Debtors']
            count_deleted_documents = debtors_col.delete_many({})
            logging.warning('%s documents deleted. The wanted persons collection is empty.', str(
                count_deleted_documents.deleted_count))

    @Dataset.measure_execution_time
    def __create_service_json(self):
        created_date = datetime.now()
        last_modified_date = datetime.now()
        debtors_col = self.db['Debtors']
        documents_count = debtors_col.count_documents({})
        debtors_register_service_json = {
            '_id': 3,
            'Description': 'Єдиний реєстр боржників',
            'DocumentsCount': documents_count,
            'CreatedDate': str(created_date),
            'LastModifiedDate': str(last_modified_date)
        }
        self.serviceCol.insert_one(debtors_register_service_json)

    @Dataset.measure_execution_time
    def __update_service_json(self):
        last_modified_date = datetime.now()
        debtors_col = self.db['Debtors']
        documents_count = debtors_col.count_documents({})
        self.serviceCol.update_one(
            {'_id': 3},
            {'$set': {'LastModifiedDate': str(last_modified_date),
                      'DocumentsCount': documents_count}}
        )

    @Dataset.measure_execution_time
    def __update_metadata(self):
        collections_list = self.db.list_collection_names()
        # update or create DebtorsRegisterServiceJson
        if (Dataset.is_collection_exists('ServiceCollection')) and (self.serviceCol.count_documents({'_id': 3}, limit=1) != 0):
            self.__update_service_json()
            logging.info('DebtorsRegisterServiceJson updated')
        else:
            self.__create_service_json()
            logging.info('DebtorsRegisterServiceJson created')

    @Dataset.measure_execution_time
    def __delete_collection_index(self):
        if Dataset.is_collection_exists('Debtors'):
            debtors_col = self.db['Debtors']
            if 'full_text' in debtors_col.index_information():
                debtors_col.drop_index('full_text')
                logging.warning('Debtors Text index deleted')

    @Dataset.measure_execution_time
    def __create_collection_index(self):
        debtors_col = self.db['Debtors']
        debtors_col.create_index(
            [('DEBTOR_NAME', 'text')], name='full_text')
        logging.info('Debtors Text Index created')

    @Dataset.measure_execution_time
    def search_into_collection(self, query_string):
        debtors_col = self.db['Debtors']
        try:
            result_count = debtors_col.count_documents(
                {'$text': {'$search': query_string}})
        except PyMongoError:
            logging.error(
                'Error during search into Debtors Register')
            print('Error during search into Debtors Register')
        else:
            if result_count == 0:
                print('The debtors register: No data found')
                logging.warning('The debtors register: No data found')
            else:
                result_table = PrettyTable(['DEBTOR NAME', 'DEBTOR CODE', 'PUBLISHER',
                                            'EXECUTIVE SERVICE', 'EXECUTIVE SERVICE EMPLOYEE', 'CATEGORY'])
                result_table.align = 'l'
                result_table._max_width = {'DEBTOR NAME': 25, 'PUBLISHER': 25, 'EXECUTIVE SERVICE': 35,
                                           'EXECUTIVE SERVICE EMPLOYEE': 25, 'CATEGORY': 25}
                # show only 10 first search results
                for result in debtors_col.find({'$text': {'$search': query_string}}, {'score': {'$meta': 'textScore'}})\
                        .sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                    result_table.add_row([result['DEBTOR_NAME'], result['DEBTOR_CODE'], result['PUBLISHER'],
                                          result['EMP_ORG'], result['EMP_FULL_FIO'], result['VD_CAT']])
                print(result_table.get_string(
                    title=f'The debtors register: {result_count} records found'))
                logging.warning(
                    'The debtors register: %s records found', str(result_count))
                print('Only 10 first search results showed')
                # save all search results into HTML
                for result in debtors_col.find({'$text': {'$search': query_string}}, {'score': {'$meta': 'textScore'}}).\
                        sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                    result_table.add_row([result['DEBTOR_NAME'], result['DEBTOR_CODE'], result['PUBLISHER'],
                                          result['EMP_ORG'], result['EMP_FULL_FIO'], result['VD_CAT']])
                html_result = result_table.get_html_string()
                f = open('results/Debtors.html', 'w', encoding='utf-8')
                f.write(html_result)
                f.close()
                print('All result dataset was saved into Debtors.html')
                logging.warning(
                    'All result dataset was saved into Debtors.html')
        gc.collect()

    @Dataset.measure_execution_time
    def setup_dataset(self):
        self.__delete_collection_index()
        self.__clear_collection()
        __debtors_dataset_zip_url = self.__get_dataset()
        self.__save_dataset(__debtors_dataset_zip_url)
        self.__update_metadata()
        self.__create_collection_index()
