import gc
import json
import logging
import os
import shutil
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from io import BytesIO

import requests
from prettytable import PrettyTable
from pymongo.errors import PyMongoError

from src.dataset import Dataset


class LustratedPersonsRegister(Dataset):
    def __init__(self):
        super().__init__()

    @Dataset.measure_execution_time
    def __get_dataset(self):
        print(
            'The register "Єдиний державний реєстр осіб, щодо яких застосовано положення Закону України «Про очищення влади»" is retrieving...')
        try:
            general_dataset = requests.get(
                'https://data.gov.ua/api/3/action/package_show?id=8faa71c1-3a54-45e8-8f6e-06c92b1ff8bc').text
        except ConnectionError:
            logging.error('Error during general LustratedPersonsRegister dataset JSON receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            general_dataset_json = json.loads(general_dataset)
            logging.info('A general LustratedPersonsRegister dataset JSON received')
        # get dataset id
        lustrated_persons_general_dataset_id = general_dataset_json['result']['resources'][0]['id']
        try:
            # get resources JSON id
            lustrated_persons_general_dataset_id_json = requests.get(
                'https://data.gov.ua/api/3/action/resource_show?id=' + lustrated_persons_general_dataset_id).text
        except ConnectionError:
            logging.error('Error during LustratedPersonsRegister resources JSON id receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            lustrated_persons_general_dataset_json = json.loads(lustrated_persons_general_dataset_id_json)
            logging.info('A LustratedPersonsRegister resources JSON id received')
        # get ZIP url
        lustrated_persons_dataset_zip_url = lustrated_persons_general_dataset_json['result']['url']
        return lustrated_persons_dataset_zip_url

    @Dataset.measure_execution_time
    def __save_dataset(self, zip_url):
        lustrated_col = self.db['Lustrated']
        try:
            # get ZIP file
            lustrated_dataset_zip = requests.get(zip_url).content
        except OSError:
            logging.error('Error during LustratedPersonsRegister ZIP receiving occurred')
            print('Error during ZIP file receiving occurred!')
        else:
            logging.info('A LustratedPersonsRegister dataset received')
            # get lists of files
            lustrated_zip = zipfile.ZipFile(
                BytesIO(lustrated_dataset_zip), 'r')
            # go inside ZIP
            root_folder_name = ''
            for xmlFile in lustrated_zip.namelist():
                # skip root folder
                if xmlFile.endswith('/'):
                    root_folder_name = xmlFile
                    continue
                logging.warning('File in ZIP: ' + str(xmlFile))
            # unzip
            lustrated_zip.extractall('Temp')
            lustrated_zip.close()
            for xmlFile in os.listdir('Temp/' + root_folder_name):
                # read the lustrated persons Xml file
                path_to_file = 'Temp/' + root_folder_name + xmlFile
                # parse xml
                lustrated_json = {}
                tree = ET.parse(path_to_file)
                xml_data = tree.getroot()
                for record in xml_data:
                    fio = record.find('FIO').text
                    job = record.find('JOB').text
                    judgment_composition = record.find('JUDGMENT_COMPOSITION').text
                    period = record.find('PERIOD').text
                    lustrated_json = {
                        'fio': fio,
                        'job': job,
                        'judgment_composition': judgment_composition,
                        'period': period
                    }
                    try:
                        # save to the collection
                        lustrated_col.insert_one(lustrated_json)
                    except PyMongoError:
                        logging.error(
                            'Error during saving Lustrated Persons Register into Database')
                        print(
                            'Error during saving Lustrated Persons Register into Database')
                logging.info(
                    'Lustrated Persons dataset was saved into the database')
            print(
                'The Register "Єдиний державний реєстр осіб, щодо яких застосовано положення Закону України «Про очищення влади»" refreshed')
        finally:
            # delete temp files
            shutil.rmtree('Temp', ignore_errors=True)
        gc.collect()

    @Dataset.measure_execution_time
    def __clear_collection(self):
        if self.is_collection_exists('Lustrated'):
            lustrated_col = self.db['Lustrated']
            count_deleted_documents = lustrated_col.delete_many({})
            logging.warning('%s documents deleted. The Lustrated Persons collection is empty.',
                            str(count_deleted_documents.deleted_count))

    @Dataset.measure_execution_time
    def __create_service_json(self):
        created_date = datetime.now()
        last_modified_date = datetime.now()
        lustrated_col = self.db['Lustrated']
        documents_count = lustrated_col.count_documents({})
        lustrated_register_service_json = {
            '_id': 6,
            'Description': 'Єдиний державний реєстр осіб, щодо яких застосовано положення Закону України «Про очищення влади»',
            'DocumentsCount': documents_count,
            'CreatedDate': str(created_date),
            'LastModifiedDate': str(last_modified_date)
        }
        self.serviceCol.insert_one(lustrated_register_service_json)

    @Dataset.measure_execution_time
    def __update_service_json(self):
        last_modified_date = datetime.now()
        lustrated_col = self.db['Lustrated']
        documents_count = lustrated_col.count_documents({})
        self.serviceCol.update_one(
            {'_id': 6},
            {'$set': {'LastModifiedDate': str(last_modified_date),
                      'DocumentsCount': documents_count}}
        )

    @Dataset.measure_execution_time
    def __update_metadata(self):
        # update or create LustratedPersonsRegisterServiceJson
        if (self.is_collection_exists('ServiceCollection')) and (
                self.serviceCol.count_documents({'_id': 6}, limit=1) != 0):
            self.__update_service_json()
            logging.info('LustratedPersonsRegisterServiceJson updated')
        else:
            self.__create_service_json()
            logging.info('LustratedPersonsRegisterServiceJson created')

    @Dataset.measure_execution_time
    def __delete_collection_index(self):
        if self.is_collection_exists('Lustrated'):
            lustrated_col = self.db['Lustrated']
            if 'full_text' in lustrated_col.index_information():
                lustrated_col.drop_index('full_text')
                logging.warning('Lustrated Text index deleted')

    @Dataset.measure_execution_time
    def __create_collection_index(self):
        lustrated_col = self.db['Lustrated']
        lustrated_col.create_index([('fio', 'text')], name='full_text')
        logging.info('Lustrated Text Index created')

    @Dataset.measure_execution_time
    def search_into_collection(self, query_string):
        lustrated_col = self.db['Lustrated']
        try:
            result_count = lustrated_col.count_documents(
                {'$text': {'$search': query_string}})
        except PyMongoError:
            logging.error('Error during search into The Lustrated Persons Register')
            print('Error during search into The Lustrated Persons Register')
        else:
            if result_count == 0:
                print('The The Lustrated Persons register: No data found')
                logging.warning('The Lustrated Persons register: No data found')
            else:
                result_table = PrettyTable(['NAME', 'JOB', 'JUDGMENT COMPOSITION', 'PERIOD'])
                result_table.align = 'l'
                result_table._max_width = {
                    'NAME': 40, 'JOB': 40, 'JUDGMENT COMPOSITION': 30}
                # show only 10 first search results
                for result in lustrated_col.find({'$text': {'$search': query_string}},
                                                 {'score': {'$meta': 'textScore'}}) \
                        .sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                    result_table.add_row(
                        [result['fio'], result['job'], result['judgment_composition'], result['period']])
                print(result_table.get_string(
                    title=f'The Lustrated Persons register: {result_count} records found'))
                logging.warning(
                    'The Lustrated Persons register: %s records found', str(result_count))
                print('Only 10 first search results showed')
                # save all search results into HTML
                for result in lustrated_col.find({'$text': {'$search': query_string}},
                                                 {'score': {'$meta': 'textScore'}}) \
                        .sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                    result_table.add_row(
                        [result['fio'], result['job'], result['judgment_composition'], result['period']])
                html_result = result_table.get_html_string()
                f = open('results/Lustrated.html', 'w', encoding='utf-8')
                f.write(html_result)
                f.close()
                print('All result dataset was saved into Lustrated.html')
                logging.warning(
                    'All result dataset was saved into Lustrated.html')
        gc.collect()

    @Dataset.measure_execution_time
    def setup_dataset(self):
        self.__delete_collection_index()
        self.__clear_collection()
        __lustrated_dataset_zip_url = self.__get_dataset()
        self.__save_dataset(__lustrated_dataset_zip_url)
        self.__update_metadata()
        self.__create_collection_index()
