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


class LegalEntitiesRegister(Dataset):
    def __init__(self):
        super().__init__()

    @Dataset.measure_execution_time
    def get_dataset(self):
        print(
            'The register "Єдиний державний реєстр юридичних осіб, фізичних осіб – підприємців та громадських формувань" is retrieving...')
        try:
            general_dataset = requests.get(
                'https://data.gov.ua/api/3/action/package_show?id=1c7f3815-3259-45e0-bdf1-64dca07ddc10').text
        except ConnectionError:
            logging.error('Error during general EntrepreneursRegister dataset JSON receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            general_dataset_json = json.loads(general_dataset)
            logging.info('A general EntrepreneursRegister dataset JSON received')
        # get dataset id
        entrepreneurs_general_dataset_id = general_dataset_json['result']['resources'][0]['id']
        try:
            # get resources JSON id
            entrepreneurs_general_dataset_id_json = requests.get(
                'https://data.gov.ua/api/3/action/resource_show?id=' + entrepreneurs_general_dataset_id).text
        except ConnectionError:
            logging.error('Error during EntrepreneursRegister resources JSON id receiving occurred')
            print('Error during dataset receiving occurred!')
        else:
            entrepreneurs_general_dataset_json = json.loads(entrepreneurs_general_dataset_id_json)
            logging.info('A EntrepreneursRegister resources JSON id received')
        # get ZIP url
        entrepreneurs_dataset_zip_url = entrepreneurs_general_dataset_json['result']['url']
        return entrepreneurs_dataset_zip_url

    @Dataset.measure_execution_time
    def save_dataset(self, zip_url):
        entrepreneurs_col = self.db['Entrepreneurs']
        legal_entities_col = self.db['LegalEntities']
        try:
            # get ZIP file
            entrepreneurs_dataset_zip = requests.get(zip_url).content
        except OSError:
            logging.error('Error during EntrepreneursRegister ZIP receiving occurred')
            print('Error during ZIP file receiving occurred!')
        else:
            logging.info('A EntrepreneursRegister dataset received')
            # get lists of file
            entrepreneurs_zip = zipfile.ZipFile(
                BytesIO(entrepreneurs_dataset_zip), 'r')
            # go inside ZIP
            for xmlFile in entrepreneurs_zip.namelist():
                # skip root folder
                if xmlFile.endswith('/'):
                    root_folder_name = xmlFile
                    continue
                logging.warning('File in ZIP: ' + str(xmlFile))
            # unzip all files
            entrepreneurs_zip.extractall('Temp')
            for xmlFile in os.listdir('Temp/' + root_folder_name):
                if xmlFile.find('_UO_') != -1:
                    # read the legal Entities Xml file
                    path_to_file = 'Temp/' + root_folder_name + xmlFile
                    # parse xml
                    legal_entities_json = {}
                    tree = ET.parse(path_to_file)
                    xml_data = tree.getroot()
                    for record in xml_data:
                        name = record.find('NAME').text
                        short_name = record.find('SHORT_NAME').text
                        edrpou = record.find('EDRPOU').text
                        address = record.find('ADDRESS').text
                        kved = record.find('KVED').text
                        boss = record.find('BOSS').text
                        beneficiaries_dict = {}
                        beneficiary_number = 1
                        for beneficiaries in record.iter('BENEFICIARIES'):
                            if beneficiaries.find('BENEFICIARY') is not None:
                                for beneficiary in beneficiaries.iter('BENEFICIARY'):
                                    beneficiary_to_dict = beneficiary.text
                                    key = 'beneficiary' + str(beneficiary_number)
                                    beneficiaries_dict[key] = beneficiary_to_dict
                                    beneficiary_number += 1
                        founders_dict = {}
                        founders_number = 1
                        for founders in record.iter('FOUNDERS'):
                            if founders.find('FOUNDER') is not None:
                                for founder in founders.iter('FOUNDER'):
                                    founder_to_dict = founder.text
                                    key = 'founder' + str(founders_number)
                                    founders_dict[key] = founder_to_dict
                                    founders_number += 1
                        stan = record.find('STAN').text
                        legal_entities_json = {
                            'name': name,
                            'short_name': short_name,
                            'edrpou': edrpou,
                            'address': address,
                            'kved': kved,
                            'boss': boss,
                            'beneficiaries': beneficiaries_dict,
                            'founders': founders_dict,
                            'stan': stan
                        }
                        try:
                            # save to the collection
                            legal_entities_col.insert_one(legal_entities_json)
                        except PyMongoError:
                            logging.error(
                                'Error during saving Legal Entities Register into Database')
                            print(
                                'Error during saving Legal Entities Register into Database')
                    logging.info(
                        'LegalEntities dataset was saved into the database')
                if xmlFile.find('_FOP_') != -1:
                    # read the entrepreneurs Xml file
                    path_to_file = 'Temp/' + root_folder_name + xmlFile
                    # parse xml
                    entrepreneurs_json = {}
                    tree = ET.parse(path_to_file)
                    xml_data = tree.getroot()
                    for record in xml_data:
                        fio = record.find('FIO').text
                        address = record.find('ADDRESS').text
                        kved = record.find('KVED').text
                        stan = record.find('STAN').text
                        entrepreneurs_json = {
                            'fio': fio,
                            'address': address,
                            'kved': kved,
                            'stan': stan
                        }
                        try:
                            # save to the collection
                            entrepreneurs_col.insert_one(entrepreneurs_json)
                        except PyMongoError:
                            logging.error(
                                'Error during saving Entrepreneurs Register into Database')
                            print(
                                'Error during saving Entrepreneurs Register into Database')
                    logging.info(
                        'Entrepreneurs dataset was saved into the database')
            print(
                'The Register "Єдиний державний реєстр юридичних осіб, фізичних осіб – підприємців та громадських формувань" refreshed')
        finally:
            # delete temp files
            shutil.rmtree('Temp', ignore_errors=True)
        gc.collect()

    @Dataset.measure_execution_time
    def clear_collection(self):
        if self.is_collection_exists('LegalEntities'):
            legal_entities_col = self.db['LegalEntities']
            count_deleted_documents = legal_entities_col.delete_many({})
            logging.warning('%s documents deleted. The legal entities collection is empty.',
                            str(count_deleted_documents.deleted_count))

    @Dataset.measure_execution_time
    def __create_service_json(self):
        created_date = datetime.now()
        last_modified_date = datetime.now()
        legal_entities_col = self.db['LegalEntities']
        documents_count = legal_entities_col.count_documents({})
        legal_entities_register_service_json = {
            '_id': 4,
            'Description': 'Єдиний державний реєстр юридичних осіб та громадських формувань',
            'DocumentsCount': documents_count,
            'CreatedDate': str(created_date),
            'LastModifiedDate': str(last_modified_date)
        }
        self.serviceCol.insert_one(legal_entities_register_service_json)

    @Dataset.measure_execution_time
    def __update_service_json(self):
        last_modified_date = datetime.now()
        legal_entities_col = self.db['LegalEntities']
        documents_count = legal_entities_col.count_documents({})
        self.serviceCol.update_one(
            {'_id': 4},
            {'$set': {'LastModifiedDate': str(last_modified_date),
                      'DocumentsCount': documents_count}}
        )

    @Dataset.measure_execution_time
    def update_metadata(self):
        # update or create LegalEntitiesRegisterServiceJson
        if (self.is_collection_exists('ServiceCollection')) and (
                self.serviceCol.count_documents({'_id': 4}, limit=1) != 0):
            self.__update_service_json()
            logging.info('LegalEntitiesRegisterServiceJson updated')
        else:
            self.__create_service_json()
            logging.info('LegalEntitiesRegisterServiceJson created')

    @Dataset.measure_execution_time
    def delete_collection_index(self):
        if self.is_collection_exists('LegalEntities'):
            legal_entities_col = self.db['LegalEntities']
            if 'full_text' in legal_entities_col.index_information():
                legal_entities_col.drop_index('full_text')
                logging.warning('LegalEntities Text index deleted')

    @Dataset.measure_execution_time
    def create_collection_index(self):
        legal_entities_col = self.db['LegalEntities']
        legal_entities_col.create_index([('short_name', 'text'), ('edrpou', 'text'), ('boss', 'text'),
                                         ('beneficiaries', 'text'), ('founders', 'text')], name='full_text')
        logging.info('LegalEntities Text Index created')

    @Dataset.measure_execution_time
    def search_into_collection(self, query_string):
        legal_entities_col = self.db['LegalEntities']
        try:
            result_count = legal_entities_col.count_documents(
                {'$text': {'$search': query_string}})
        except PyMongoError:
            logging.error(
                'Error during search into Legal Entities Register')
            print('Error during search into Legal Entities Register')
        else:
            if result_count == 0:
                print('The legal entities register: No data found')
                logging.warning('The legal entities register: No data found')
            else:
                result_table = PrettyTable(
                    ['SHORT NAME', 'EDRPOU', 'ADDRESS', 'KVED', 'BOSS', 'FOUNDERS', 'STATE'])
                result_table.align = 'l'
                result_table._max_width = {'SHORT NAME': 25, 'ADDRESS': 25, 'KVED': 30, 'BOSS': 25, 'FOUNDERS': 25}
                # show only 10 first search results
                for result in legal_entities_col.find({'$text': {'$search': query_string}},
                                                      {'score': {'$meta': 'textScore'}}) \
                        .sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                    result_table.add_row([result['short_name'], result['edrpou'], result['address'], result['kved'],
                                          result['boss'], result['founders'], result['stan']])
                print(result_table.get_string(
                    title=f'The legal entities register: {result_count} records found'))
                logging.warning(
                    'The legal entities register: %s records found', str(result_count))
                print('Only 10 first search results showed')
                # save all search results into HTML
                for result in legal_entities_col.find({'$text': {'$search': query_string}},
                                                      {'score': {'$meta': 'textScore'}}) \
                        .sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                    result_table.add_row([result['short_name'], result['edrpou'], result['address'], result['kved'],
                                          result['boss'], result['founders'], result['stan']])
                html_result = result_table.get_html_string()
                f = open('results/LegalEntities.html', 'w', encoding='utf-8')
                f.write(html_result)
                f.close()
                print('All result dataset was saved into LegalEntities.html')
                logging.warning(
                    'All result dataset was saved into LegalEntities.html')
        gc.collect()
