import gc
import json
import logging
import os
import shutil
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from io import BytesIO
from pymongo.errors import PyMongoError

import requests
from prettytable import PrettyTable

from src.dataset import Dataset


class LustratedPersonsRegister(Dataset):
    def __init__(self):
        super().__init__()

    @Dataset.measure_execution_time
    def get_dataset(self):
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
    def save_dataset(self, zip_url):
        lustrated_col = self.db['Lustrated']
        try:
            # get ZIP file
            lustrated_dataset_zip = requests.get(zip_url).content
        except OSError:
            logging.error('Error during LustratedPersonsRegister ZIP receiving occurred')
            print('Error during ZIP file receiving occurred!')
        else:
            logging.info('A LustratedPersonsRegister dataset received')
            # get lists of file
            lustrated_zip = zipfile.ZipFile(
                BytesIO(lustrated_dataset_zip), 'r')
            # go inside ZIP
            for xmlFile in lustrated_zip.namelist():
                # skip root folder
                if xmlFile.endswith('/'):
                    root_folder_name = xmlFile
                    continue
                logging.warning('File in ZIP: ' + str(xmlFile))
            # unzip all files
            lustrated_zip.extractall('Temp')
            for xmlFile in os.listdir('Temp/' + root_folder_name):
                # read the entrepreneurs Xml file
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
            print('The Register "Єдиний державний реєстр осіб, щодо яких застосовано положення Закону України «Про очищення влади»" refreshed')
        finally:
            # delete temp files
            shutil.rmtree('Temp', ignore_errors=True)
        gc.collect()
