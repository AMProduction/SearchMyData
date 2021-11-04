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

from src.Dataset import Dataset


class LegalEntitiesRegister(Dataset):
    def __init__(self):
        super().__init__()

    @Dataset.measureExecutionTime
    def getDataset(self):
        print('The register "Єдиний державний реєстр юридичних осіб, фізичних осіб – підприємців та громадських формувань" is retrieving...')
        try:
            generalDataset = requests.get(
                'https://data.gov.ua/api/3/action/package_show?id=1c7f3815-3259-45e0-bdf1-64dca07ddc10').text
        except:
            logging.error(
                'Error during general EntrepreneursRegister dataset JSON receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            generalDatasetJson = json.loads(generalDataset)
            logging.info(
                'A general EntrepreneursRegister dataset JSON received')
        # get dataset id
        entrepreneursGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']
        try:
            # get resources JSON id
            entrepreneursGeneralDatasetIdJson = requests.get(
                'https://data.gov.ua/api/3/action/resource_show?id=' + entrepreneursGeneralDatasetId).text
        except:
            logging.error(
                'Error during EntrepreneursRegister resources JSON id receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            entrepreneursGeneralDatasetJson = json.loads(
                entrepreneursGeneralDatasetIdJson)
            logging.info('A EntrepreneursRegister resources JSON id received')
        # get ZIP url
        entrepreneursDatasetZIPUrl = entrepreneursGeneralDatasetJson['result']['url']
        return entrepreneursDatasetZIPUrl

    @Dataset.measureExecutionTime
    def saveDataset(self, zipUrl):
        entrepreneursCol = self.db['Entrepreneurs']
        legalEntitiesCol = self.db['LegalEntities']
        try:
            # get ZIP file
            entrepreneursDatasetZIP = requests.get(zipUrl).content
        except:
            logging.error(
                'Error during EntrepreneursRegister ZIP receiving occured')
            print('Error during ZIP file receiving occured!')
        else:
            logging.info('A EntrepreneursRegister dataset received')
            # get lists of file
            entrepreneursZip = zipfile.ZipFile(
                BytesIO(entrepreneursDatasetZIP), 'r')
            # go inside ZIP
            for xmlFile in entrepreneursZip.namelist():
                # skip root folder
                if xmlFile.endswith('/'):
                    rootFolderName = xmlFile
                    continue
                logging.warning('File in ZIP: ' + str(xmlFile))
            # unzip all files
            entrepreneursZip.extractall('Temp')
            for xmlFile in os.listdir('Temp/'+rootFolderName):
                if xmlFile.find('_UO_') != -1:
                    # read the legal Entities Xml file
                    pathToFile = 'Temp/' + rootFolderName + xmlFile
                    # parse xml
                    legalEntitiesJson = {}
                    tree = ET.parse(pathToFile)
                    xml_data = tree.getroot()
                    for record in xml_data:
                        name = record.find('NAME').text
                        shortName = record.find('SHORT_NAME').text
                        edrpou = record.find('EDRPOU').text
                        address = record.find('ADDRESS').text
                        kved = record.find('KVED').text
                        boss = record.find('BOSS').text
                        beneficiariesDict = {}
                        beneficiaryNumber = 1
                        for beneficiaries in record.iter('BENEFICIARIES'):
                            if beneficiaries.find('BENEFICIARY') is not None:
                                for beneficiary in beneficiaries.iter('BENEFICIARY'):
                                    beneficiaryToDict = beneficiary.text
                                    key = 'beneficiary' + \
                                        str(beneficiaryNumber)
                                    beneficiariesDict[key] = beneficiaryToDict
                                    beneficiaryNumber += 1
                        foundersDict = {}
                        foundersNumber = 1
                        for founders in record.iter('FOUNDERS'):
                            if founders.find('FOUNDER') is not None:
                                for founder in founders.iter('FOUNDER'):
                                    founderToDict = founder.text
                                    key = 'founder' + str(foundersNumber)
                                    foundersDict[key] = founderToDict
                                    foundersNumber += 1
                        stan = record.find('STAN').text
                        legalEntitiesJson = {
                            'name': name,
                            'short_name': shortName,
                            'edrpou': edrpou,
                            'address': address,
                            'kved': kved,
                            'boss': boss,
                            'beneficiaries': beneficiariesDict,
                            'founders': foundersDict,
                            'stan': stan
                        }
                        try:
                            # save to the collection
                            legalEntitiesCol.insert_one(legalEntitiesJson)
                        except PyMongoError:
                            logging.error(
                                'Error during saving Legal Entities Register into Database')
                            print(
                                'Error during saving Legal Entities Register into Database')
                    logging.info(
                        'LegalEntities dataset was saved into the database')
                if xmlFile.find('_FOP_') != -1:
                    # read the entrepreneurs Xml file
                    pathToFile = 'Temp/' + rootFolderName + xmlFile
                    # parse xml
                    entrepreneursJson = {}
                    tree = ET.parse(pathToFile)
                    xml_data = tree.getroot()
                    for record in xml_data:
                        fio = record.find('FIO').text
                        address = record.find('ADDRESS').text
                        kved = record.find('KVED').text
                        stan = record.find('STAN').text
                        entrepreneursJson = {
                            'fio': fio,
                            'address': address,
                            'kved': kved,
                            'stan': stan
                        }
                        try:
                            # save to the collection
                            entrepreneursCol.insert_one(entrepreneursJson)
                        except PyMongoError:
                            logging.error(
                                'Error during saving Entrepreneurs Register into Database')
                            print(
                                'Error during saving Entrepreneurs Register into Database')
                    logging.info(
                        'Entrepreneurs dataset was saved into the database')
            print('The Register "Єдиний державний реєстр юридичних осіб, фізичних осіб – підприємців та громадських формувань" refreshed')
        finally:
            # delete temp files
            shutil.rmtree('Temp', ignore_errors=True)
        gc.collect()

    @Dataset.measureExecutionTime
    def clearCollection(self):
        legalEntitiesCol = self.db['LegalEntities']
        countDeletedDocuments = legalEntitiesCol.delete_many({})
        logging.warning('%s documents deleted. The legal entities collection is empty.', str(
            countDeletedDocuments.deleted_count))

    @Dataset.measureExecutionTime
    def __createServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        legalEntitiesCol = self.db['LegalEntities']
        documentsCount = legalEntitiesCol.count_documents({})
        legalEntitiesRegisterServiceJson = {
            '_id': 4,
            'Description': 'Єдиний державний реєстр юридичних осіб та громадських формувань',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.serviceCol.insert_one(legalEntitiesRegisterServiceJson)

    @Dataset.measureExecutionTime
    def __updateServiceJson(self):
        lastModifiedDate = datetime.now()
        legalEntitiesCol = self.db['LegalEntities']
        documentsCount = legalEntitiesCol.count_documents({})
        self.serviceCol.update_one(
            {'_id': 4},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )

    @Dataset.measureExecutionTime
    def updateMetadata(self):
        collectionsList = self.db.list_collection_names()
        # update or create LegalEntitiesRegisterServiceJson
        if ('ServiceCollection' in collectionsList) and (self.serviceCol.count_documents({'_id': 4}, limit=1) != 0):
            self.__updateServiceJson()
            logging.info('LegalEntitiesRegisterServiceJson updated')
        else:
            self.__createServiceJson()
            logging.info('LegalEntitiesRegisterServiceJson created')

    @Dataset.measureExecutionTime
    def deleteCollectionIndex(self):
        legalEntitiesCol = self.db['LegalEntities']
        if ('full_text' in legalEntitiesCol.index_information()):
            legalEntitiesCol.drop_index('full_text')
            logging.warning('LegalEntities Text index deleted')

    @Dataset.measureExecutionTime
    def createCollectionIndex(self):
        legalEntitiesCol = self.db['LegalEntities']
        legalEntitiesCol.create_index([('short_name', 'text'), ('edrpou', 'text'), (
            'boss', 'text'), ('beneficiaries', 'text'), ('founders', 'text')], name='full_text')
        logging.info('LegalEntities Text Index created')

    @Dataset.measureExecutionTime
    def searchIntoCollection(self, queryString):
        legalEntitiesCol = self.db['LegalEntities']
        try:
            resultCount = legalEntitiesCol.count_documents(
                {'$text': {'$search': queryString}})
        except PyMongoError:
            logging.error(
                'Error during search into Legal Entities Register')
            print('Error during search into Legal Entities Register')
        else:
            if resultCount == 0:
                print('The legal entities register: No data found')
                logging.warning('The legal entities register: No data found')
            else:
                resultTable = PrettyTable(
                    ['SHORT NAME', 'EDRPOU', 'ADDRESS', 'KVED', 'BOSS', 'FOUNDERS', 'STATE'])
                resultTable.align = 'l'
                resultTable._max_width = {
                    'SHORT NAME': 25, 'ADDRESS': 25, 'KVED': 30, 'BOSS': 25, 'FOUNDERS': 25}
                # show only 10 first search results
                for result in legalEntitiesCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'}}).sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                    resultTable.add_row([result['short_name'], result['edrpou'], result['address'],
                                        result['kved'], result['boss'], result['founders'], result['stan']])
                print(resultTable.get_string(
                    title='The legal entities register: ' + str(resultCount) + ' records found'))
                logging.warning(
                    'The legal entities register: %s records found', str(resultCount))
                print('Only 10 first search results showed')
                # save all search results into HTML
                for result in legalEntitiesCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'}}).sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                    resultTable.add_row([result['short_name'], result['edrpou'], result['address'],
                                        result['kved'], result['boss'], result['founders'], result['stan']])
                htmlResult = resultTable.get_html_string()
                f = open('results/LegalEntities.html', 'w', encoding='utf-8')
                f.write(htmlResult)
                f.close()
                print('All result dataset was saved into LegalEntities.html')
                logging.warning(
                    'All result dataset was saved into LegalEntities.html')
        gc.collect()
