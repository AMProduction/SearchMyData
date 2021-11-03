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

from src.Dataset import Dataset


class DebtorsRegister(Dataset):
    def __init__(self):
        super().__init__()

    @Dataset.measureExecutionTime
    def __getDataset(self):
        print('The register "Єдиний реєстр боржників" is retrieving...')
        try:
            generalDataset = requests.get(
                'https://data.gov.ua/api/3/action/package_show?id=506734bf-2480-448c-a2b4-90b6d06df11e').text
        except:
            logging.error(
                'Error during general DebtorsRegister dataset JSON receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            generalDatasetJson = json.loads(generalDataset)
            logging.info('A general DebtorsRegister dataset JSON received')
        # get dataset id
        debtorsGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']
        try:
            # get resources JSON id
            debtorsGeneralDatasetIdJson = requests.get(
                'https://data.gov.ua/api/3/action/resource_show?id=' + debtorsGeneralDatasetId).text
        except:
            logging.error(
                'Error during DebtorsRegisterr resources JSON id receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            debtorsGeneralDatasetJson = json.loads(debtorsGeneralDatasetIdJson)
            logging.info('A DebtorsRegister resources JSON id received')
        # get ZIP url
        debtorsDatasetZIPUrl = debtorsGeneralDatasetJson['result']['url']
        return debtorsDatasetZIPUrl

    @Dataset.measureExecutionTime
    def __saveDataset(self, zipUrl):
        debtorsCol = self.db['Debtors']
        try:
            # get ZIP file
            debtorsDatasetZIP = requests.get(zipUrl).content
        except:
            logging.error('Error during DebtorsRegisterZIP receiving occured')
            print('Error during ZIP file receiving occured!')
        else:
            logging.info('A DebtorsRegister dataset received')
            # get lists of file
            debtorsZip = zipfile.ZipFile(BytesIO(debtorsDatasetZIP), 'r')
            # go inside ZIP
            for csvFile in debtorsZip.namelist():
                logging.warning('File in ZIP: ' + str(csvFile))
                debtorsCsvFileName = str(csvFile)
            debtorsZip.extractall()
            debtorsZip.close()
            # read CSV using Dask
            debtorsCsv = dd.read_csv(debtorsCsvFileName, encoding='windows-1251', header=None, skiprows=[0], dtype={1: 'object'}, names=[
                                     'DEBTOR_NAME', 'DEBTOR_CODE', 'PUBLISHER', 'EMP_FULL_FIO', 'EMP_ORG', 'ORG_PHONE', 'EMAIL_ADDR', 'VP_ORDERNUM', 'VD_CAT'])
            # convert CSV to JSON using Dask
            debtorsCsv.to_json('debtorsJson')
            for file in os.listdir('debtorsJson'):
                file_object = open('debtorsJson/'+file, mode='r')
                # map the entire file into memory, size 0 means whole file, normally much faster than buffered i/o
                mm = mmap.mmap(file_object.fileno(), 0,
                               access=mmap.ACCESS_READ)
                # iterate over the block, until next newline
                for line in iter(mm.readline, b''):
                    debtorsJson = json.loads(line)
                    try:
                        # save to the collection
                        debtorsCol.insert_one(debtorsJson)
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
            os.remove(debtorsCsvFileName)
            shutil.rmtree('debtorsJson', ignore_errors=True)
        gc.collect()

    @Dataset.measureExecutionTime
    def __clearCollection(self):
        debtorsCol = self.db['Debtors']
        countDeletedDocuments = debtorsCol.delete_many({})
        logging.warning('%s documents deleted. The wanted persons collection is empty.', str(
            countDeletedDocuments.deleted_count))

    @Dataset.measureExecutionTime
    def __createServiceJson(self):
        createdDate = datetime.now()
        lastModifiedDate = datetime.now()
        debtorsCol = self.db['Debtors']
        documentsCount = debtorsCol.count_documents({})
        debtorsRegisterServiceJson = {
            '_id': 3,
            'Description': 'Єдиний реєстр боржників',
            'DocumentsCount': documentsCount,
            'CreatedDate': str(createdDate),
            'LastModifiedDate': str(lastModifiedDate)
        }
        self.serviceCol.insert_one(debtorsRegisterServiceJson)

    @Dataset.measureExecutionTime
    def __updateServiceJson(self):
        lastModifiedDate = datetime.now()
        debtorsCol = self.db['Debtors']
        documentsCount = debtorsCol.count_documents({})
        self.serviceCol.update_one(
            {'_id': 3},
            {'$set': {'LastModifiedDate': str(lastModifiedDate),
                      'DocumentsCount': documentsCount}}
        )

    @Dataset.measureExecutionTime
    def __updateMetadata(self):
        collectionsList = self.db.list_collection_names()
        # update or create DebtorsRegisterServiceJson
        if ('ServiceCollection' in collectionsList) and (self.serviceCol.count_documents({'_id': 3}, limit=1) != 0):
            self.__updateServiceJson()
            logging.info('DebtorsRegisterServiceJson updated')
        else:
            self.__createServiceJson()
            logging.info('DebtorsRegisterServiceJson created')

    @Dataset.measureExecutionTime
    def __deleteCollectionIndex(self):
        debtorsCol = self.db['Debtors']
        if ('full_text' in debtorsCol.index_information()):
            debtorsCol.drop_index('full_text')
            logging.warning('Debtors Text index deleted')

    @Dataset.measureExecutionTime
    def __createCollectionIndex(self):
        debtorsCol = self.db['Debtors']
        debtorsCol.create_index(
            [('DEBTOR_NAME', 'text')], name='full_text')
        logging.info('Debtors Text Index created')

    @Dataset.measureExecutionTime
    def searchIntoCollection(self, queryString):
        debtorsCol = self.db['Debtors']
        resultCount = debtorsCol.count_documents(
            {'$text': {'$search': queryString}})
        if resultCount == 0:
            print('The debtors register: No data found')
            logging.warning('The debtors register: No data found')
        else:
            resultTable = PrettyTable(['DEBTOR NAME', 'DEBTOR CODE', 'PUBLISHER',
                                       'EXECUTIVE SERVICE', 'EXECUTIVE SERVICE EMPLOYEE', 'CATEGORY'])
            resultTable.align = 'l'
            resultTable._max_width = {'DEBTOR NAME': 25, 'PUBLISHER': 25,
                                      'EXECUTIVE SERVICE': 35, 'EXECUTIVE SERVICE EMPLOYEE': 25, 'CATEGORY': 25}
            # show only 10 first search results
            for result in debtorsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'}}).sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                resultTable.add_row([result['DEBTOR_NAME'], result['DEBTOR_CODE'], result['PUBLISHER'],
                                     result['EMP_ORG'], result['EMP_FULL_FIO'], result['VD_CAT']])
            print(resultTable.get_string(
                title='The debtors register: ' + str(resultCount) + ' records found'))
            logging.warning(
                'The debtors register: %s records found', str(resultCount))
            print('Only 10 first search results showed')
            # save all search results into HTML
            for result in debtorsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'}}).sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                resultTable.add_row([result['DEBTOR_NAME'], result['DEBTOR_CODE'], result['PUBLISHER'],
                                     result['EMP_ORG'], result['EMP_FULL_FIO'], result['VD_CAT']])
            htmlResult = resultTable.get_html_string()
            f = open('results/Debtors.html', 'w', encoding='utf-8')
            f.write(htmlResult)
            f.close()
            print('All result dataset was saved into Debtors.html')
            logging.warning('All result dataset was saved into Debtors.html')
        gc.collect()

    @Dataset.measureExecutionTime
    def setupDataset(self):
        self.__deleteCollectionIndex()
        self.__clearCollection()
        __debtorsDatasetZIPUrl = self.__getDataset()
        self.__saveDataset(__debtorsDatasetZIPUrl)
        self.__updateMetadata()
        self.__createCollectionIndex()
