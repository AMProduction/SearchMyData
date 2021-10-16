import pymongo
from prettytable import PrettyTable
import logging
from pathlib import Path
import json
from pymongo.errors import ServerSelectionTimeoutError
from datetime import datetime
import gc

class SearchEngine:
        
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.__configJsonFilePath = Path('config.json')
        #check if config.json exists
        if self.__configJsonFilePath.is_file():
            logging.warning('Config.json is found')
            self.__configJsonFile = open(self.__configJsonFilePath)
            #try to read json
            try:
                self.__configJson = json.loads(self.__configJsonFile.read())
            except ValueError:
                logging.error('Config.json format error')
                logging.info('The application closed')
                print('Config.json format error')
                print('Quitting...')
                exit()
            #read db connection string
            try:
                self.__dbstring = self.__configJson['dbstring']
            except KeyError:
                logging.error('"dbstring" key is not found in Config.json')
                logging.info('The application closed')
                print('Config.json format error')
                print('Quitting...')
                exit()                          
            #try to connect
            try:
                maxSevSelDelay = 3 #Set server Selection Timeout in ms. The default value is 30s.
                self.__dbserver = pymongo.MongoClient(self.__dbstring, serverSelectionTimeoutMS = maxSevSelDelay)
                self.__dbserver.server_info() #force connection on a request
            except ServerSelectionTimeoutError:
                logging.error('Connection error')
                logging.info('The application closed')
                print('Connection error')
                print('Quitting...')
                exit()
            else:
                self.__db = self.__dbserver["searchmydata"]
        #if config.json does not exists
        else:
            logging.error('Config.json is not found')
            logging.info('The application closed')
            print('Config.json is not found')
            print('Quitting...')
            exit()
        
    def search(self, queryString):      
        print('Search results'.center(80, '#'))
        logging.warning('Query string: "%s"', queryString)
        self.__searchMissingPersons(queryString)
        self.__searchWantedPersons(queryString)
        self.__searchDebtors(queryString)
        self.__searchLegalEntities(queryString)
        self.__searchEntrepreneurs(queryString)
        gc.collect()
                
    def __searchMissingPersons(self, queryString):
        start_time = datetime.now()
        missingPersonsCol = self.__db['MissingPersons']
        resultCount = missingPersonsCol.count_documents({'$text': {'$search': queryString}})
        if resultCount == 0:
            print('The missing persons register: No data found')
            logging.warning('The missing persons register: No data found')
        else:
            resultTable = PrettyTable(['LAST NAME', 'FIRST NAME', 'MIDDLE NAME', 'BIRTH DATE', 'LOST PLACE', 'LOST DATE'])
            resultTable.align = 'l'
            #show only 10 first search results
            for result in missingPersonsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'} }).sort([('score', {'$meta': 'textScore'})]).limit(10):
                resultTable.add_row([result['LAST_NAME_E'], result['FIRST_NAME_E'], result['MIDDLE_NAME_E'], '{:.10}'.format(result['BIRTH_DATE']), result['LOST_PLACE'], '{:.10}'.format(result['LOST_DATE'])])
            print(resultTable.get_string(title = 'The missing persons register: ' + str(resultCount) + ' records found'))
            logging.warning('The missing persons register: %s records found', str(resultCount))
            print('Only 10 first search results showed')
            #save all search results into HTML
            for result in missingPersonsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'} }).sort([('score', {'$meta': 'textScore'})]):
                resultTable.add_row([result['LAST_NAME_E'], result['FIRST_NAME_E'], result['MIDDLE_NAME_E'], '{:.10}'.format(result['BIRTH_DATE']), result['LOST_PLACE'], '{:.10}'.format(result['LOST_DATE'])])
            htmlResult = resultTable.get_html_string()
            f = open('results/MissingPersons.html', 'w', encoding='utf-8')
            f.write(htmlResult)
            f.close()
            print('All result dataset was saved into MissingPersons.html')
            logging.warning('All result dataset was saved into MissingPersons.html')
        end_time = datetime.now()
        logging.info('Search time into the missing person register: ' + str(end_time-start_time))
        gc.collect()
                    
    def __searchWantedPersons(self, queryString):
        start_time = datetime.now()
        wantedPersonsCol = self.__db['WantedPersons']
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
    
    def __searchDebtors(self, queryString):
        start_time = datetime.now()
        debtorsCol = self.__db['Debtors']
        resultCount = debtorsCol.count_documents({'$text': {'$search': queryString}})
        if resultCount == 0:
            print('The debtors register: No data found')
            logging.warning('The debtors register: No data found')
        else:
            resultTable = PrettyTable(['DEBTOR NAME', 'DEBTOR CODE', 'PUBLISHER', 'EXECUTIVE SERVICE', 'EXECUTIVE SERVICE EMPLOYEE', 'CATEGORY'])
            resultTable.align = 'l'
            resultTable._max_width = {'DEBTOR NAME': 25, 'PUBLISHER': 25, 'EXECUTIVE SERVICE': 35, 'EXECUTIVE SERVICE EMPLOYEE': 25, 'CATEGORY': 25}
            #show only 10 first search results
            for result in debtorsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'}}).sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                resultTable.add_row([result['DEBTOR_NAME'], result['DEBTOR_CODE'], result['PUBLISHER'], result['EMP_ORG'], result['EMP_FULL_FIO'], result['VD_CAT']])
            print(resultTable.get_string(title = 'The debtors register: ' + str(resultCount) + ' records found'))
            logging.warning('The debtors register: %s records found', str(resultCount))
            print('Only 10 first search results showed')
            #save all search results into HTML
            for result in debtorsCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'}}).sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                resultTable.add_row([result['DEBTOR_NAME'], result['DEBTOR_CODE'], result['PUBLISHER'], result['EMP_ORG'], result['EMP_FULL_FIO'], result['VD_CAT']])
            htmlResult = resultTable.get_html_string()
            f = open('results/Debtors.html', 'w', encoding='utf-8')
            f.write(htmlResult)
            f.close()
            print('All result dataset was saved into Debtors.html')
            logging.warning('All result dataset was saved into Debtors.html')
        end_time = datetime.now()
        logging.info('Search time into the debtors register: ' + str(end_time-start_time))
        gc.collect()
        
    def __searchLegalEntities(self, queryString):
        start_time = datetime.now()
        legalEntitiesCol = self.__db['LegalEntities']
        resultCount = legalEntitiesCol.count_documents({'$text': {'$search': queryString}})
        if resultCount == 0:
            print('The legal entities register: No data found')
            logging.warning('The legal entities register: No data found')
        else:
            resultTable = PrettyTable(['SHORT NAME', 'EDRPOU', 'ADDRESS', 'KVED', 'BOSS', 'FOUNDERS', 'STATE'])
            resultTable.align = 'l'
            resultTable._max_width = {'SHORT NAME': 25, 'ADDRESS': 25, 'KVED': 30, 'BOSS': 25, 'FOUNDERS':25}
            #show only 10 first search results
            for result in legalEntitiesCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'} }).sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                resultTable.add_row([result['short_name'], result['edrpou'], result['address'], result['kved'], result['boss'], result['founders'], result['stan']])
            print(resultTable.get_string(title = 'The legal entities register: ' + str(resultCount) + ' records found'))
            logging.warning('The legal entities register: %s records found', str(resultCount))
            print('Only 10 first search results showed')
            #save all search results into HTML
            for result in legalEntitiesCol.find({'$text': {'$search': queryString}}, {'score': {'$meta': 'textScore'} }).sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                resultTable.add_row([result['short_name'], result['edrpou'], result['address'], result['kved'], result['boss'], result['founders'], result['stan']])
            htmlResult = resultTable.get_html_string()
            f = open('results/LegalEntities.html', 'w', encoding='utf-8')
            f.write(htmlResult)
            f.close()
            print('All result dataset was saved into LegalEntities.html')
            logging.warning('All result dataset was saved into LegalEntities.html')
        end_time = datetime.now()
        logging.info('Search time into the legal entities register: ' + str(end_time-start_time))
        gc.collect()
        
    def __searchEntrepreneurs(self, queryString):
        start_time = datetime.now()
        entrepreneursCol = self.__db['Entrepreneurs']
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