import gc
import logging
from datetime import datetime

from prettytable import PrettyTable
from pymongo.errors import PyMongoError

from src.dataset import Dataset


class EntrepreneursRegister(Dataset):
    def __init__(self):
        super().__init__()

    @Dataset.measure_execution_time
    def get_dataset(self):
        logging.info('EntrepreneursRegister getDataset call')

    @Dataset.measure_execution_time
    def save_dataset(self):
        logging.info('EntrepreneursRegister saveDataset call')

    @Dataset.measure_execution_time
    def clear_collection(self):
        if self.is_collection_exists('Entrepreneurs'):
            entrepreneurs_col = self.db['Entrepreneurs']
            count_deleted_documents = entrepreneurs_col.delete_many({})
            logging.warning('%s documents deleted. The entrepreneurs collection is empty.', str(
                count_deleted_documents.deleted_count))

    @Dataset.measure_execution_time
    def __create_service_json(self):
        created_date = datetime.now()
        last_modified_date = datetime.now()
        entrepreneurs_col = self.db['Entrepreneurs']
        documents_count = entrepreneurs_col.count_documents({})
        entrepreneurs_register_service_json = {
            '_id': 5,
            'Description': 'Єдиний державний реєстр фізичних осіб – підприємців',
            'DocumentsCount': documents_count,
            'CreatedDate': str(created_date),
            'LastModifiedDate': str(last_modified_date)
        }
        self.serviceCol.insert_one(entrepreneurs_register_service_json)

    @Dataset.measure_execution_time
    def __update_service_json(self):
        last_modified_date = datetime.now()
        entrepreneurs_col = self.db['Entrepreneurs']
        documents_count = entrepreneurs_col.count_documents({})
        self.serviceCol.update_one(
            {'_id': 5},
            {'$set': {'LastModifiedDate': str(last_modified_date),
                      'DocumentsCount': documents_count}}
        )

    @Dataset.measure_execution_time
    def update_metadata(self):
        # update or create EntrepreneursRegisterServiceJson
        if (self.is_collection_exists('ServiceCollection')) and (
                self.serviceCol.count_documents({'_id': 5}, limit=1) != 0):
            self.__update_service_json()
            logging.info('EntrepreneursRegisterServiceJson updated')
        else:
            self.__create_service_json()
            logging.info('EntrepreneursRegisterServiceJson created')

    @Dataset.measure_execution_time
    def delete_collection_index(self):
        if self.is_collection_exists('Entrepreneurs'):
            entrepreneurs_col = self.db['Entrepreneurs']
            if 'full_text' in entrepreneurs_col.index_information():
                entrepreneurs_col.drop_index('full_text')
                logging.warning('Entrepreneurs Text index deleted')

    @Dataset.measure_execution_time
    def create_collection_index(self):
        entrepreneurs_col = self.db['Entrepreneurs']
        entrepreneurs_col.create_index([('fio', 'text')], name='full_text')
        logging.info('Entrepreneurs Text Index created')

    @Dataset.measure_execution_time
    def search_into_collection(self, query_string):
        entrepreneurs_col = self.db['Entrepreneurs']
        try:
            result_count = entrepreneurs_col.count_documents(
                {'$text': {'$search': query_string}})
        except PyMongoError:
            logging.error('Error during search into Entrepreneurs Register')
            print('Error during search into Entrepreneurs Register')
        else:
            if result_count == 0:
                print('The Entrepreneurs register: No data found')
                logging.warning('The Entrepreneurs register: No data found')
            else:
                result_table = PrettyTable(['NAME', 'ADDRESS', 'KVED', 'STATE'])
                result_table.align = 'l'
                result_table._max_width = {
                    'NAME': 40, 'ADDRESS': 40, 'KVED': 35}
                # show only 10 first search results
                for result in entrepreneurs_col.find({'$text': {'$search': query_string}},
                                                     {'score': {'$meta': 'textScore'}}) \
                        .sort([('score', {'$meta': 'textScore'})]).limit(10).allow_disk_use(True):
                    result_table.add_row([result['fio'], result['address'], result['kved'], result['stan']])
                print(result_table.get_string(
                    title=f'The Entrepreneurs register: {result_count} records found'))
                logging.warning(
                    'The Entrepreneurs register: %s records found', str(result_count))
                print('Only 10 first search results showed')
                # save all search results into HTML
                for result in entrepreneurs_col.find({'$text': {'$search': query_string}},
                                                     {'score': {'$meta': 'textScore'}}) \
                        .sort([('score', {'$meta': 'textScore'})]).allow_disk_use(True):
                    result_table.add_row([result['fio'], result['address'], result['kved'], result['stan']])
                html_result = result_table.get_html_string()
                f = open('results/Entrepreneurs.html', 'w', encoding='utf-8')
                f.write(html_result)
                f.close()
                print('All result dataset was saved into Entrepreneurs.html')
                logging.warning(
                    'All result dataset was saved into Entrepreneurs.html')
        gc.collect()
