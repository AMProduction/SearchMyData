import logging

import src.ServiceTools
from src.DebtorsRegister import DebtorsRegister
from src.EntrepreneursRegister import EntrepreneursRegister
from src.LegalEntitiesRegister import LegalEntitiesRegister
from src.MissingPersonsRegister import MissingPersonsRegister
from src.WantedPersonsRegister import WantedPersonsRegister
from src.LustratedPersonsRegister import  LustratedPersonsRegister


def search():
    search_string = str(input('Search query: '))
    logging.info('The search string: ' + search_string)
    service.clear_results_dir()
    # call search methods
    missingPersons.search_into_collection(search_string)
    wantedPersons.search_into_collection(search_string)
    debtors.search_into_collection(search_string)
    legalEntities.search_into_collection(search_string)
    entrepreneurs.search_into_collection(search_string)


def setup_datasets():
    lustrated.get_dataset()
'''
    # Інформація про безвісно зниклих громадян (JSON)
    missingPersons.setup_dataset()

    # Інформація про осіб, які переховуються від органів влади (JSON)
    wantedPersons.setup_dataset()

    # Єдиний реєстр боржників (CSV in ZIP)
    debtors.setup_dataset()

    # Єдиний державний реєстр юридичних осіб, фізичних осіб-підприємців та громадських формувань (XMLs in ZIPped)
    legalEntities.delete_collection_index()
    legalEntities.clear_collection()

    entrepreneurs.delete_collection_index()
    entrepreneurs.clear_collection()

    entrepreneurs_dataset_zip_url = legalEntities.get_dataset()
    legalEntities.save_dataset(entrepreneurs_dataset_zip_url)

    legalEntities.update_metadata()
    entrepreneurs.update_metadata()

    legalEntities.create_collection_index()
    entrepreneurs.create_collection_index()
'''

menu_options = {
    1: 'Search',
    2: 'Refresh datasets',
    3: 'Exit'
}


def print_menu():
    for key in menu_options.keys():
        print(key, '--', menu_options[key])


if __name__ == '__main__':
    # Set up logging
    logging.basicConfig(filename='logs/searchmydata.log', filemode='a',
                        format='%(asctime)s %(levelname)10s:%(filename)26s:%(message)s', datefmt='%d/%m/%Y %H:%M:%S',
                        encoding='utf-8', level=logging.DEBUG)
    logging.info('The application started')
    # create instances
    service = src.ServiceTools.ServiceTools()
    missingPersons = MissingPersonsRegister()
    wantedPersons = WantedPersonsRegister()
    debtors = DebtorsRegister()
    legalEntities = LegalEntitiesRegister()
    entrepreneurs = EntrepreneursRegister()
    lustrated = LustratedPersonsRegister()
    service.clear_console()
    # main loop
    while True:
        service.get_registers_info()
        service.check_is_expired()
        print_menu()
        option = ''
        try:
            option = int(input('Enter your choice: '))
        except ValueError:
            logging.error('The wrong input type of menu item choice')
            print('Wrong input. Please enter a number ...')
        # Check what choice was entered and act accordingly
        if option == 1:
            logging.warning('The "Search" menu item chosen')
            search()
        elif option == 2:
            logging.warning('The "Refresh datasets" menu item chosen')
            setup_datasets()
        elif option == 3:
            logging.warning('The "Exit" menu item chosen')
            logging.info('The application closed')
            print('Quitting...')
            exit()
        else:
            logging.error('The wrong menu item choice')
            print('Invalid option. Please enter a number between 1 and 3.')
