import logging
import src.ServiceTools
from src.DebtorsRegister import DebtorsRegister
from src.EntrepreneursRegister import EntrepreneursRegister
from src.LegalEntitiesRegister import LegalEntitiesRegister
from src.MissingPersonsRegister import MissingPersonsRegister
from src.WantedPersonsRegister import WantedPersonsRegister


def search():
    searchString = str(input('Search query: '))
    service.clearResultsDir()
    # call search method
    missingPersons.searchIntoCollection(searchString)
    wantedPersons.searchIntoCollection(searchString)
    debtors.searchIntoCollection(searchString)
    legalEntities.searchIntoCollection(searchString)
    entrepreneurs.searchIntoCollection(searchString)


def setupDatasets():
    # Інформація про безвісно зниклих громадян (JSON)
    missingPersons.setupDataset()

    # Інформація про осіб, які переховуються від органів влади (JSON)
    wantedPersons.setupDataset()

    # Єдиний реєстр боржників (CSV in ZIP)
    debtors.setupDataset()

    # Єдиний державний реєстр юридичних осіб, фізичних осіб-підприємців та громадських формувань (XMLs in ZIPped)
    legalEntities.deleteCollectionIndex()
    legalEntities.clearCollection()

    entrepreneurs.deleteCollectionIndex()
    entrepreneurs.clearCollection()

    entrepreneursDatasetZIPUrl = legalEntities.getDataset()
    legalEntities.saveDataset(entrepreneursDatasetZIPUrl)

    legalEntities.updateMetadata()
    entrepreneurs.updateMetadata()

    legalEntities.createCollectionIndex()
    entrepreneurs.createCollectionIndex()


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
    logging.basicConfig(filename='logs/searchmydata.log', filemode='a', format='%(asctime)s %(levelname)8s:%(filename)16s:%(message)s',
                        datefmt='%d/%m/%Y %H:%M:%S', encoding='utf-8', level=logging.DEBUG)
    logging.info('The application started')
    # create instances
    service = src.ServiceTools.ServiceTools()
    missingPersons = MissingPersonsRegister()
    wantedPersons = WantedPersonsRegister()
    debtors = DebtorsRegister()
    legalEntities = LegalEntitiesRegister()
    entrepreneurs = EntrepreneursRegister()
    service.clearConsole()
    # main loop
    while(True):
        service.getRegistersInfo()
        print_menu()
        option = ''
        try:
            option = int(input('Enter your choice: '))
        except:
            logging.error('The wrong input type of menu item choice')
            print('Wrong input. Please enter a number ...')
        # Check what choice was entered and act accordingly
        if option == 1:
            logging.warning('The "Search" menu item chosen')
            search()
        elif option == 2:
            logging.warning('The "Refresh datasets" menu item chosen')
            setupDatasets()
        elif option == 3:
            logging.warning('The "Exit" menu item chosen')
            logging.info('The application closed')
            print('Quitting...')
            exit()
        else:
            logging.error('The wrong menu item choice')
            print('Invalid option. Please enter a number between 1 and 3.')