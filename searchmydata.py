import src.dbtools
import src.elt
import src.service_tools
import src.search_engine
import os
import logging

def search():
    searchString = str(input('Search query: '))
    searchEngine = src.search_engine.SearchEngine()        
    searchEngine.search(searchString)
    
def getDataSets():
    eltinstance = src.elt.GetDatasets()
    dbTools = src.dbtools.DBTools()
    json = eltinstance.getMissingPersonsRegister()
    dbTools.saveMissingPersonsRegister(json)
    json = eltinstance.getWantedPersonsRegister()
    dbTools.saveWantedPersonsRegister(json)
    #zipUrlDebtors = eltinstance.getDebtorsRegister()
    #dbTools.saveDebtorsRegister(zipUrlDebtors)
    zipUrlEntrepreneursRegister = eltinstance.getEntrepreneursRegister()
    dbTools.saveEntrepreneursRegister(zipUrlEntrepreneursRegister)
    service.refreshMetadata()
    
def clearConsole():
    command = 'clear'
    if os.name in ('nt', 'dos'):  # If Machine is running on Windows, use cls
        command = 'cls'
    os.system(command)
    
def main():
    service.getRegistersInfo()

menu_options = {
    1: 'Search',
    2: 'Refresh datasets',
    3: 'Exit'
}

def print_menu():
    for key in menu_options.keys():
        print (key, '--', menu_options[key])
        
if __name__=='__main__':
    #Set up logging
    logging.basicConfig(filename='logs/searchmydata.log', filemode='a', format='%(asctime)s %(levelname)8s:%(filename)16s:%(message)s', datefmt='%d/%m/%Y %H:%M:%S', encoding='utf-8', level=logging.DEBUG)
    logging.info('The application started')    
    service = src.service_tools.ServiceTools()
    clearConsole()
    #main loop
    while(True):
        main()
        print_menu()
        option = ''
        try:
            option = int(input('Enter your choice: '))
        except:
            logging.error('The wrong input type of menu item choice')
            print('Wrong input. Please enter a number ...')
        #Check what choice was entered and act accordingly
        if option == 1:
            logging.warning('The "Search" menu item chosen')
            search()
        elif option == 2:
            logging.warning('The "Refresh datasets" menu item chosen')
            getDataSets()
        elif option == 3:
            logging.warning('The "Exit" menu item chosen')
            logging.info('The application closed')
            print('Quitting...')
            exit()
        else:
            logging.error('The wrong menu item choice')
            print('Invalid option. Please enter a number between 1 and 3.')