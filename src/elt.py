import requests
import json
import logging

class GetDatasets:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    #Інформація про безвісно зниклих громадян (JSON)    
    def getMissingPersonsRegister(self):
        try:
            generalDataset = requests.get('https://data.gov.ua/api/3/action/package_show?id=470196d3-4e7a-46b0-8c0c-883b74ac65f0').text
        except:
            logging.error('Error during general MissingPersons dataset JSON receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            generalDatasetJson = json.loads(generalDataset) 
            logging.info('A general MissingPersons dataset JSON received')       
        #get dataset id
        missingPersonsGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']       
        try:
            #get resources JSON id
            missingPersonsGeneralDatasetIdJson = requests.get('https://data.gov.ua/api/3/action/resource_show?id=' + missingPersonsGeneralDatasetId).text
        except:
            logging.error('Error during MissingPersons resources JSON id receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            missingPersonsGeneralDatasetJson = json.loads(missingPersonsGeneralDatasetIdJson)
            logging.info('A MissingPersons resources JSON id received')
        #get dataset json url
        missingPersonsDatasetJsonUrl = missingPersonsGeneralDatasetJson['result']['url']        
        try:
            #get dataset
            missingPersonsDatasetJson = requests.get(missingPersonsDatasetJsonUrl).text
        except:
            logging.error('Error during MissingPersons dataset receiving occured')
            print('Error during dataset receiving occured!')
        else:
            missingPersonsDataset = json.loads(missingPersonsDatasetJson)
            logging.info('A MissingPersons dataset received')          
        print('The Register "' + generalDatasetJson['result']['title'] + '" refreshed')
        return missingPersonsDataset
    
    #Інформація про осіб, які переховуються від органів влади (JSON)
    def getWantedPersonsRegister(self):
        try:
            generalDataset = requests.get('https://data.gov.ua/api/3/action/package_show?id=7c51c4a0-104b-4540-a166-e9fc58485c1b').text
        except:
            logging.error('Error during general WantedPersons dataset JSON receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            generalDatasetJson = json.loads(generalDataset)
            logging.info('A general WantedPersons dataset JSON received')        
        #get dataset id
        wantedPersonsGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']
        try:
            #get resources JSON id
            wantedPersonsGeneralDatasetIdJson = requests.get('https://data.gov.ua/api/3/action/resource_show?id=' + wantedPersonsGeneralDatasetId).text
        except:
            logging.error('Error during WantedPersons resources JSON id receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            wantedPersonsGeneralDatasetJson = json.loads(wantedPersonsGeneralDatasetIdJson)
            logging.info('A WantedPersons resources JSON id received')
        #get dataset json url
        wantedPersonsDatasetJsonUrl = wantedPersonsGeneralDatasetJson['result']['url']
        try:
            #get dataset
            wantedPersonsDatasetJson = requests.get(wantedPersonsDatasetJsonUrl).text
        except:
            logging.error('Error during WantedPersons dataset receiving occured')
            print('Error during dataset receiving occured!')
        else:
            wantedPersonsDataset = json.loads(wantedPersonsDatasetJson)
            logging.info('A WantedPersons dataset received')          
        print('The Register "' + generalDatasetJson['result']['title'] + '" refreshed')
        return wantedPersonsDataset
    
    #Єдиний державний реєстр юридичних осіб, фізичних осіб-підприємців та громадських формувань (XMLs in ZIPped)
    def getEntrepreneursRegister(self):
        try:
            generalDataset = requests.get('https://data.gov.ua/api/3/action/package_show?id=1c7f3815-3259-45e0-bdf1-64dca07ddc10').text
        except:
            logging.error('Error during general EntrepreneursRegister dataset JSON receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            generalDatasetJson = json.loads(generalDataset)
            logging.info('A general EntrepreneursRegister dataset JSON received')
        #get dataset id
        entrepreneursGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']
        try:
            #get resources JSON id
            entrepreneursGeneralDatasetIdJson = requests.get('https://data.gov.ua/api/3/action/resource_show?id=' + entrepreneursGeneralDatasetId).text
        except:
            logging.error('Error during EntrepreneursRegister resources JSON id receiving occured')
            print('Error during dataset receiving occurred!')
        else:
            entrepreneursGeneralDatasetJson = json.loads(entrepreneursGeneralDatasetIdJson)
            logging.info('A EntrepreneursRegister resources JSON id received')
        #get ZIP url
        entrepreneursDatasetZIPUrl = entrepreneursGeneralDatasetJson['result']['url']
        return entrepreneursDatasetZIPUrl