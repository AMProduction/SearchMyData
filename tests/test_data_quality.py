import pytest
import requests
import json
import pymongo
from pathlib import Path


class TestDataQuality:

    @pytest.fixture
    def databaseConnect(self):
        configJsonFilePath = Path('config.json')
        # check if config.json exists
        if configJsonFilePath.is_file():
            configJsonFile = open(configJsonFilePath)
            # try to read json
            configJson = json.loads(configJsonFile.read())
            dbstring = configJson['dbstring']
            maxSevSelDelay = 3
            dbserver = pymongo.MongoClient(
                dbstring, serverSelectionTimeoutMS=maxSevSelDelay)
            dbserver.server_info()  # force connection on a request
            return dbserver['searchmydata']

    @pytest.mark.missingpersons
    def test_MissingPersons_documents_count(self, databaseConnect):
        # get expected documents count
        generalDataset = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=470196d3-4e7a-46b0-8c0c-883b74ac65f0').text
        generalDatasetJson = json.loads(generalDataset)
        # get dataset id
        missingPersonsGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']
        # get resources JSON id
        missingPersonsGeneralDatasetIdJson = requests.get(
            'https://data.gov.ua/api/3/action/resource_show?id=' + missingPersonsGeneralDatasetId).text
        missingPersonsGeneralDatasetJson = json.loads(
            missingPersonsGeneralDatasetIdJson)
        # get dataset json url
        missingPersonsDatasetJsonUrl = missingPersonsGeneralDatasetJson['result']['url']
        # get dataset
        missingPersonsDatasetJson = requests.get(missingPersonsDatasetJsonUrl)
        # get expected documents count
        expected_documents_count = len(missingPersonsDatasetJson.json())
        # get actual documents count
        db = databaseConnect
        missingPersonsCol = db['MissingPersons']
        actual_documents_count = missingPersonsCol.count_documents({})
        assert expected_documents_count == actual_documents_count

    @pytest.mark.wantedpersons
    def test_WantedPersons_documents_count(self, databaseConnect):
        # get expected documents count
        generalDataset = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=7c51c4a0-104b-4540-a166-e9fc58485c1b').text
        generalDatasetJson = json.loads(generalDataset)
        # get dataset id
        wantedPersonsGeneralDatasetId = generalDatasetJson['result']['resources'][0]['id']
        # get resources JSON id
        wantedPersonsGeneralDatasetIdJson = requests.get(
            'https://data.gov.ua/api/3/action/resource_show?id=' + wantedPersonsGeneralDatasetId).text
        wantedPersonsGeneralDatasetJson = json.loads(
            wantedPersonsGeneralDatasetIdJson)
        # get dataset json url
        wantedPersonsDatasetJsonUrl = wantedPersonsGeneralDatasetJson['result']['url']
        # get dataset
        wantedPersonsDatasetJson = requests.get(wantedPersonsDatasetJsonUrl)
        # get expected documents count
        expected_documents_count = len(wantedPersonsDatasetJson.json())
        # get actual documents count
        db = databaseConnect
        wantedPersonsCol = db['WantedPersons']
        actual_documents_count = wantedPersonsCol.count_documents({})
        assert expected_documents_count == actual_documents_count
