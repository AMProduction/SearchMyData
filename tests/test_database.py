import json
from pathlib import Path
import pytest
import pymongo


class TestDatabase:

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

    def test_service_collection(self, databaseConnect):
        db = databaseConnect
        serviceCol = db['ServiceCollection']
        documents_count = serviceCol.count_documents({})
        assert documents_count > 0

    @pytest.mark.missingpersons
    def test_MissingPersons_collection(self, databaseConnect):
        db = databaseConnect
        missingPersonsCol = db['MissingPersons']
        documents_count = missingPersonsCol.count_documents({})
        assert documents_count > 0

    @pytest.mark.wantedpersons
    def test_WantedPersons_collection(self, databaseConnect):
        db = databaseConnect
        wantedPersonsCol = db['WantedPersons']
        documents_count = wantedPersonsCol.count_documents({})
        assert documents_count > 0

    @pytest.mark.debtors
    def test_Debtors_collection(self, databaseConnect):
        db = databaseConnect
        debtorsCol = db['Debtors']
        documents_count = debtorsCol.count_documents({})
        assert documents_count > 0

    def test_Entrepreneurs_collection(self, databaseConnect):
        db = databaseConnect
        entrepreneursCol = db['Entrepreneurs']
        documents_count = entrepreneursCol.count_documents({})
        assert documents_count > 0

    @pytest.mark.legalentities
    def test_LegalEntities_collection(self, databaseConnect):
        db = databaseConnect
        legalEntitiesCol = db['LegalEntities']
        documents_count = legalEntitiesCol.count_documents({})
        assert documents_count > 0
