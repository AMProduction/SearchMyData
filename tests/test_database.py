import json
from pathlib import Path

import pymongo
import pytest


class TestDatabase:

    @pytest.fixture
    def database_connect(self):
        config_json_file_path = Path('../config.json')
        # check if config.json exists
        if config_json_file_path.is_file():
            config_json_file = open(config_json_file_path)
            # try to read json
            config_json = json.loads(config_json_file.read())
            dbstring = config_json['dbstring']
            maxSevSelDelay = 3
            dbserver = pymongo.MongoClient(dbstring, serverSelectionTimeoutMS=maxSevSelDelay)
            dbserver.server_info()  # force connection on a request
            return dbserver['searchmydata']
        else:
            return None

    def test_service_collection(self, database_connect):
        db = database_connect
        service_col = db['ServiceCollection']
        documents_count = service_col.count_documents({})
        assert documents_count > 0

    @pytest.mark.missingpersons
    def test_missing_persons_collection(self, database_connect):
        db = database_connect
        missing_persons_col = db['MissingPersons']
        documents_count = missing_persons_col.count_documents({})
        assert documents_count > 0

    @pytest.mark.wantedpersons
    def test_wanted_persons_collection(self, database_connect):
        db = database_connect
        wanted_persons_col = db['WantedPersons']
        documents_count = wanted_persons_col.count_documents({})
        assert documents_count > 0

    @pytest.mark.debtors
    def test_debtors_collection(self, database_connect):
        db = database_connect
        debtors_col = db['Debtors']
        documents_count = debtors_col.count_documents({})
        assert documents_count > 0

    def test_entrepreneurs_collection(self, database_connect):
        db = database_connect
        entrepreneurs_col = db['Entrepreneurs']
        documents_count = entrepreneurs_col.count_documents({})
        assert documents_count > 0

    @pytest.mark.legalentities
    def test_legal_entities_collection(self, database_connect):
        db = database_connect
        legal_entities_col = db['LegalEntities']
        documents_count = legal_entities_col.count_documents({})
        assert documents_count > 0

    @pytest.mark.lustrated
    def test_lustrated_collection(self, database_connect):
        db = database_connect
        legal_entities_col = db['Lustrated']
        documents_count = legal_entities_col.count_documents({})
        assert documents_count > 0
