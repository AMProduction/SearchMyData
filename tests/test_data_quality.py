import json
from pathlib import Path

import pymongo
import pytest
import requests


class TestDataQuality:

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
            dbserver = pymongo.MongoClient(
                dbstring, serverSelectionTimeoutMS=maxSevSelDelay)
            dbserver.server_info()  # force connection on a request
            return dbserver['searchmydata']
        else:
            return None

    @pytest.mark.missingpersons
    def test_missing_persons_documents_count(self, database_connect):
        # get expected documents count
        general_dataset = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=470196d3-4e7a-46b0-8c0c-883b74ac65f0').text
        general_dataset_json = json.loads(general_dataset)
        # get dataset id
        missing_persons_general_dataset_id = general_dataset_json['result']['resources'][0]['id']
        # get resources JSON id
        missing_persons_general_dataset_id_json = requests.get(
            'https://data.gov.ua/api/3/action/resource_show?id=' + missing_persons_general_dataset_id).text
        missing_persons_general_dataset_json = json.loads(
            missing_persons_general_dataset_id_json)
        # get dataset json url
        missing_persons_dataset_json_url = missing_persons_general_dataset_json['result']['url']
        # get dataset
        missing_persons_dataset_json = requests.get(missing_persons_dataset_json_url)
        # get expected documents count
        expected_documents_count = len(missing_persons_dataset_json.json())
        # get actual documents count
        db = database_connect
        missing_persons_col = db['MissingPersons']
        actual_documents_count = missing_persons_col.count_documents({})
        assert expected_documents_count == actual_documents_count

    @pytest.mark.wantedpersons
    def test_wanted_persons_documents_count(self, database_connect):
        # get expected documents count
        general_dataset = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=7c51c4a0-104b-4540-a166-e9fc58485c1b').text
        general_dataset_json = json.loads(general_dataset)
        # get dataset id
        wanted_persons_general_dataset_id = general_dataset_json['result']['resources'][0]['id']
        # get resources JSON id
        wanted_persons_general_dataset_id_json = requests.get(
            'https://data.gov.ua/api/3/action/resource_show?id=' + wanted_persons_general_dataset_id).text
        wanted_persons_general_dataset_json = json.loads(
            wanted_persons_general_dataset_id_json)
        # get dataset json url
        wanted_persons_dataset_json_url = wanted_persons_general_dataset_json['result']['url']
        # get dataset
        wanted_persons_dataset_json = requests.get(wanted_persons_dataset_json_url)
        # get expected documents count
        expected_documents_count = len(wanted_persons_dataset_json.json())
        # get actual documents count
        db = database_connect
        wanted_persons_col = db['WantedPersons']
        actual_documents_count = wanted_persons_col.count_documents({})
        assert expected_documents_count == actual_documents_count
