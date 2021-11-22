import pytest
import requests


class TestDataApi:

    @pytest.mark.missingpersons
    def test_get_missing_persons_json_t1(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=470196d3-4e7a-46b0-8c0c-883b74ac65f0')
        assert response.status_code == 200

    @pytest.mark.missingpersons
    def test_get_missing_persons_json_t2(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=470196d3-4e7a-46b0-8c0c-883b74ac65f0')
        response_body = response.json()
        assert response_body['result']['title'] == 'Інформація про безвісно зниклих громадян'

    @pytest.mark.wantedpersons
    def test_get_wanted_persons_json_t1(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=7c51c4a0-104b-4540-a166-e9fc58485c1b')
        assert response.status_code == 200

    @pytest.mark.wantedpersons
    def test_get_wanted_persons_json_t2(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=7c51c4a0-104b-4540-a166-e9fc58485c1b')
        response_body = response.json()
        assert response_body['result']['title'] == 'Інформація про осіб, які переховуються від органів влади'

    @pytest.mark.debtors
    def test_get_debtors_json_t1(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=506734bf-2480-448c-a2b4-90b6d06df11e')
        assert response.status_code == 200

    @pytest.mark.debtors
    def test_get_debtors_json_t2(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=506734bf-2480-448c-a2b4-90b6d06df11e')
        response_body = response.json()
        assert response_body['result']['title'] == 'Єдиний реєстр боржників'

    @pytest.mark.legalentities
    def test_get_legal_entities_json_t1(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=1c7f3815-3259-45e0-bdf1-64dca07ddc10')
        assert response.status_code == 200

    @pytest.mark.legalentities
    def test_get_legal_entities_json_t2(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=1c7f3815-3259-45e0-bdf1-64dca07ddc10')
        response_body = response.json()
        assert response_body['result'][
                   'title'] == 'Єдиний державний реєстр юридичних осіб, фізичних осіб-підприємців та громадських формувань'

    @pytest.mark.lustrated
    def test_get_lustrated_json_t1(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=8faa71c1-3a54-45e8-8f6e-06c92b1ff8bc')
        assert response.status_code == 200

    @pytest.mark.lustrated
    def test_get_lustrated_json_t2(self):
        response = requests.get(
            'https://data.gov.ua/api/3/action/package_show?id=8faa71c1-3a54-45e8-8f6e-06c92b1ff8bc')
        response_body = response.json()
        assert response_body['result'][
                   'title'] == 'Єдиний державний реєстр осіб, щодо яких застосовано положення Закону України «Про очищення влади»'
