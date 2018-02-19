# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END


import json
from unittest.mock import MagicMock, patch
from aiohttp import web
import pytest
from foglamp.services.core import routes
from foglamp.services.core import connect
from foglamp.common.storage_client.storage_client import StorageClient


__author__ = "Ashish Jabble"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


@pytest.allure.feature("unit")
@pytest.allure.story("api", "audit")
class TestAudit:

    @pytest.fixture
    def client(self, loop, test_client):
        app = web.Application(loop=loop)
        # fill the routes table
        routes.setup(app)
        return loop.run_until_complete(test_client(app))

    @pytest.fixture()
    def get_log_codes(self):
        return {"rows": [{"code": "PURGE", "description": "Data Purging Process"},
                         {"code": "LOGGN", "description": "Logging Process"},
                         {"code": "STRMN", "description": "Streaming Process"},
                         {"code": "SYPRG", "description": "System Purge"},
                         {"code": "START", "description": "System Startup"},
                         {"code": "FSTOP", "description": "System Shutdown"},
                         {"code": "CONCH", "description": "Configuration Change"},
                         {"code": "CONAD", "description": "Configuration Addition"},
                         {"code": "SCHCH", "description": "Schedule Change"},
                         {"code": "SCHAD", "description": "Schedule Addition"},
                         {"code": "SRVRG", "description": "Service Registered"},
                         {"code": "SRVUN", "description": "Service Unregistered"},
                         {"code": "SRVFL", "description": "Service Fail"},
                         {"code": "NHCOM", "description": "North Process Complete"},
                         {"code": "NHDWN", "description": "North Destination Unavailable"},
                         {"code": "NHAVL", "description": "North Destination Available"},
                         {"code": "UPEXC", "description": "Update Complete"},
                         {"code": "BKEXC", "description": "Backup Complete"}
                         ]}

    async def test_get_severity(self, client):
        resp = await client.get('/foglamp/audit/severity')
        assert 200 == resp.status
        result = await resp.text()
        json_response = json.loads(result)
        log_severity = json_response['logSeverity']

        # verify the severity count
        assert 4 == len(log_severity)

        # verify the name and value of severity
        for i in range(len(log_severity)):
            if log_severity[i]['index'] == 1:
                assert 'FATAL' == log_severity[i]['name']
            elif log_severity[i]['index'] == 2:
                assert 'ERROR' == log_severity[i]['name']
            elif log_severity[i]['index'] == 3:
                assert 'WARNING' == log_severity[i]['name']
            elif log_severity[i]['index'] == 4:
                assert 'INFORMATION' == log_severity[i]['name']

    async def test_audit_log_codes(self, client, get_log_codes):
        storage_client_mock = MagicMock(StorageClient)
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(storage_client_mock, 'query_tbl', return_value=get_log_codes) as log_code_patch:
                resp = await client.get('/foglamp/audit/logcode')
                assert 200 == resp.status
                result = await resp.text()
                json_response = json.loads(result)
                codes = [key['code'] for key in json_response['logCode']]
                actual_code_list = [key['code'] for key in get_log_codes['rows']]

                # verify the default log_codes with their values which are defined in init.sql
                assert 18 == len(codes)
                assert all([a == b for a, b in zip(actual_code_list, codes)])
            log_code_patch.assert_called_once_with('log_codes')

    @pytest.mark.parametrize("request_params, payload", [
        ('', '{"where": {"column": "1", "condition": "=", "value": "1"}, "sort": {"column": "ts", "direction": "desc"}, "limit": 20}'),
        ('?source=PURGE', '{"where": {"column": "1", "condition": "=", "value": "1", "and": {"column": "code", "condition": "=", "value": "PURGE"}}, "sort": {"column": "ts", "direction": "desc"}, "limit": 20}'),
        ('?skip=1', '{"where": {"column": "1", "condition": "=", "value": "1"}, "sort": {"column": "ts", "direction": "desc"}, "limit": 20, "skip": 1}'),
        ('?severity=error', '{"where": {"column": "1", "condition": "=", "value": "1", "and": {"column": "level", "condition": "=", "value": 2}}, "sort": {"column": "ts", "direction": "desc"}, "limit": 20}'),
        ('?severity=ERROR&limit=1', '{"where": {"column": "1", "condition": "=", "value": "1", "and": {"column": "level", "condition": "=", "value": 2}}, "sort": {"column": "ts", "direction": "desc"}, "limit": 1}'),
        ('?severity=INFORMATION&limit=1&skip=1', '{"where": {"column": "1", "condition": "=", "value": "1", "and": {"column": "level", "condition": "=", "value": 4}}, "sort": {"column": "ts", "direction": "desc"}, "limit": 1, "skip": 1}'),
        ('?source=&severity=&limit=&skip=', '{"where": {"column": "1", "condition": "=", "value": "1"}, "sort": {"column": "ts", "direction": "desc"}, "limit": 20}')
    ])
    async def test_get_audit_with_params(self, client, request_params, payload, get_log_codes):
        storage_client_mock = MagicMock(StorageClient)
        response = {"rows": [{"log": {"end_time": "2018-01-30 18:39:48.1517317788", "rowsRemaining": 0,
                                      "start_time": "2018-01-30 18:39:48.1517317788", "rowsRemoved": 0,
                                      "unsentRowsRemoved": 0, "rowsRetained": 0},
                              "code": "PURGE", "level": "4", "id": 2,
                              "ts": "2018-01-30 18:39:48.796263+05:30", 'count': 1}]}
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(storage_client_mock, 'query_tbl', return_value=get_log_codes):
                with patch.object(storage_client_mock, 'query_tbl_with_payload', return_value=response) as log_code_patch:
                    resp = await client.get('/foglamp/audit{}'.format(request_params))
                    assert 200 == resp.status
                    result = await resp.text()
                    json_response = json.loads(result)
                    assert 1 == json_response['totalCount']
                    assert 1 == len(json_response['audit'])
                log_code_patch.assert_called_with('log', payload)

    @pytest.mark.parametrize("request_params, response_code, response_message", [
        ('?source=BLA', 400, "BLA is not a valid source"),
        ('?source=1234', 400, "1234 is not a valid source"),
        ('?limit=invalid', 400, "Limit must be a positive integer"),
        ('?limit=-1', 400, "Limit must be a positive integer"),
        ('?skip=invalid', 400, "Skip/Offset must be a positive integer"),
        ('?skip=-1', 400, "Skip/Offset must be a positive integer"),
        ('?severity=BLA', 400, "'BLA' is not a valid severity")
    ])
    async def test_source_param_with_bad_data(self, client, request_params, response_code, response_message, get_log_codes):
        storage_client_mock = MagicMock(StorageClient)
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(storage_client_mock, 'query_tbl', return_value=get_log_codes):
                resp = await client.get('/foglamp/audit{}'.format(request_params))
                assert response_code == resp.status
                assert response_message == resp.reason

    async def test_get_audit_http_exception(self, client):
        resp = await client.get('/foglamp/audit')
        assert 500 == resp.status
        assert 'Internal Server Error' == resp.reason