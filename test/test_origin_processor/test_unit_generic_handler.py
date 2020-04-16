import unittest
import pytest
import json
from datetime import datetime, timezone

from src.ledger_dto import GGO, TransferGGORequest, GGONext, GGOAction

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from src.origin_handlers import PublishMeasurementTransactionHandler
 
from .mocks import MockContext, FakeTransaction, FakeTransactionHeader

from marshmallow_dataclass import class_schema


class TestGenericFunctions(unittest.TestCase):

    @pytest.mark.unittest
    def test_get_type(self):
        handler = PublishMeasurementTransactionHandler()

        context = MockContext({
            'add_1': json.dumps({'value': 'not a ggo'}).encode('utf8')
        })

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            handler._get_type(GGO, context, 'add_1')

        self.assertEqual(str(invalid_transaction.exception), 'Address "add_1" does not contain a valid GGO.')
