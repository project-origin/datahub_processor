

import unittest
import pytest
import json

from datetime import datetime, timezone
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from src.datahub_processor.publish_measurement_handler import PublishMeasurementTransactionHandler, Measurement
from src.datahub_processor.ledger_dto import MeasurementType
 
from .mocks import MockContext, FakeTransaction, FakeTransactionHeader


class TestPublishMeasurement(unittest.TestCase):

    def create_fake_transaction(self, inputs, outputs, payload):
        
        return FakeTransaction(
            header=FakeTransactionHeader(
                batcher_public_key="039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8",
                dependencies=[],
                family_name="datahub",
                family_version="0.1",
                inputs=inputs,
                outputs=outputs,
                signer_public_key="039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8"),
            payload=payload
        )


    @pytest.mark.unittest
    def test_identifiers(self):
        handler = PublishMeasurementTransactionHandler()
        
        self.assertEqual(handler.family_name, 'PublishMeasurementRequest')

        self.assertEqual(len(handler.family_versions), 1)
        self.assertIn('0.1', handler.family_versions)

        self.assertEqual(len(handler.namespaces), 1)
        self.assertIn('5a9839', handler.namespaces)


    @pytest.mark.unittest
    def test_internal_error(self):
        with self.assertRaises(InternalError) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(None, None)

        self.assertEqual(str(invalid_transaction.exception), 'An unknown error has occured.')
        

    @pytest.mark.unittest
    def test_publish_measurement(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "end": "2020-01-01T13:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        PublishMeasurementTransactionHandler().apply(transaction, context)

        # Assert that the measurement has been added to the context states with the correct values.
        self.assertIn(address, context.states)

        obj = json.loads(context.states[address].decode('utf8'))
        self.assertEqual(len(obj), 5)

        measurement: Measurement = Measurement.get_schema().loads(context.states[address].decode('utf8'))
        self.assertEqual(measurement.amount, 5123)
        self.assertEqual(measurement.begin, datetime(2020,1,1,12, tzinfo=timezone.utc))
        self.assertEqual(measurement.end, datetime(2020,1,1,13, tzinfo=timezone.utc))
        self.assertEqual(measurement.type, MeasurementType.CONSUMPTION)
        self.assertEqual(measurement.sector, 'DK1')


    @pytest.mark.unittest
    def test_publish_measurement_negative_amount(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": -5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "end": "2020-01-01T13:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)
        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'amount': ['Must be greater than or equal to 0.']}")


    @pytest.mark.unittest
    def test_publish_measurement_invalid_type(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "LEFT", "begin": "2020-01-01T12:00:00+00:00", "end": "2020-01-01T13:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'type': ['Invalid enum member LEFT']}")



    @pytest.mark.unittest
    def test_publish_measurement_invalid_end_before_begin(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T13:00:00+00:00", "end": "2020-01-01T12:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'_schema': ['Begin must be before End!']}")

        
    @pytest.mark.unittest
    def test_publish_measurement_invalid_not_hourly(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "end": "2020-01-01T14:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'_schema': ['Only positive hourly measurements are currently supported!']}")


    @pytest.mark.unittest
    def test_publish_measurement_invalid_sector(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "end": "2020-01-01T14:00:00+00:00", "sector": "NO1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'sector': ['Must be one of: DK1, DK2.']}")


    @pytest.mark.unittest
    def test_publish_measurement_gibberish(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'gibberish'
        transaction = self.create_fake_transaction([address],[address],payload)

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'The transaction payload was an invalid request. Invalid JSON.')



    @pytest.mark.unittest
    def test_address_in_use(self):

        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        
        context = MockContext(states={address: b"SomeData"})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "end": "2020-01-01T13:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)


        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), f'Address already in use "{address}"!')