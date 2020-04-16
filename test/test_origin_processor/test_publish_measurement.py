

import unittest
import pytest
import json

from datetime import datetime, timezone
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from src.origin_handlers import PublishMeasurementTransactionHandler
from src.ledger_dto import MeasurementType, Measurement
 
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
                payload_sha512="d70bfa9020d4f03a7ca4e706b81d3d8b3cf93fe9942b83f1e1661517d8da8991708a87ca7a50fd536fdd218e7ebe5385454286693897cd96686dca6f5649256e",
                signer_public_key="039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8"),
            header_signature="7651c96e081880de546683b7f47ca9124bd398bb7ad5880813a7cb882d2901e405e386730d8ca04aabdfa354b6b66105b1b7e51141d25bf34a0a245004209e45",
            payload=payload
        )


    @pytest.mark.unittest
    def test_identifiers(self):
        handler = PublishMeasurementTransactionHandler()
        
        self.assertEqual(handler.family_name, 'PublishMeasurementRequest')

        self.assertEqual(len(handler.family_versions), 1)
        self.assertIn('0.1', handler.family_versions)

        self.assertEqual(len(handler.namespaces), 1)
        self.assertIn('146fca', handler.namespaces)


    @pytest.mark.unittest
    def test_internal_error(self):
        with self.assertRaises(InternalError) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(None, None)

        self.assertEqual(str(invalid_transaction.exception), 'An unknown error has occured.')
        

    @pytest.mark.unittest
    def test_publish_measurement(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "key": "03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e", "end": "2020-01-01T13:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        PublishMeasurementTransactionHandler().apply(transaction, context)

        # Assert that the measurement has been added to the context states with the correct values.
        self.assertIn(address, context.states)

        obj = json.loads(context.states[address].decode('utf8'))
        self.assertEqual(len(obj), 6)

        measurement: Measurement = Measurement.get_schema().loads(context.states[address].decode('utf8'))
        self.assertEqual(measurement.amount, 5123)
        self.assertEqual(measurement.begin, datetime(2020,1,1,12, tzinfo=timezone.utc))
        self.assertEqual(measurement.end, datetime(2020,1,1,13, tzinfo=timezone.utc))
        self.assertEqual(measurement.type, MeasurementType.CONSUMPTION)
        self.assertEqual(measurement.sector, 'DK1')
        self.assertEqual(measurement.key, '03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e')


    @pytest.mark.unittest
    def test_publish_measurement_negative_amount(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": -5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "key": "03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e", "end": "2020-01-01T13:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)
        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'amount': ['Must be greater than or equal to 0.']}")


    @pytest.mark.unittest
    def test_publish_measurement_invalid_type(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "LEFT", "begin": "2020-01-01T12:00:00+00:00", "key": "03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e", "end": "2020-01-01T13:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'type': ['Invalid enum member LEFT']}")



    @pytest.mark.unittest
    def test_publish_measurement_invalid_end_before_begin(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T13:00:00+00:00", "key": "03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e", "end": "2020-01-01T12:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'_schema': ['Begin must be before End!']}")

        
    @pytest.mark.unittest
    def test_publish_measurement_invalid_not_hourly(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "key": "03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e", "end": "2020-01-01T14:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), "{'_schema': ['Only positive hourly measurements are currently supported!']}")


    @pytest.mark.unittest
    def test_publish_measurement_invalid_sector(self):
        
        address = '5a98391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={})

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "key": "03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e", "end": "2020-01-01T14:00:00+00:00", "sector": "NO1"}'
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

        payload = b'{"amount": 5123, "type": "CONSUMPTION", "begin": "2020-01-01T12:00:00+00:00", "key": "03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e", "end": "2020-01-01T13:00:00+00:00", "sector": "DK1"}'
        transaction = self.create_fake_transaction([address],[address],payload)


        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            PublishMeasurementTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), f'Address already in use "{address}"!')