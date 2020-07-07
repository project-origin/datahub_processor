import unittest
import pytest
import json

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from src.datahub_processor.issue_ggo_transaction_handler import IssueGGOTransactionHandler
 
from .mocks import MockContext, FakeTransaction, FakeTransactionHeader



class TestIssueGGO(unittest.TestCase):

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
        handler = IssueGGOTransactionHandler()
        
        self.assertEqual(handler.family_name, 'IssueGGORequest')

        self.assertEqual(len(handler.family_versions), 1)
        self.assertIn('0.1', handler.family_versions)

        self.assertEqual(len(handler.namespaces), 1)
        self.assertIn('849c0b', handler.namespaces)

    @pytest.mark.unittest
    def test_internal_error(self):
        with self.assertRaises(InternalError) as invalid_transaction:
            IssueGGOTransactionHandler().apply(None, None)

        self.assertEqual(str(invalid_transaction.exception), 'An unknown error has occured.')
        


    @pytest.mark.unittest
    def test_issue_ggo_no_measurement(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={
        })

        payload = json.dumps({
            "origin":mea_add,
            "destination": ggo_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "emissions":{
                "co2": {
                    "value": 1113342.14,
                    "unit": "g/Wh",
                },
                "so2": {
                    "value": 9764446,
                    "unit": "g/Wh",
                },
            }
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            IssueGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Address "mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c" does not contain a valid Measurement.')

        
    @pytest.mark.unittest
    def test_issue_ggo_not_a_measurement(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={
            mea_add: b''
        })

        payload = json.dumps({
            "origin": mea_add,
            "destination": ggo_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "emissions":{
                "co2": {
                    "value": 1113342.14,
                    "unit": "g/Wh",
                },
                "so2": {
                    "value": 9764446,
                    "unit": "g/Wh",
                },
            }
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            IssueGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Address "mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c" does not contain a valid Measurement.')


    @pytest.mark.unittest
    def test_issue_ggo_not_production(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        mea = json.dumps({
                'amount': 123,
                'type': 'CONSUMPTION',
                'begin': "2020-01-01T12:00:00+00:00",
                'end': "2020-01-01T13:00:00+00:00",
                'sector': 'DK1'
            }).encode('utf8')

        context = MockContext(states={
            mea_add: mea
        })

        payload = json.dumps({
            "origin": mea_add,
            "destination": ggo_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "emissions":{
                "co2": {
                    "value": 1113342.14,
                    "unit": "g/Wh",
                },
                "so2": {
                    "value": 9764446,
                    "unit": "g/Wh",
                },
            }
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            IssueGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Measurement is not of type Production!')

          
    @pytest.mark.unittest
    def test_issue_ggo_success(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        mea = json.dumps({
                'amount': 123,
                'type': 'PRODUCTION',
                'begin': "2020-01-01T12:00:00+00:00",
                'end': "2020-01-01T13:00:00+00:00",
                'sector': 'DK1'
            }).encode('utf8')

        context = MockContext(states={
            mea_add: mea
        })

        payload = json.dumps({
            "origin": mea_add,
            "destination": ggo_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "emissions":{
                "co2": {
                    "value": 1113342.14,
                    "unit": "g/Wh",
                },
                "so2": {
                    "value": 9764446,
                    "unit": "g/Wh",
                },
            }
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        IssueGGOTransactionHandler().apply(transaction, context)
        self.assertIn(ggo_add, context.states)

        obj = json.loads(context.states[ggo_add].decode('utf8'))
        self.assertEqual(len(obj), 9)

        self.assertEqual(obj['origin'], mea_add)
        self.assertEqual(obj['amount'], 123)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['emissions'], {
            "co2": {
                "value": 1113342.14,
                "unit": "g/Wh",
            },
            "so2": {
                "value": 9764446,
                "unit": "g/Wh",
            },
        })
        self.assertEqual(obj['next'], None)


    @pytest.mark.unittest
    def test_issue_ggo_fail_reissue(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        mea = json.dumps({
                'amount': 123,
                'type': 'PRODUCTION',
                'begin': "2020-01-01T12:00:00+00:00",
                'end': "2020-01-01T13:00:00+00:00",
                'sector': 'DK1'
            }).encode('utf8')

        context = MockContext(states={
            mea_add: mea
        })

        payload = json.dumps({
            "origin": mea_add,
            "destination": ggo_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "emissions":{
                "co2": {
                    "value": 1113342.14,
                    "unit": "g/Wh",
                },
                "so2": {
                    "value": 9764446,
                    "unit": "g/Wh",
                },
            }
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        IssueGGOTransactionHandler().apply(transaction, context)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            IssueGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'GGO already issued!')

        
    