import unittest
import json

from sawtooth_sdk.processor.exceptions import InvalidTransaction
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
                payload_sha512="d70bfa9020d4f03a7ca4e706b81d3d8b3cf93fe9942b83f1e1661517d8da8991708a87ca7a50fd536fdd218e7ebe5385454286693897cd96686dca6f5649256e",
                signer_public_key="039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8"),
            header_signature="7651c96e081880de546683b7f47ca9124bd398bb7ad5880813a7cb882d2901e405e386730d8ca04aabdfa354b6b66105b1b7e51141d25bf34a0a245004209e45",
            payload=payload
        )


    def test_issue_ggo_no_measurement(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={
        })

        payload = json.dumps({
            "origin":mea_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "key":"aregaerg"
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            IssueGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Address "mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c" does not contain a valid measurement.')

        
    def test_issue_ggo_not_a_measurement(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={
            mea_add: b''
        })

        payload = json.dumps({
            "origin": mea_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "key":"aregaerg"
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            IssueGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Address "mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c" does not contain a valid measurement.')


    def test_issue_ggo_not_production(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        mea = json.dumps({
                'amount': 123,
                'type': 'CONSUMPTION',
                'begin': "2020-01-01T12:00:00+00:00",
                'end': "2020-01-01T13:00:00+00:00",
                'sector': 'DK1',
                'key': '03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e'
            }).encode('utf8')

        context = MockContext(states={
            mea_add: mea
        })

        payload = json.dumps({
            "origin": mea_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "key":"03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e"
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            IssueGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Measurement is not of type Production!')

          
    def test_issue_ggo_success(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        mea = json.dumps({
                'amount': 123,
                'type': 'PRODUCTION',
                'begin': "2020-01-01T12:00:00+00:00",
                'end': "2020-01-01T13:00:00+00:00",
                'sector': 'DK1',
                'key': '03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e'
            }).encode('utf8')

        context = MockContext(states={
            mea_add: mea
        })

        payload = json.dumps({
            "origin": mea_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "key":"03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e"
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        IssueGGOTransactionHandler().apply(transaction, context)
        self.assertIn(ggo_add, context.states)

        obj = json.loads(context.states[ggo_add].decode('utf8'))
        self.assertEqual(len(obj), 7)

        self.assertEqual(obj['amount'], 123)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['key'], '03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e')


    def test_issue_ggo_fail_reissue(self):
        
        mea_add = 'mea8391c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_add = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        mea = json.dumps({
                'amount': 123,
                'type': 'PRODUCTION',
                'begin': "2020-01-01T12:00:00+00:00",
                'end': "2020-01-01T13:00:00+00:00",
                'sector': 'DK1',
                'key': '03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e'
            }).encode('utf8')

        context = MockContext(states={
            mea_add: mea
        })

        payload = json.dumps({
            "origin": mea_add,
            "tech_type":"T12412",
            "fuel_type":"F010101",
            "key":"03a93d2ee81b16ee95a20356d6560c99da4c1bd3f384923f63906ad0f6fb19e48e"
        }).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[mea_add, ggo_add],
            outputs=[ggo_add],
            payload=payload)

        IssueGGOTransactionHandler().apply(transaction, context)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            IssueGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'GGO already issued!')

        
    