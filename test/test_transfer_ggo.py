import unittest
import json
from datetime import datetime, timezone
from src.datahub_processor.ledger_dto import GGO, LedgerTransferGGORequest, GGONext, GGOAction

from sawtooth_sdk.processor.exceptions import InvalidTransaction
from src.datahub_processor.transfer_ggo_handler import TransferGGOTransactionHandler
 
from .mocks import MockContext, FakeTransaction, FakeTransactionHeader

from marshmallow_dataclass import class_schema


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
          
    def test_transfer_ggo_success(self):
        
        ggo_src = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            key='039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8',
            next=None
            )).encode('utf8')

        context = MockContext(states={
            ggo_src: ggo
        })

        payload = class_schema(LedgerTransferGGORequest)().dumps(LedgerTransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst,
            key='d3f384923f63906ad06ee903a93d2ee81b14c1b5a20356d6560c99daf6fb19e48e'
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload)

        TransferGGOTransactionHandler().apply(transaction, context)


        self.assertIn(ggo_src, context.states)
        obj = json.loads(context.states[ggo_src].decode('utf8'))
        self.assertEqual(len(obj), 9)
        
        self.assertEqual(obj['origin'], 'meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c')
        self.assertEqual(obj['amount'], 123)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['next']['action'], GGOAction.TRANSFER.value)
        self.assertEqual(len(obj['next']['addresses']), 1)

        # , [ggo_dst])
        self.assertEqual(obj['key'], '039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8')


        self.assertIn(ggo_dst, context.states)
        obj = json.loads(context.states[ggo_dst].decode('utf8'))
        self.assertEqual(len(obj), 9)
        self.assertEqual(obj['origin'], ggo_src)
        self.assertEqual(obj['amount'], 123)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['next'], None)
        self.assertEqual(obj['key'], 'd3f384923f63906ad06ee903a93d2ee81b14c1b5a20356d6560c99daf6fb19e48e')


    def test_transfer_ggo_no_src_ggo(self):
        
        ggo_src = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={
        })

        payload = class_schema(LedgerTransferGGORequest)().dumps(LedgerTransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst,
            key='d3f384923f63906ad06ee903a93d2ee81b14c1b5a20356d6560c99daf6fb19e48e'
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            TransferGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Address "ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c" does not contain a valid GGO.')


    def test_transfer_ggo_not_available(self):
        
        ggo_src = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            key='039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8',
            next=GGONext(GGOAction.TRANSFER, ['somewhereontheledger'])
            )).encode('utf8')

        context = MockContext(states={
            ggo_src: ggo
        })


        payload = class_schema(LedgerTransferGGORequest)().dumps(LedgerTransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst,
            key='d3f384923f63906ad06ee903a93d2ee81b14c1b5a20356d6560c99daf6fb19e48e'
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            TransferGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'GGO already has been used')


    def test_transfer_ggo_not_authorized(self):
        
        ggo_src = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            key='ff1294df728047a6c9fd0a37b9b8d5039c6c728796613c8fc4b3f0a09fc4906be8',
            next=None
            )).encode('utf8')

        context = MockContext(states={
            ggo_src: ggo
        })


        payload = class_schema(LedgerTransferGGORequest)().dumps(LedgerTransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst,
            key='d3f384923f63906ad06ee903a93d2ee81b14c1b5a20356d6560c99daf6fb19e48e'
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            TransferGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Unauthorized transfer on GGO')



    def test_transfer_ggo_address_not_empty(self):
        
        ggo_src = 'ggoaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            key='039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8',
            next=None
            )).encode('utf8')

        ggo2 = GGO.get_schema().dumps(GGO(
            origin='meaaaa29d5271edd8b4c2714e3c8979c1c37509b1de4a7f9f1c59e0efc2ed285e7c96c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            key='d3f384923f63906ad06ee903a93d2ee81b14c1b5a20356d6560c99daf6fb19e48e',
            next=None
            )).encode('utf8')


        context = MockContext(states={
            ggo_src: ggo,
            ggo_dst: ggo2
        })

        payload = class_schema(LedgerTransferGGORequest)().dumps(LedgerTransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst,
            key='d3f384923f63906ad06ee903a93d2ee81b14c1b5a20356d6560c99daf6fb19e48e'
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            TransferGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Destination address not empty')


        

