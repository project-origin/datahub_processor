import unittest
import pytest
import json
from datetime import datetime, timezone

from bip32utils import BIP32Key


from src.datahub_processor.ledger_dto import GGO, TransferGGORequest, GGONext, GGOAction, generate_address, AddressPrefix

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from src.datahub_processor.transfer_ggo_handler import TransferGGOTransactionHandler
 
from .mocks import MockContext, FakeTransaction, FakeTransactionHeader

from marshmallow_dataclass import class_schema


class TestIssueGGO(unittest.TestCase):

    def create_fake_transaction(self, inputs, outputs, payload, key: BIP32Key):
        
        return FakeTransaction(
            header=FakeTransactionHeader(
                batcher_public_key=key.PublicKey().hex(),
                dependencies=[],
                family_name="datahub",
                family_version="0.1",
                inputs=inputs,
                outputs=outputs,
                signer_public_key=key.PublicKey().hex()),
            payload=payload
        )


    @pytest.mark.unittest
    def test_identifiers(self):
        handler = TransferGGOTransactionHandler()   
        
        self.assertEqual(handler.family_name, 'TransferGGORequest')

        self.assertEqual(len(handler.family_versions), 1)
        self.assertIn('0.1', handler.family_versions)

        self.assertEqual(len(handler.namespaces), 1)
        self.assertIn('849c0b', handler.namespaces)


    @pytest.mark.unittest
    def test_internal_error(self):
        with self.assertRaises(InternalError) as invalid_transaction:
            TransferGGOTransactionHandler().apply(None, None)

        self.assertEqual(str(invalid_transaction.exception), 'An unknown error has occured.')
        
          
    @pytest.mark.unittest
    def test_transfer_ggo_success(self):

        key = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())

        ggo_src = generate_address(AddressPrefix.GGO, key.PublicKey())
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            next=None
            )).encode('utf8')

        context = MockContext(states={
            ggo_src: ggo
        })

        payload = class_schema(TransferGGORequest)().dumps(TransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key)

        TransferGGOTransactionHandler().apply(transaction, context)


        self.assertIn(ggo_src, context.states)
        obj = json.loads(context.states[ggo_src].decode('utf8'))
        self.assertEqual(len(obj), 8)
        self.assertEqual(obj['origin'], 'meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c')
        self.assertEqual(obj['amount'], 123)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['next']['action'], GGOAction.TRANSFER.value)
        self.assertEqual(len(obj['next']['addresses']), 1)


        self.assertIn(ggo_dst, context.states)
        obj = json.loads(context.states[ggo_dst].decode('utf8'))
        self.assertEqual(len(obj), 8)
        self.assertEqual(obj['origin'], ggo_src)
        self.assertEqual(obj['amount'], 123)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['next'], None)


    @pytest.mark.unittest
    def test_transfer_ggo_no_src_ggo(self):
        key = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        
        ggo_src = generate_address(AddressPrefix.GGO, key.PublicKey())
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={
        })

        payload = class_schema(TransferGGORequest)().dumps(TransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            TransferGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), f'Address "{ggo_src}" does not contain a valid GGO.')


    @pytest.mark.unittest
    def test_transfer_ggo_not_available(self):
        key = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        
        ggo_src = generate_address(AddressPrefix.GGO, key.PublicKey())
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            next=GGONext(GGOAction.TRANSFER, ['somewhereontheledger'])
            )).encode('utf8')

        context = MockContext(states={
            ggo_src: ggo
        })


        payload = class_schema(TransferGGORequest)().dumps(TransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            TransferGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'GGO already has been used')


    @pytest.mark.unittest
    def test_transfer_ggo_not_authorized(self):
        key_owner = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        key_criminal = BIP32Key.fromEntropy("this_key_should_not_be_authorized".encode())
        
        ggo_src = generate_address(AddressPrefix.GGO, key_owner.PublicKey())
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            next=None
            )).encode('utf8')

        context = MockContext(states={
            ggo_src: ggo
        })


        payload = class_schema(TransferGGORequest)().dumps(TransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key_criminal)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            TransferGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid key for GGO')



    @pytest.mark.unittest
    def test_transfer_ggo_address_not_empty(self):
        key = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        
        ggo_src = generate_address(AddressPrefix.GGO, key.PublicKey())
        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=123,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
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
            next=None
            )).encode('utf8')


        context = MockContext(states={
            ggo_src: ggo,
            ggo_dst: ggo2
        })

        payload = class_schema(TransferGGORequest)().dumps(TransferGGORequest(
            origin=ggo_src,
            destination=ggo_dst
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            TransferGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Destination address not empty')
