import unittest
import pytest
import json
from datetime import datetime, timezone
from src.datahub_processor.ledger_dto import GGO, SplitGGOPart, SplitGGORequest, GGONext, GGOAction, generate_address, AddressPrefix
from bip32utils import BIP32Key

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from src.datahub_processor.split_ggo_handler import SplitGGOTransactionHandler
 
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
        handler = SplitGGOTransactionHandler()
        
        self.assertEqual(handler.family_name, 'SplitGGORequest')

        self.assertEqual(len(handler.family_versions), 1)
        self.assertIn('0.1', handler.family_versions)

        self.assertEqual(len(handler.namespaces), 1)
        self.assertIn('849c0b', handler.namespaces)


    @pytest.mark.unittest
    def test_internal_error(self):
        with self.assertRaises(InternalError) as invalid_transaction:
            SplitGGOTransactionHandler().apply(None, None)

        self.assertEqual(str(invalid_transaction.exception), 'An unknown error has occured.')
        
          
          
    @pytest.mark.unittest
    def test_transfer_ggo_success(self):
        
        key = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        ggo_src = generate_address(AddressPrefix.GGO, key.PublicKey())

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=80,
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            tech_type='T12412',
            fuel_type='F010101',
            sector='DK1',
            next=None,
            emissions={
                "co2": {
                    "value": 1113342.14,
                    "unit": "g/Wh",
                },
                "so2": {
                    "value": 9764446,
                    "unit": "g/Wh",
                },
            }
            )).encode('utf8')

        context = MockContext(states={
            ggo_src: ggo
        })

        payload = class_schema(SplitGGORequest)().dumps(SplitGGORequest(
            origin=ggo_src,
            parts=[
                SplitGGOPart(address="split1_add", amount=10),
                SplitGGOPart(address="split2_add", amount=20),
                SplitGGOPart(address="split3_add", amount=50)
            ]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, "split1_add", "split2_add", "split3_add"],
            outputs=[ggo_src, "split1_add", "split2_add", "split3_add"],
            payload=payload,
            key=key)

        SplitGGOTransactionHandler().apply(transaction, context)


        self.assertIn(ggo_src, context.states)
        obj = json.loads(context.states[ggo_src].decode('utf8'))
        self.assertEqual(len(obj), 9)
        
        self.assertEqual(obj['origin'], 'meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c')
        self.assertEqual(obj['amount'], 80)
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
        self.assertEqual(obj['next']['action'], GGOAction.SPLIT.name)
        self.assertEqual(len(obj['next']['addresses']), 3)
        self.assertEqual(obj['next']['addresses'][0], 'split1_add')
        self.assertEqual(obj['next']['addresses'][1], 'split2_add')
        self.assertEqual(obj['next']['addresses'][2], 'split3_add')
        

        obj = json.loads(context.states['split1_add'].decode('utf8'))
        self.assertEqual(len(obj), 9)
        self.assertEqual(obj['origin'], ggo_src)
        self.assertEqual(obj['amount'], 10)
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

        obj = json.loads(context.states['split2_add'].decode('utf8'))
        self.assertEqual(len(obj), 9)
        self.assertEqual(obj['origin'], ggo_src)
        self.assertEqual(obj['amount'], 20)
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


        obj = json.loads(context.states['split3_add'].decode('utf8'))
        self.assertEqual(len(obj), 9)
        self.assertEqual(obj['origin'], ggo_src)
        self.assertEqual(obj['amount'], 50)
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
    def test_transfer_ggo_sum_not_equal(self):
        
        key = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        ggo_src = generate_address(AddressPrefix.GGO, key.PublicKey())

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=40,
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

        payload = class_schema(SplitGGORequest)().dumps(SplitGGORequest(
            origin=ggo_src,
            parts=[
                SplitGGOPart(address="split1_add", amount=10),
                SplitGGOPart(address="split2_add", amount=20),
            ]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, "split1_add", "split2_add", "split3_add"],
            outputs=[ggo_src, "split1_add", "split2_add", "split3_add"],
            payload=payload,
            key=key)


        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SplitGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'The sum of the parts does not equal the whole')



    @pytest.mark.unittest
    def test_transfer_ggo_no_src_ggo(self):
        
        key = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        ggo_src = generate_address(AddressPrefix.GGO, key.PublicKey())

        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        context = MockContext(states={
        })
        
        payload = class_schema(SplitGGORequest)().dumps(SplitGGORequest(
            origin=ggo_src,
            parts=[
                SplitGGOPart(address="split1_add", amount=10),
                SplitGGOPart(address="split2_add", amount=20)
            ]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SplitGGOTransactionHandler().apply(transaction, context)

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

        payload = class_schema(SplitGGORequest)().dumps(SplitGGORequest(
            origin=ggo_src,
            parts=[
                SplitGGOPart(address="split1_add", amount=10),
                SplitGGOPart(address="split2_add", amount=20)
            ]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SplitGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'GGO already has been used')


    @pytest.mark.unittest
    def test_transfer_ggo_not_authorized(self):
        key_owner = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        key_criminal = BIP32Key.fromEntropy("this_key_should_not_be_authorized".encode())
        
        ggo_src = generate_address(AddressPrefix.GGO, key_owner.PublicKey())

        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=30,
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

        payload = class_schema(SplitGGORequest)().dumps(SplitGGORequest(
            origin=ggo_src,
            parts=[
                SplitGGOPart(address="split1_add", amount=10),
                SplitGGOPart(address="split2_add", amount=20)
            ]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key_criminal)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SplitGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid key for GGO')



    @pytest.mark.unittest
    def test_transfer_ggo_address_not_empty(self):
        
        key = BIP32Key.fromEntropy("the_valid_key_that_owns_the_specific_ggo".encode())
        ggo_src = generate_address(AddressPrefix.GGO, key.PublicKey())

        ggo_dst = 'ggonextc37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c'

        ggo = GGO.get_schema().dumps(GGO(
            origin='meaaaa1c37509b1de4a7f9f1c59e0efc2ed285e7c96c29d5271edd8b4c2714e3c8979c',
            amount=30,
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
            "split1_add": ggo2
        })

        payload = class_schema(SplitGGORequest)().dumps(SplitGGORequest(
            origin=ggo_src,
            parts=[
                SplitGGOPart(address="split1_add", amount=10),
                SplitGGOPart(address="split2_add", amount=20)
            ]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            inputs=[ggo_src, ggo_dst],
            outputs=[ggo_src, ggo_dst],
            payload=payload,
            key=key)
   
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SplitGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Destination address not empty')
