import unittest
import pytest
import json
from datetime import datetime, timezone
from bip32utils import BIP32Key
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory, Signer
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey as PrivateKey

from src.datahub_processor.ledger_dto import GGO, GGONext, GGOAction, Measurement, MeasurementType, RetireGGOPart, SignedRetireGGOPart, RetireGGORequest

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from src.datahub_processor.retire_ggo_handler import RetireGGOTransactionHandler
 
from .mocks import MockContext, FakeTransaction, FakeTransactionHeader

from marshmallow_dataclass import class_schema


class TestIssueGGO(unittest.TestCase):

    def setUp(self):
        master_key = BIP32Key.fromEntropy("bfdgafgaertaehtaha43514r<aefag".encode())
        context = create_context('secp256k1')

        self.key_1 = master_key.ChildKey(1)
        self.false_key = master_key.ChildKey(1241).ChildKey(1241)
        self.false_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.false_key.PrivateKey()))   

        
        self.mea_prod_1_key = master_key.ChildKey(1).ChildKey(1)
        self.mea_prod_1_add = 'mea_prod_1_add'
        self.mea_prod_1 = Measurement.get_schema().dumps(Measurement(
                amount=25,
                type=MeasurementType.PRODUCTION,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                sector='DK1',
                key=self.mea_prod_1_key.PublicKey().hex(),
                )).encode('utf8')

        self.mea_prod_2_add = 'mea_prod_2_add'
        self.mea_prod_3_add = 'mea_prod_3_add'
        

        
        self.mea_con_1_key = master_key.ChildKey(3).ChildKey(1)
        self.mea_con_1_add = 'mea_con_1_add'
        self.mea_con_1 = Measurement.get_schema().dumps(Measurement(
                amount=150,
                type=MeasurementType.CONSUMPTION,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                sector='DK1',
                key=self.mea_con_1_key.PublicKey().hex(),
                )).encode('utf8')

        self.mea_con_2_key = master_key.ChildKey(3).ChildKey(2)
        self.mea_con_2_add = 'mea_con_2_add'
        self.mea_con_2 = Measurement.get_schema().dumps(Measurement(
                amount=15,
                type=MeasurementType.CONSUMPTION,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                sector='DK1',
                key=self.mea_con_2_key.PublicKey().hex(),
                )).encode('utf8')

        self.ggo_1_key = master_key.ChildKey(2).ChildKey(1)
        self.ggo_1_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_1_key.PrivateKey()))   
        self.ggo_1_add = 'ggo_1_add'
        self.ggo_1 = GGO.get_schema().dumps(GGO(
                origin=self.mea_prod_1_add,
                amount=10,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                key=self.ggo_1_key.PublicKey().hex(),
                next=None
                )).encode('utf8')

        self.ggo_2_key = master_key.ChildKey(2).ChildKey(2)
        self.ggo_2_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_2_key.PrivateKey()))   
        self.ggo_2_add = 'ggo_2_add'
        self.ggo_2 = GGO.get_schema().dumps(GGO(
                origin=self.mea_prod_2_add,
                amount=25,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                key=self.ggo_2_key.PublicKey().hex(),
                next=None
                )).encode('utf8')
        
        self.ggo_3_key = master_key.ChildKey(2).ChildKey(3)
        self.ggo_3_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_3_key.PrivateKey()))   
        self.ggo_3_add = 'ggo_3_add'
        self.ggo_3 = GGO.get_schema().dumps(GGO(
                origin=self.mea_prod_3_add,
                amount=15,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                key=self.ggo_3_key.PublicKey().hex(),
                next=None
                )).encode('utf8')

        self.ggo_used_key = master_key.ChildKey(2).ChildKey(54687)
        self.ggo_used_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_used_key.PrivateKey()))   
        self.ggo_used_add = 'ggo_used_add'
        self.ggo_used = GGO.get_schema().dumps(GGO(
                origin='mea_prod_used_add',
                amount=15,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                key=self.ggo_used_key.PublicKey().hex(),
                next=GGONext(
                    action=GGOAction.RETIRE,
                    addresses=['mea_con_used_add']
                )
                )).encode('utf8')


    def create_fake_transaction(self, payload, signer_key):
        return FakeTransaction(
            header=FakeTransactionHeader(
                batcher_public_key="039c6c728796613c8fc4bff1294df728047a6c9fd0a37b9b8d53f0a09fc4906be8",
                dependencies=[],
                family_name="datahub",
                family_version="0.1",
                inputs=[],
                outputs=[],
                payload_sha512="d70bfa9020d4f03a7ca4e706b81d3d8b3cf93fe9942b83f1e1661517d8da8991708a87ca7a50fd536fdd218e7ebe5385454286693897cd96686dca6f5649256e",
                signer_public_key=signer_key.PublicKey().hex()),
            header_signature="7651c96e081880de546683b7f47ca9124bd398bb7ad5880813a7cb882d2901e405e386730d8ca04aabdfa354b6b66105b1b7e51141d25bf34a0a245004209e45",
            payload=payload
        )

        
    @pytest.mark.unittest
    def test_identifiers(self):
        handler = RetireGGOTransactionHandler()
        
        self.assertEqual(handler.family_name, 'RetireGGORequest')

        self.assertEqual(len(handler.family_versions), 1)
        self.assertIn('0.1', handler.family_versions)

        self.assertEqual(len(handler.namespaces), 2)
        self.assertIn('849c0b', handler.namespaces)
        self.assertIn('ba4817', handler.namespaces)
           

    @pytest.mark.unittest
    def test_internal_error(self):
        with self.assertRaises(InternalError) as invalid_transaction:
            RetireGGOTransactionHandler().apply(None, None)

        self.assertEqual(str(invalid_transaction.exception), 'An unknown error has occured.')
        
          
    @pytest.mark.unittest
    def test_retire_single_ggo_success(self):

        set_add = 'setad_' + self.mea_con_1_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add :self.mea_con_1
        })

        part = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_add,
            key="new_key",
            parts=[SignedRetireGGOPart(
                content=part,
                signature=self.ggo_1_signer.sign(part.get_signature_bytes())
            )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.mea_con_1_key)

        RetireGGOTransactionHandler().apply(transaction, context)

        self.assertIn(self.ggo_1_add, context.states)

        obj = json.loads(context.states[self.ggo_1_add].decode('utf8'))
        self.assertEqual(len(obj), 9)
        
        self.assertEqual(obj['origin'], self.mea_prod_1_add)
        self.assertEqual(obj['amount'], 10)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['next']['action'], GGOAction.RETIRE.value)
        self.assertEqual(len(obj['next']['addresses']), 1)
        self.assertEqual(obj['next']['addresses'][0], set_add)

        self.assertIn(set_add, context.states)
        obj = json.loads(context.states[set_add].decode('utf8'))
        self.assertEqual(len(obj), 3)
        self.assertEqual(obj['measurement'], self.mea_con_1_add)
        self.assertEqual(obj['key'], 'new_key')
        self.assertIn('parts', obj)
        self.assertEqual(len(obj['parts']), 1)
        self.assertEqual(obj['parts'][0]['ggo'], self.ggo_1_add)
        self.assertEqual(obj['parts'][0]['amount'], 10)


    @pytest.mark.unittest
    def test_retire_multiple_ggo_success(self):

        set_add = 'setad_' + self.mea_con_1_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.ggo_2_add: self.ggo_2,
            self.ggo_3_add: self.ggo_3,
            self.mea_con_1_add: self.mea_con_1
        })

        part_1 = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_add)

        part_2 = RetireGGOPart(
                origin=self.ggo_2_add,
                settlement_address=set_add)
            
        part_3 = RetireGGOPart(
                origin=self.ggo_3_add,
                settlement_address=set_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_add,
            key='new_key',
            parts=[
                SignedRetireGGOPart(
                    content=part_1,
                    signature=self.ggo_1_signer.sign(part_1.get_signature_bytes())
                ),
                SignedRetireGGOPart(
                    content=part_2,
                    signature=self.ggo_2_signer.sign(part_2.get_signature_bytes())
                ),
                SignedRetireGGOPart(
                    content=part_3,
                    signature=self.ggo_3_signer.sign(part_3.get_signature_bytes())
                )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.mea_con_1_key
            )

        RetireGGOTransactionHandler().apply(transaction, context)

        self.assertIn(self.ggo_1_add, context.states)

        obj = json.loads(context.states[self.ggo_1_add].decode('utf8'))
        self.assertEqual(len(obj), 9)
        
        self.assertEqual(obj['origin'], self.mea_prod_1_add)
        self.assertEqual(obj['amount'], 10)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['next']['action'], GGOAction.RETIRE.value)
        self.assertEqual(len(obj['next']['addresses']), 1)
        self.assertEqual(obj['next']['addresses'][0], set_add)

        self.assertIn(set_add, context.states)
        obj = json.loads(context.states[set_add].decode('utf8'))
        self.assertEqual(len(obj), 3)
        self.assertEqual(obj['measurement'], self.mea_con_1_add)
        self.assertEqual(obj['key'], 'new_key')
        self.assertIn('parts', obj)
        self.assertEqual(len(obj['parts']), 3)
        self.assertEqual(obj['parts'][0]['ggo'], self.ggo_1_add)
        self.assertEqual(obj['parts'][0]['amount'], 10)
        self.assertEqual(obj['parts'][1]['ggo'], self.ggo_2_add)
        self.assertEqual(obj['parts'][1]['amount'], 25)
        self.assertEqual(obj['parts'][2]['ggo'], self.ggo_3_add)
        self.assertEqual(obj['parts'][2]['amount'], 15)


        
    @pytest.mark.unittest
    def test_retire_multiple_ggo_two_transations_success(self):

        set_add = 'setad_' + self.mea_con_1_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.ggo_2_add: self.ggo_2,
            self.ggo_3_add: self.ggo_3,
            self.mea_con_1_add: self.mea_con_1
        })

        part_1 = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_add)

        part_2 = RetireGGOPart(
                origin=self.ggo_2_add,
                settlement_address=set_add)
            
        part_3 = RetireGGOPart(
                origin=self.ggo_3_add,
                settlement_address=set_add)

        payload_1 = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_add,
            key=self.key_1.PublicKey().hex(),
            parts=[
                SignedRetireGGOPart(
                    content=part_1,
                    signature=self.ggo_1_signer.sign(part_1.get_signature_bytes())
                )]
        )).encode('utf8')
        
        transaction_1 = self.create_fake_transaction(
            payload=payload_1,
            signer_key=self.mea_con_1_key
            )

        RetireGGOTransactionHandler().apply(transaction_1, context)

        payload_2 = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_add,
            key='new_key',
            parts=[
                SignedRetireGGOPart(
                    content=part_2,
                    signature=self.ggo_2_signer.sign(part_2.get_signature_bytes())
                ),
                SignedRetireGGOPart(
                    content=part_3,
                    signature=self.ggo_3_signer.sign(part_3.get_signature_bytes())
                )]
        )).encode('utf8')

        transaction_2 = self.create_fake_transaction(
            payload=payload_2,
            signer_key=self.key_1)

        RetireGGOTransactionHandler().apply(transaction_2, context)
                

        self.assertIn(self.ggo_1_add, context.states)

        obj = json.loads(context.states[self.ggo_1_add].decode('utf8'))
        self.assertEqual(len(obj), 9)
        
        self.assertEqual(obj['origin'], self.mea_prod_1_add)
        self.assertEqual(obj['amount'], 10)
        self.assertEqual(obj['begin'], '2020-01-01T12:00:00+00:00')
        self.assertEqual(obj['end'], '2020-01-01T13:00:00+00:00')
        self.assertEqual(obj['sector'], 'DK1')
        self.assertEqual(obj['tech_type'], 'T12412')
        self.assertEqual(obj['fuel_type'], 'F010101')
        self.assertEqual(obj['next']['action'], GGOAction.RETIRE.value)
        self.assertEqual(len(obj['next']['addresses']), 1)
        self.assertEqual(obj['next']['addresses'][0], set_add)

        self.assertIn(set_add, context.states)
        obj = json.loads(context.states[set_add].decode('utf8'))
        self.assertEqual(len(obj), 3)
        self.assertEqual(obj['measurement'], self.mea_con_1_add)
        self.assertEqual(obj['key'], 'new_key')
        self.assertIn('parts', obj)
        self.assertEqual(len(obj['parts']), 3)
        self.assertEqual(obj['parts'][0]['ggo'], self.ggo_1_add)
        self.assertEqual(obj['parts'][0]['amount'], 10)
        self.assertEqual(obj['parts'][1]['ggo'], self.ggo_2_add)
        self.assertEqual(obj['parts'][1]['amount'], 25)
        self.assertEqual(obj['parts'][2]['ggo'], self.ggo_3_add)
        self.assertEqual(obj['parts'][2]['amount'], 15)

          
    @pytest.mark.unittest
    def test_retire_fail_measurment_not_consumption(self):

        set_1_add = 'setad_' + self.mea_prod_1_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_prod_1_add: self.mea_prod_1
        })

        part = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_1_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_prod_1_add,
            settlement_address=set_1_add,
            key='new_key',
            parts=[SignedRetireGGOPart(
                content=part,
                signature=self.ggo_1_signer.sign(part.get_signature_bytes())
            )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.ggo_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Measurment is not of type consumption')
        
          
    @pytest.mark.unittest
    def test_retire_fail_measurment_not_authorized(self):

        set_1_add = 'setad_' + self.mea_con_1_add[6:]


        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
        })

        part = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_1_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_1_add,
            key='new_key',
            parts=[SignedRetireGGOPart(
                content=part,
                signature=self.ggo_1_signer.sign(part.get_signature_bytes())
            )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.false_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Unauthorized retire to measurement')



    @pytest.mark.unittest
    def test_retire_fail_invalid_settlement_add(self):

        set_1_add = 'setad_' + self.mea_con_1_add[6:]
        set_2_add = 'setad_' + self.mea_con_2_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
        })

        part = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_2_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_1_add,
            key='new_key',
            parts=[SignedRetireGGOPart(
                content=part,
                signature=self.ggo_1_signer.sign(part.get_signature_bytes())
            )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid destination, not the same as measurement')



    @pytest.mark.unittest
    def test_retire_fail_ggo_used(self):

        set_1_add = 'setad_' + self.mea_con_1_add[6:]

        context = MockContext(states={
            self.ggo_used_add: self.ggo_used,
            self.mea_con_1_add: self.mea_con_1
        })

        part = RetireGGOPart(
                origin=self.ggo_used_add,
                settlement_address=set_1_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_1_add,
            key='new_key',
            parts=[SignedRetireGGOPart(
                content=part,
                signature=self.ggo_used_signer.sign(part.get_signature_bytes())
            )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'GGO already has been used')



    @pytest.mark.unittest
    def test_retire_fail_ggo_not_authorized(self):

        set_1_add = 'setad_' + self.mea_con_1_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
        })

        part = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_1_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_1_add,
            key='new_key',
            parts=[SignedRetireGGOPart(
                content=part,
                signature=self.false_signer.sign(part.get_signature_bytes())
            )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Unauthorized retire on GGO')



    @pytest.mark.unittest
    def test_retire_fail_too_large(self):

        set_add = 'setad_' + self.mea_con_2_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.ggo_2_add: self.ggo_2,
            self.ggo_3_add: self.ggo_3,
            self.mea_con_2_add: self.mea_con_2
        })

        part_1 = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_add)

        part_2 = RetireGGOPart(
                origin=self.ggo_2_add,
                settlement_address=set_add)
            
        part_3 = RetireGGOPart(
                origin=self.ggo_3_add,
                settlement_address=set_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_2_add,
            settlement_address=set_add,
            key='new_key',
            parts=[
                SignedRetireGGOPart(
                    content=part_1,
                    signature=self.ggo_1_signer.sign(part_1.get_signature_bytes())
                ),
                SignedRetireGGOPart(
                    content=part_2,
                    signature=self.ggo_2_signer.sign(part_2.get_signature_bytes())
                ),
                SignedRetireGGOPart(
                    content=part_3,
                    signature=self.ggo_3_signer.sign(part_3.get_signature_bytes())
                )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.mea_con_2_key
            )

        
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid to retire more that measurement amount')

        
    @pytest.mark.unittest
    def test_retire_fail_different_measurement_add(self):

        set_add = 'setad_' + self.mea_con_1_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.ggo_2_add: self.ggo_2,
            self.ggo_3_add: self.ggo_3,
            self.mea_con_1_add: self.mea_con_1,
            self.mea_con_2_add: self.mea_con_2
        })

        part_1 = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_add)

    
        payload_1 = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_add,
            key=self.key_1.PublicKey().hex(),
            parts=[
                SignedRetireGGOPart(
                    content=part_1,
                    signature=self.ggo_1_signer.sign(part_1.get_signature_bytes())
                )]
        )).encode('utf8')
        
        transaction_1 = self.create_fake_transaction(
            payload=payload_1,
            signer_key=self.mea_con_1_key
            )

        part_2 = RetireGGOPart(
                origin=self.ggo_2_add,
                settlement_address=set_add)


        RetireGGOTransactionHandler().apply(transaction_1, context)

        payload_2 = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_2_add,
            settlement_address=set_add,
            key='new_key',
            parts=[
                SignedRetireGGOPart(
                    content=part_2,
                    signature=self.ggo_2_signer.sign(part_2.get_signature_bytes())
                )]
        )).encode('utf8')

        transaction_2 = self.create_fake_transaction(
            payload=payload_2,
            signer_key=self.mea_con_2_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_2, context)

        self.assertEqual(str(invalid_transaction.exception), 'Measurement does not equal settlement measurement')

                
    @pytest.mark.unittest
    def test_retire_fail_multiple_settlement_wrong_key(self):


        set_add = 'setad_' + self.mea_con_1_add[6:]

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.ggo_2_add: self.ggo_2,
            self.ggo_3_add: self.ggo_3,
            self.mea_con_1_add: self.mea_con_1
        })

        part_1 = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_add)

    
        payload_1 = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_add,
            key=self.key_1.PublicKey().hex(),
            parts=[
                SignedRetireGGOPart(
                    content=part_1,
                    signature=self.ggo_1_signer.sign(part_1.get_signature_bytes())
                )]
        )).encode('utf8')
        
        transaction_1 = self.create_fake_transaction(
            payload=payload_1,
            signer_key=self.mea_con_1_key
            )

        part_2 = RetireGGOPart(
                origin=self.ggo_2_add,
                settlement_address=set_add)


        RetireGGOTransactionHandler().apply(transaction_1, context)

        payload_2 = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_add,
            key='new_key',
            parts=[
                SignedRetireGGOPart(
                    content=part_2,
                    signature=self.ggo_2_signer.sign(part_2.get_signature_bytes())
                )]
        )).encode('utf8')

        transaction_2 = self.create_fake_transaction(
            payload=payload_2,
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_2, context)

        self.assertEqual(str(invalid_transaction.exception), 'Unauthorized retire to settlement')


    @pytest.mark.unittest
    def test_retire_fail_wrong_settlement_add(self):

        set_1_add = 'wrong_settlement_add'

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
        })

        part = RetireGGOPart(
                origin=self.ggo_1_add,
                settlement_address=set_1_add)

        payload = class_schema(RetireGGORequest)().dumps(RetireGGORequest(
            measurement_address=self.mea_con_1_add,
            settlement_address=set_1_add,
            key='new_key',
            parts=[SignedRetireGGOPart(
                content=part,
                signature=self.ggo_1_signer.sign(part.get_signature_bytes())
            )]
        )).encode('utf8')

        transaction = self.create_fake_transaction(
            payload=payload,
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction, context)

        self.assertEqual(str(invalid_transaction.exception), 'Not correct settlement address for measurement')