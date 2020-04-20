import unittest
import pytest
import json
from datetime import datetime, timezone
from bip32utils import BIP32Key
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory, Signer
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey as PrivateKey

from src.datahub_processor.ledger_dto import GGO, GGONext, GGOAction, Measurement, MeasurementType, RetireGGORequest, SettlementRequest, generate_address, AddressPrefix, Settlement, SettlementPart

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from src.datahub_processor import RetireGGOTransactionHandler, SettlementHandler
 
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
        self.mea_prod_1_add = generate_address(AddressPrefix.MEASUREMENT, self.mea_prod_1_key.PublicKey())
        self.mea_prod_1 = Measurement.get_schema().dumps(Measurement(
                amount=25,
                type=MeasurementType.PRODUCTION,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                sector='DK1'
                )).encode('utf8')

        self.mea_prod_2_add = 'mea_prod_2_add'
        self.mea_prod_3_add = 'mea_prod_3_add'
        

        
        self.mea_con_1_key = master_key.ChildKey(3).ChildKey(1)
        self.mea_con_1_add = generate_address(AddressPrefix.MEASUREMENT, self.mea_con_1_key.PublicKey())
        self.mea_con_1 = Measurement.get_schema().dumps(Measurement(
                amount=150,
                type=MeasurementType.CONSUMPTION,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                sector='DK1'
                )).encode('utf8')

        self.mea_con_2_key = master_key.ChildKey(3).ChildKey(2)
        self.mea_con_2_add = generate_address(AddressPrefix.MEASUREMENT, self.mea_con_2_key.PublicKey())
        self.mea_con_2 = Measurement.get_schema().dumps(Measurement(
                amount=15,
                type=MeasurementType.CONSUMPTION,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                sector='DK1'
                )).encode('utf8')

        self.ggo_1_key = master_key.ChildKey(2).ChildKey(1)
        self.ggo_1_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_1_key.PrivateKey()))   
        self.ggo_1_add = generate_address(AddressPrefix.GGO, self.ggo_1_key.PublicKey())
        self.ggo_1 = GGO.get_schema().dumps(GGO(
                origin=self.mea_prod_1_add,
                amount=10,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                next=None
                )).encode('utf8')

        self.ggo_2_key = master_key.ChildKey(2).ChildKey(2)
        self.ggo_2_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_2_key.PrivateKey()))   
        self.ggo_2_add = generate_address(AddressPrefix.GGO, self.ggo_2_key.PublicKey())
        self.ggo_2 = GGO.get_schema().dumps(GGO(
                origin=self.mea_prod_2_add,
                amount=25,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                next=None
                )).encode('utf8')
        
        self.ggo_3_key = master_key.ChildKey(2).ChildKey(3)
        self.ggo_3_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_3_key.PrivateKey()))   
        self.ggo_3_add = generate_address(AddressPrefix.GGO, self.ggo_3_key.PublicKey())
        self.ggo_3 = GGO.get_schema().dumps(GGO(
                origin=self.mea_prod_3_add,
                amount=15,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                next=None
                )).encode('utf8')

        self.ggo_used_key = master_key.ChildKey(2).ChildKey(54687)
        self.ggo_used_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_used_key.PrivateKey()))   
        self.ggo_used_add = generate_address(AddressPrefix.GGO, self.ggo_used_key.PublicKey())
        self.ggo_used = GGO.get_schema().dumps(GGO(
                origin='mea_prod_used_add',
                amount=15,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                next=GGONext(
                    action=GGOAction.RETIRE,
                    addresses=['mea_con_used_add']
                )
                )).encode('utf8')

        self.ggo_used_multiple_key = master_key.ChildKey(2).ChildKey(54687)
        self.ggo_used_multiple_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_used_multiple_key.PrivateKey()))   
        self.ggo_used_multiple_add = generate_address(AddressPrefix.GGO, self.ggo_used_multiple_key.PublicKey())
        self.ggo_used_multiple = GGO.get_schema().dumps(GGO(
                origin='mea_prod_used_add',
                amount=15,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                next=GGONext(
                    action=GGOAction.RETIRE,
                    addresses=['mea_con_used_add', 'mea_con_used_add2']
                )
                )).encode('utf8')


        self.ggo_transfered_key = master_key.ChildKey(2).ChildKey(54688)
        self.ggo_transfered_signer =  CryptoFactory(context).new_signer(PrivateKey.from_bytes(self.ggo_transfered_key.PrivateKey()))   
        self.ggo_transfered_add = generate_address(AddressPrefix.GGO, self.ggo_transfered_key.PublicKey())
        self.ggo_transfered = GGO.get_schema().dumps(GGO(
                origin='mea_prod_used_add',
                amount=15,
                begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
                end=datetime(2020,1,1,13, tzinfo=timezone.utc),
                tech_type='T12412',
                fuel_type='F010101',
                sector='DK1',
                next=GGONext(
                    action=GGOAction.TRANSFER,
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
                signer_public_key=signer_key.PublicKey().hex()),
            payload=payload
        )

        
    @pytest.mark.unittest
    def test_identifiers(self):
        handler = RetireGGOTransactionHandler()
        
        self.assertEqual(handler.family_name, 'RetireGGORequest')

        self.assertEqual(len(handler.family_versions), 1)
        self.assertIn('0.1', handler.family_versions)

        self.assertEqual(len(handler.namespaces), 1)
        self.assertIn('849c0b', handler.namespaces)

        handler = SettlementHandler()
        
        self.assertEqual(handler.family_name, 'SettlementRequest')

        self.assertEqual(len(handler.family_versions), 1)
        self.assertIn('0.1', handler.family_versions)

        self.assertEqual(len(handler.namespaces), 3)
        self.assertIn('849c0b', handler.namespaces)
        self.assertIn('ba4817', handler.namespaces)
        self.assertIn('5a9839', handler.namespaces)
           

    @pytest.mark.unittest
    def test_internal_retire_error(self):
        with self.assertRaises(InternalError) as invalid_transaction:
            RetireGGOTransactionHandler().apply(None, None)

        self.assertEqual(str(invalid_transaction.exception), 'An unknown error has occured.')
        
    @pytest.mark.unittest
    def test_internal_settlement_error(self):
        with self.assertRaises(InternalError) as invalid_transaction:
            SettlementHandler().apply(None, None)

        self.assertEqual(str(invalid_transaction.exception), 'An unknown error has occured.')
        
          
    @pytest.mark.unittest
    def test_retire_single_ggo_success(self):

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
        })

        transaction = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_1_key)
        RetireGGOTransactionHandler().apply(transaction, context)

        transaction = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)
        SettlementHandler().apply(transaction, context)

        self.assertIn(self.ggo_1_add, context.states)

        obj = json.loads(context.states[self.ggo_1_add].decode('utf8'))
        self.assertEqual(len(obj), 8)
        
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
        print(context.states)
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj['measurement'], self.mea_con_1_add)
        self.assertIn('parts', obj)
        self.assertEqual(len(obj['parts']), 1)
        self.assertEqual(obj['parts'][0]['ggo'], self.ggo_1_add)
        self.assertEqual(obj['parts'][0]['amount'], 10)


    @pytest.mark.unittest
    def test_retire_multiple_ggo_success(self):

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.ggo_2_add: self.ggo_2,
            self.ggo_3_add: self.ggo_3,
            self.mea_con_1_add: self.mea_con_1
        })

        transaction_retire_1 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_1_key)
        RetireGGOTransactionHandler().apply(transaction_retire_1, context)

        transaction_retire_2 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_2_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_2_key)
        RetireGGOTransactionHandler().apply(transaction_retire_2, context)

        transaction_retire_3 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_3_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_3_key)
        RetireGGOTransactionHandler().apply(transaction_retire_3, context)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add, self.ggo_2_add, self.ggo_3_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)
        SettlementHandler().apply(transaction_settlement, context)


        self.assertIn(self.ggo_1_add, context.states)

        obj = json.loads(context.states[self.ggo_1_add].decode('utf8'))
        self.assertEqual(len(obj), 8)
        
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
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj['measurement'], self.mea_con_1_add)
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

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.ggo_2_add: self.ggo_2,
            self.ggo_3_add: self.ggo_3,
            self.mea_con_1_add: self.mea_con_1
        })


        transaction_retire_1 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_1_key)
        RetireGGOTransactionHandler().apply(transaction_retire_1, context)

        transaction_retire_2 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_2_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_2_key)
        RetireGGOTransactionHandler().apply(transaction_retire_2, context)


        transaction_settlement_1 = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add, self.ggo_2_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)
        SettlementHandler().apply(transaction_settlement_1, context)


        transaction_retire_3 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_3_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_3_key)
        RetireGGOTransactionHandler().apply(transaction_retire_3, context)

        transaction_settlement_2 = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_3_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)
        SettlementHandler().apply(transaction_settlement_2, context)


        self.assertIn(self.ggo_1_add, context.states)

        obj = json.loads(context.states[self.ggo_1_add].decode('utf8'))
        self.assertEqual(len(obj), 8)
        
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
        self.assertEqual(len(obj), 2)
        self.assertEqual(obj['measurement'], self.mea_con_1_add)
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

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_prod_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_prod_1_add: self.mea_prod_1
        })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_1_key)
        RetireGGOTransactionHandler().apply(transaction_retire, context)


        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_prod_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)


        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Measurment is not of type consumption')
        
          
    @pytest.mark.unittest
    def test_retire_fail_measurment_not_authorized(self):

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
        })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_2_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire, context)
            SettlementHandler().apply(transaction_settlement, context)
            

        self.assertEqual(str(invalid_transaction.exception), 'Invalid key for GGO')


    @pytest.mark.unittest
    def test_retire_fail_ggo_used(self):

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_used_add: self.ggo_used,
            self.mea_con_1_add: self.mea_con_1
        })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_used_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_used_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_used_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire, context)
            SettlementHandler().apply(transaction_settlement, context)
            
        self.assertEqual(str(invalid_transaction.exception), 'GGO already has been used')


    @pytest.mark.unittest
    def test_retire_fail_not_retired_ggo(self):

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
        })

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)

       
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid retired GGO in settlement')

    
    @pytest.mark.unittest
    def test_retire_fail_transferred(self):

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_transfered_add: self.ggo_transfered,
            self.mea_con_1_add: self.mea_con_1
        })

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_transfered_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)

       
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid retired GGO in settlement')

    
    @pytest.mark.unittest
    def test_retire_fail_retired_multiple(self):

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_used_multiple_add: self.ggo_used_multiple,
            self.mea_con_1_add: self.mea_con_1
        })

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_used_multiple_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)

       
        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid retired GGO in settlement')


    @pytest.mark.unittest
    def test_retire_fail_not_correct_measurement(self):

        set_add_1 = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())
        set_add_2 = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_2_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
        })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add_1
            )).encode('utf8'),
            signer_key=self.ggo_1_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add_2,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire, context)
            SettlementHandler().apply(transaction_settlement, context)
            
        self.assertEqual(str(invalid_transaction.exception), 'Not correct settlement address for measurement')


    @pytest.mark.unittest
    def test_retire_fail_not_equal_settlement(self):

        set_add_1 = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1,
            set_add_1: class_schema(Settlement)().dumps(Settlement(
                measurement=self.mea_con_2_add,
                parts=[]
            )).encode()
            })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add_1
            )).encode('utf8'),
            signer_key=self.ggo_1_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add_1,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_2_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire, context)
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Measurement does not equal settlement measurement')


    @pytest.mark.unittest
    def test_retire_fail_wrong_key(self):

        set_add_1 = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1,
            set_add_1: class_schema(Settlement)().dumps(Settlement(
                measurement=self.mea_con_1_add,
                parts=[]
            )).encode()
            })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add_1
            )).encode('utf8'),
            signer_key=self.ggo_1_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add_1,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_2_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire, context)
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid key for settlement')

    
    @pytest.mark.unittest
    def test_retire_fail_wrong_key_no_settlment(self):

        set_add_1 = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
            })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add_1
            )).encode('utf8'),
            signer_key=self.ggo_1_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add_1,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_2_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire, context)
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid key for measurement')

    
    @pytest.mark.unittest
    def test_retire_fail_already_part_of_settlement(self):

        set_add_1 = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
            })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add_1
            )).encode('utf8'),
            signer_key=self.ggo_1_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add_1,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire, context)
            SettlementHandler().apply(transaction_settlement, context)
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'GGO already part of settlement')


    @pytest.mark.unittest
    def test_retire_fail_ggo_other_settlement(self):

        set_add_1 = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_1_key.PublicKey())
        set_add_2 = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_2_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.mea_con_1_add: self.mea_con_1
            })

        transaction_retire = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add_2
            )).encode('utf8'),
            signer_key=self.ggo_1_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add_1,
                measurement_address=self.mea_con_1_add,
                ggo_addresses=[self.ggo_1_add]
            )).encode('utf8'),
            signer_key=self.mea_con_1_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire, context)
            SettlementHandler().apply(transaction_settlement, context)
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid retired GGO in settlement')


    @pytest.mark.unittest
    def test_retire_fail_too_large(self):

        set_add = generate_address(AddressPrefix.SETTLEMENT, self.mea_con_2_key.PublicKey())

        context = MockContext(states={
            self.ggo_1_add: self.ggo_1,
            self.ggo_2_add: self.ggo_2,
            self.ggo_3_add: self.ggo_3,
            self.mea_con_2_add: self.mea_con_2
        })

        
        transaction_retire_1 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_1_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_1_key)

        transaction_retire_2 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_2_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_2_key)

        transaction_retire_3 = self.create_fake_transaction(
            payload=class_schema(RetireGGORequest)().dumps(RetireGGORequest(
                origin=self.ggo_3_add,
                settlement_address=set_add
            )).encode('utf8'),
            signer_key=self.ggo_3_key)

        transaction_settlement = self.create_fake_transaction(
            payload=class_schema(SettlementRequest)().dumps(SettlementRequest(
                settlement_address=set_add,
                measurement_address=self.mea_con_2_add,
                ggo_addresses=[self.ggo_1_add, self.ggo_2_add, self.ggo_3_add]
            )).encode('utf8'),
            signer_key=self.mea_con_2_key)

        with self.assertRaises(InvalidTransaction) as invalid_transaction:
            RetireGGOTransactionHandler().apply(transaction_retire_1, context)
            RetireGGOTransactionHandler().apply(transaction_retire_2, context)
            RetireGGOTransactionHandler().apply(transaction_retire_3, context)
            SettlementHandler().apply(transaction_settlement, context)

        self.assertEqual(str(invalid_transaction.exception), 'Invalid to retire more that measurement amount')
