import unittest
import pytest
import requests
import time
import json
from bip32utils import BIP32Key
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory, Signer
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey as PrivateKey
from datetime import datetime, timezone
from testcontainers.compose import DockerCompose
from src.datahub_processor.ledger_dto import PublishMeasurementRequest, IssueGGORequest, SplitGGORequest, SplitGGOPart, MeasurementType, TransferGGORequest, RetireGGORequest, SettlementRequest, generate_address, AddressPrefix
from marshmallow_dataclass import class_schema
from sawtooth_sdk.protobuf.transaction_pb2 import TransactionHeader, Transaction
from hashlib import sha512
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader
from sawtooth_sdk.protobuf.batch_pb2 import Batch as SignedBatch
from sawtooth_sdk.protobuf.batch_pb2 import BatchList


class TestIntegration(unittest.TestCase):

    def setUp(self):
        self.master_key = BIP32Key.fromEntropy("bfdgafgaertaehtaha43514r<aefag".encode())
        context = create_context('secp256k1')
        self.crypto = CryptoFactory(context)

    def send_request(self, url, request, add, signer):

        bytez = class_schema(type(request))().dumps(request).encode('utf8')

        header = TransactionHeader(
            batcher_public_key=signer.get_public_key().as_hex(),
            dependencies=[],
            family_name=type(request).__name__,
            family_version='0.1',
            inputs=add,
            outputs=add,
            payload_sha512=sha512(bytez).hexdigest(),
            signer_public_key=signer.get_public_key().as_hex()
        )

        transaction_header_bytes = header.SerializeToString()
        signature = signer.sign(transaction_header_bytes)

        transaction = Transaction(
            header=transaction_header_bytes,
            header_signature=signature,
            payload=bytez
        )
        
        batch_header_bytes = BatchHeader(
            signer_public_key=signer.get_public_key().as_hex(),
            transaction_ids=[transaction.header_signature],
        ).SerializeToString()
        
        signature = signer.sign(batch_header_bytes)
        batch = SignedBatch(
            header=batch_header_bytes,
            header_signature=signature,
            transactions=[transaction]
        )

        batch_list_bytes = BatchList(batches=[batch]).SerializeToString()

        return requests.post(
            f'{url}/batches',
            batch_list_bytes,
            headers={'Content-Type': 'application/octet-stream'})
        
            
    def publish_prod_measurement(self, url):
        key = self.master_key.ChildKey(1)
        signer = self.crypto.new_signer(PrivateKey.from_bytes(key.PrivateKey()))   

        add = generate_address(AddressPrefix.MEASUREMENT, key.PublicKey())

        request = PublishMeasurementRequest(
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            sector='DK1',
            type=MeasurementType.PRODUCTION,
            amount=1024
        )

        return self.send_request(url, request, [add], signer)

            
    def publish_con_measurement(self, url):
        key = self.master_key.ChildKey(10)
        signer = self.crypto.new_signer(PrivateKey.from_bytes(key.PrivateKey()))   

        add = generate_address(AddressPrefix.MEASUREMENT, key.PublicKey())

        request = PublishMeasurementRequest(
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            sector='DK1',
            type=MeasurementType.CONSUMPTION,
            amount=500
        )

        return self.send_request(url, request, [add], signer)

            
    def issue_ggo(self, url):
        key = self.master_key.ChildKey(1)
        signer = self.crypto.new_signer(PrivateKey.from_bytes(key.PrivateKey()))   

        mea_add = generate_address(AddressPrefix.MEASUREMENT, key.PublicKey())
        ggo_add = generate_address(AddressPrefix.GGO, key.PublicKey())

        request = IssueGGORequest(
            origin=mea_add,
            destination=ggo_add,
            tech_type='T12441',
            fuel_type='F12412'
        )

        return self.send_request(url, request, [mea_add, ggo_add], signer)


    def split_ggo(self, url):
        key1 = self.master_key.ChildKey(1)
        key2 = self.master_key.ChildKey(2)
        key3 = self.master_key.ChildKey(3)

        signer = self.crypto.new_signer(PrivateKey.from_bytes(key1.PrivateKey()))   

        ggo_add_1 = generate_address(AddressPrefix.GGO, key1.PublicKey())
        ggo_add_2 = generate_address(AddressPrefix.GGO, key2.PublicKey())
        ggo_add_3 = generate_address(AddressPrefix.GGO, key3.PublicKey())

        request = SplitGGORequest(
            origin=ggo_add_1,
            parts=[
                SplitGGOPart(ggo_add_2, 500),
                SplitGGOPart(ggo_add_3, 524)
            ]
        )

        return self.send_request(url, request, [ggo_add_1, ggo_add_2, ggo_add_3], signer)
        

    def transfer_ggo(self, url):
        key2 = self.master_key.ChildKey(2)
        key4 = self.master_key.ChildKey(4)

        signer = self.crypto.new_signer(PrivateKey.from_bytes(key2.PrivateKey()))   

        ggo_add_2 = generate_address(AddressPrefix.GGO, key2.PublicKey())
        ggo_add_4 = generate_address(AddressPrefix.GGO, key4.PublicKey())

        request = TransferGGORequest(
            origin=ggo_add_2,
            destination=ggo_add_4
        )

        return self.send_request(url, request, [ggo_add_2, ggo_add_4], signer)


        
    def retire_ggo(self, url):
        key_ggo = self.master_key.ChildKey(4)
        key_mea = self.master_key.ChildKey(10)

        signer_mea = self.crypto.new_signer(PrivateKey.from_bytes(key_mea.PrivateKey()))   
        signer_ggo = self.crypto.new_signer(PrivateKey.from_bytes(key_ggo.PrivateKey()))   

        mea_add = generate_address(AddressPrefix.MEASUREMENT, key_mea.PublicKey())
        set_add = generate_address(AddressPrefix.SETTLEMENT, key_mea.PublicKey())
        ggo_add = generate_address(AddressPrefix.GGO, key_ggo.PublicKey())
        
        retire_request = RetireGGORequest(
            origin=ggo_add,
            settlement_address=set_add)
        
        retire_response = self.send_request(url, retire_request, [mea_add, set_add, ggo_add], signer_ggo)

        self.wait_for_commit(retire_response.json()['link'])

        request = SettlementRequest(
            settlement_address=set_add,
            measurement_address=mea_add,
            ggo_addresses=[ggo_add]
        )

        return self.send_request(url, request, [mea_add, set_add, ggo_add], signer_mea)
        
        

    def wait_for_commit(self, link):
        status = 'PENDING'

        i = 0

        while status == 'PENDING' or status == 'UNKNOWN':
            response = requests.get(link)
            print(response.content, "\n")
            time.sleep(1)
            status = response.json()['data'][0]['status']
            i += 1

            if i > 10:
                raise Exception("Timeout")


        self.assertEqual(status, "COMMITTED")

    @pytest.mark.integrationtest
    @pytest.mark.trylast
    def test_integration(self):

        with DockerCompose("./test") as compose:
            time.sleep(5)

            host = compose.get_service_host('rest-api', 8008)
            port = compose.get_service_port('rest-api', 8008)

            url = f'http://{host}:{port}'

            print('\npublish production measurement:')
            mea_response = self.publish_prod_measurement(url)
            self.wait_for_commit(mea_response.json()['link'])
            
            print('\npublish consumption measurement:')
            mea_con_response = self.publish_con_measurement(url)
            self.wait_for_commit(mea_con_response.json()['link'])


            print('\nissue ggo:')
            ggo_response = self.issue_ggo(url)
            self.wait_for_commit(ggo_response.json()['link'])

            print('\nsplit ggo:')
            split_response = self.split_ggo(url)
            self.wait_for_commit(split_response.json()['link'])


            print('\ntransfer ggo:')
            transfer_response = self.transfer_ggo(url)
            self.wait_for_commit(transfer_response.json()['link'])


            print('\nretire ggo:')
            retire_response = self.retire_ggo(url)
            self.wait_for_commit(retire_response.json()['link'])
