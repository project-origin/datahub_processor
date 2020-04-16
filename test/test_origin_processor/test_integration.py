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
from src.ledger_dto import PublishMeasurementRequest, IssueGGORequest, SplitGGORequest, SplitGGOPart, MeasurementType, TransferGGORequest, RetireGGORequest, RetireGGOPart, SignedRetireGGOPart
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

    def generate_address(self, prefix, extended_key: BIP32Key) -> str:
        key_add = extended_key.Address()
        prefix_add = sha512(prefix.encode('utf-8')).hexdigest()[:6]
        return prefix_add + sha512(key_add.encode('utf-8')).hexdigest()[:64]

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

        add = self.generate_address('146fca', key)

        request = PublishMeasurementRequest(
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            sector='DK1',
            type=MeasurementType.PRODUCTION,
            amount=1024,
            key=key.PublicKey().hex()
        )

        return self.send_request(url, request, [add], signer)

            
    def publish_con_measurement(self, url):
        key = self.master_key.ChildKey(10)
        signer = self.crypto.new_signer(PrivateKey.from_bytes(key.PrivateKey()))   

        add = self.generate_address('146fca', key)

        request = PublishMeasurementRequest(
            begin=datetime(2020,1,1,12, tzinfo=timezone.utc),
            end=datetime(2020,1,1,13, tzinfo=timezone.utc),
            sector='DK1',
            type=MeasurementType.CONSUMPTION,
            amount=500,
            key=key.PublicKey().hex()
        )

        return self.send_request(url, request, [add], signer)

            
    def issue_ggo(self, url):
        key = self.master_key.ChildKey(1)
        signer = self.crypto.new_signer(PrivateKey.from_bytes(key.PrivateKey()))   

        mea_add = self.generate_address('146fca', key)
        ggo_add = self.generate_address('2b7eba', key)

        request = IssueGGORequest(
            origin=mea_add,
            destination=ggo_add,
            tech_type='T12441',
            fuel_type='F12412',
            key=key.PublicKey().hex()
        )

        return self.send_request(url, request, [mea_add, ggo_add], signer)


    def split_ggo(self, url):
        key1 = self.master_key.ChildKey(1)
        key2 = self.master_key.ChildKey(2)
        key3 = self.master_key.ChildKey(3)

        signer = self.crypto.new_signer(PrivateKey.from_bytes(key1.PrivateKey()))   

        ggo_add_1 = self.generate_address('2b7eba', key1)
        ggo_add_2 = self.generate_address('2b7eba', key2)
        ggo_add_3 = self.generate_address('2b7eba', key3)

        request = SplitGGORequest(
            origin=ggo_add_1,
            parts=[
                SplitGGOPart(ggo_add_2, 500, key2.PublicKey().hex()),
                SplitGGOPart(ggo_add_3, 524, key3.PublicKey().hex())
            ]
        )

        return self.send_request(url, request, [ggo_add_1, ggo_add_2, ggo_add_3], signer)
        

    def transfer_ggo(self, url):
        key2 = self.master_key.ChildKey(2)
        key4 = self.master_key.ChildKey(4)

        signer = self.crypto.new_signer(PrivateKey.from_bytes(key2.PrivateKey()))   

        ggo_add_2 = self.generate_address('2b7eba', key2)
        ggo_add_4 = self.generate_address('2b7eba', key4)

        request = TransferGGORequest(
            origin=ggo_add_2,
            destination=ggo_add_4,
            key=key4.PublicKey().hex()
        )

        return self.send_request(url, request, [ggo_add_2, ggo_add_4], signer)


        
    def retire_ggo(self, url):
        key_ggo = self.master_key.ChildKey(4)
        key_mea = self.master_key.ChildKey(10)
        key_set = self.master_key.ChildKey(11)

        signer_mea = self.crypto.new_signer(PrivateKey.from_bytes(key_mea.PrivateKey()))   
        signer_ggo = self.crypto.new_signer(PrivateKey.from_bytes(key_ggo.PrivateKey()))   

        mea_add = self.generate_address('146fca', key_mea)
        set_add = self.generate_address('1567f1', key_mea)
        ggo_add = self.generate_address('2b7eba', key_ggo)

        
        part = RetireGGOPart(
            origin=ggo_add,
            settlement_address=set_add
        )

        # part_bytez = class_schema(RetireGGOPart)().dumps(part).encode('utf8')
        part_bytez = str(part).encode('utf8')
        signed_message = signer_ggo.sign(part_bytez)

        request = RetireGGORequest(
            measurement_address=mea_add,
            settlement_address=set_add,
            key=key_set.PublicKey().hex(),
            parts=[SignedRetireGGOPart(
                content=part,
                signature=signed_message
            )]
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

        with DockerCompose(".") as compose:
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
