import unittest
import pytest
import random
import string

from datetime import datetime, timezone
from bip32utils import BIP32Key

from sawtooth_signing import create_context

from src.origin_ledger_sdk import Ledger, Batch, BatchStatus, PublishMeasurementRequest, MeasurementType,IssueGGORequest, TransferGGORequest, SplitGGOPart, SplitGGORequest, RetireGGORequest
from src.origin_ledger_sdk.requests.helpers import get_signer


def randomString(stringLength=32):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


class TestLedger(unittest.TestCase):

    def setUp(self):
        self.ledger = Ledger('http://localhost:8008')


    @pytest.mark.unittest
    def test_sign_with_bip32(self):
        context = create_context('secp256k1')

        key = BIP32Key.fromEntropy("bfdgafgaertaehtaha43514r<aefag".encode())
        signer = get_signer(key)

        signed_message = signer.sign("Hello world".encode())
        self.assertEqual(signed_message, '01799dd2268934d11fcc95a164fb1b32c4a0da3284461890838f31f48e37917c532174048672ba1b0f028e5d74943966e799acadd41c7aa433fcf254e794fbed')

        result = context.verify(signed_message, "Hello world".encode(), signer.get_public_key())
        self.assertEqual(result, True)
        

    @pytest.mark.unittest
    def test_build_publish_measurement_request(self):
        key = BIP32Key.fromEntropy("bfdgafgaertaehtaha43514r<aefag".encode())

        request = PublishMeasurementRequest(
            extended_key=key,
            begin=datetime(2020,1,1,12),
            end=datetime(2020,1,1,12),
            sector='DK1',
            type=MeasurementType.PRODUCTION,
            amount=1244
        )

        batch = Batch(signer_key=key)
        batch.add_request(request)
        batch.get_signed_batch()

        
    @pytest.mark.unittest
    def test_build_issue_ggo_request(self):
        key = BIP32Key.fromEntropy("bfdgafgaertaehtaha43514r<aefag".encode())
        
        request = IssueGGORequest(
            extended_key=key,
            tech_type="T12412",
            fuel_type="F123123"
        )

        batch = Batch(signer_key=key)
        batch.add_request(request)
        batch.get_signed_batch()

       
    @pytest.mark.unittest
    def test_build_transfer_ggo_request(self):
        key = BIP32Key.fromEntropy("bfdgafgaertaehtaha43514r<aefag".encode())
    
        request = TransferGGORequest(
            current_key=key.ChildKey(1),
            new_key=key.ChildKey(2),
        )

        batch = Batch(signer_key=key)
        batch.add_request(request)
        batch.get_signed_batch()

        
    @pytest.mark.unittest
    def test_build_split_ggo_request(self):
        key = BIP32Key.fromEntropy("bfdgafgaertaehtaha43514r<aefag".encode())
  
        request = SplitGGORequest(
            current_key=key.ChildKey(1),
            parts=[
                SplitGGOPart(key=key.ChildKey(2), amount=124),
                SplitGGOPart(key=key.ChildKey(3), amount=124)
            ]
        )

        batch = Batch(signer_key=key)
        batch.add_request(request)
        batch.get_signed_batch()

               
    @pytest.mark.unittest
    def test_build_retire_ggo_request(self):
        key = BIP32Key.fromEntropy("bfdgafgaertaehtaha43514r<aefag".encode())
  
        request = RetireGGORequest(
            measurement_key=key.ChildKey(1),
            ggo_keys=[key.ChildKey(2), key.ChildKey(3)] 
        )

        batch = Batch(signer_key=key)
        batch.add_request(request)
        batch.get_signed_batch()
