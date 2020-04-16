
import marshmallow_dataclass

from typing import List
from dataclasses import dataclass, field
from bip32utils import BIP32Key
from sawtooth_sdk.protobuf.transaction_pb2 import Transaction

from .abstract_request import AbstractRequest
from .helpers import get_signer, generate_address, AddressPrefix

from ...ledger_dto.requests import RetireGGORequest as LedgerRetireGGORequest
from ...ledger_dto.requests import SignedRetireGGOPart as LedgerSignedRetireGGOPart
from ...ledger_dto.requests import RetireGGOPart as LedgerRetireGGOPart

retire_ggo_schema = marshmallow_dataclass.class_schema(LedgerRetireGGORequest)
signed_ggo_schema = marshmallow_dataclass.class_schema(LedgerSignedRetireGGOPart)

@dataclass
class RetireGGORequest(AbstractRequest):
    measurement_key: BIP32Key = field()
    ggo_keys: List[BIP32Key] = field()

    def get_signed_transactions(self, batch_signer) -> List[Transaction]:

        settlement_address = generate_address(AddressPrefix.GGO, self.measurement_key)
        measurement_address = generate_address(AddressPrefix.MEASUREMENT, self.measurement_key)
        
        addresses = [settlement_address, measurement_address]
        parts = []

        for ggo_key in self.ggo_keys:
            ggo_address = generate_address(AddressPrefix.GGO, ggo_key)
            addresses.append(ggo_address)

            ggo_signer = get_signer(ggo_key)

            request = LedgerRetireGGOPart(
                origin=ggo_address,
                settlement_address=settlement_address
            )
            message = str(request).encode('utf8')
            
            parts.append(LedgerSignedRetireGGOPart(
                content=request,
                signature=ggo_signer.sign(message)
            ))

        request = LedgerRetireGGORequest(
            measurement_address=measurement_address,
            settlement_address=settlement_address,
            key=self.measurement_key.PublicKey().hex(),
            parts=parts
        )

        transaction_signer = get_signer(self.measurement_key)
        byte_obj = self._to_bytes(retire_ggo_schema, request)

        signed_transaction = self.sign_transaction(
            batch_signer=batch_signer,
            transaction_signer=transaction_signer,
            payload_bytes=byte_obj,
            inputs=addresses,
            outputs=[settlement_address],
            family_name=LedgerRetireGGORequest.__name__,
            family_version='0.1')

        return [signed_transaction]


