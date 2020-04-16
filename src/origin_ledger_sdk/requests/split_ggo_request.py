

import marshmallow_dataclass

from typing import List
from dataclasses import dataclass, field
from bip32utils import BIP32Key
from sawtooth_sdk.protobuf.transaction_pb2 import Transaction

from .abstract_request import AbstractRequest
from .helpers import get_signer, generate_address, AddressPrefix

from ...ledger_dto.requests import SplitGGORequest as LedgerSplitGGORequest
from ...ledger_dto.requests import SplitGGOPart as LedgerSplitGGOPart

split_ggo_schema = marshmallow_dataclass.class_schema(LedgerSplitGGORequest)

@dataclass
class SplitGGOPart():
    key: BIP32Key = field()
    amount: int = field()


@dataclass
class SplitGGORequest(AbstractRequest):
    current_key: BIP32Key = field()
    parts: List[SplitGGOPart] = field()

    def get_signed_transactions(self, batch_signer) -> List[Transaction]:

        ggo_address = generate_address(AddressPrefix.GGO, self.current_key)

        addresses = [ggo_address]
        parts = []

        for part in self.parts:
            part_address = generate_address(AddressPrefix.GGO, part.key)
            
            ggo_part = LedgerSplitGGOPart(
                address=part_address,
                amount=part.amount,
                key=part.key.PublicKey().hex()
            )

            addresses.append(part_address)
            parts.append(ggo_part)
            
        request = LedgerSplitGGORequest(
            origin=ggo_address,
            parts=parts)

        byte_obj = self._to_bytes(split_ggo_schema, request)

        return [self.sign_transaction(
            batch_signer,
            get_signer(self.current_key),
            byte_obj,
            inputs=addresses,
            outputs=addresses,
            family_name=LedgerSplitGGORequest.__name__,
            family_version='0.1')]
