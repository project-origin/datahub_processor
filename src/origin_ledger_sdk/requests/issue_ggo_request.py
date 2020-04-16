

from sawtooth_sdk.protobuf.transaction_pb2 import Transaction
from bip32utils import BIP32Key

from dataclasses import dataclass, field
import marshmallow_dataclass
from typing import List

from .abstract_request import AbstractRequest
from .helpers import generate_address, AddressPrefix

from ...ledger_dto.requests import IssueGGORequest as LedgerIssueGGORequest
issue_ggo_schema = marshmallow_dataclass.class_schema(LedgerIssueGGORequest)


@dataclass
class IssueGGORequest(AbstractRequest):
    extended_key: BIP32Key = field()
    tech_type: str = field()
    fuel_type: str = field()

    def get_signed_transactions(self, batch_signer) -> List[Transaction]:

        ggo_address = generate_address(AddressPrefix.GGO, self.extended_key)
        measurement_address = generate_address(AddressPrefix.MEASUREMENT, self.extended_key)

        request = LedgerIssueGGORequest(
            origin=measurement_address,
            destination=ggo_address,
            tech_type=self.tech_type,
            fuel_type=self.fuel_type,
            key=self.extended_key.PublicKey().hex())

        byte_obj = self._to_bytes(issue_ggo_schema, request)

        return [self.sign_transaction(
            batch_signer,
            batch_signer,
            byte_obj,
            inputs=[measurement_address, ggo_address],
            outputs=[ggo_address],
            family_name=LedgerIssueGGORequest.__name__,
            family_version='0.1')]
