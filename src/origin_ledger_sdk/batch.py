
from enum import Enum
from typing import List

from bip32utils import BIP32Key
from sawtooth_sdk.protobuf.batch_pb2 import BatchHeader
from sawtooth_sdk.protobuf.batch_pb2 import Batch as SignedBatch

from .requests import AbstractRequest
from .requests.helpers import get_signer


class BatchStatus(Enum):
    UNKNOWN = 'UNKNOWN'
    PENDING = 'PENDING'
    COMMITTED = 'COMMITTED'
    INVALID = 'INVALID'

class Batch():


    def __init__(self, 
        signer_key: BIP32Key):

        self._requests: List[AbstractRequest] = []
        self._signer = get_signer(signer_key)

    def add_request(self, request: AbstractRequest):
        self._requests.append(request)

    def get_signed_batch(self) -> SignedBatch:

        signed_transactions = [t for r in self._requests for tl in r.get_signed_transactions(self._signer) for t in tl ]

        batch_header_bytes = BatchHeader(
            signer_public_key=self._signer.get_public_key().as_hex(),
            transaction_ids=[txn.header_signature for txn in signed_transactions],
        ).SerializeToString()
        
        signature = self._signer.sign(batch_header_bytes)
        batch = SignedBatch(
            header=batch_header_bytes,
            header_signature=signature,
            transactions=signed_transactions
        )

        return batch

    