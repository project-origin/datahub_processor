import requests
import base64
import marshmallow_dataclass

from bip32utils import BIP32Key
# from enum import Enum
from typing import List #, Optional
from dataclasses import dataclass, field
from sawtooth_sdk.protobuf.batch_pb2 import BatchList
from marshmallow_dataclass import class_schema

from .batch import Batch, BatchStatus
from ..ledger_dto import Measurement, GGO
from .requests import generate_address, AddressPrefix




@dataclass
class Handle():
    link: str = field()

@dataclass
class Paging():
    limit: int = field()
    start: int = field()

@dataclass
class BatchStatusResponseData():
    id: str = field()
    invalid_transactions: List[str] = field()
    status: BatchStatus = field()

@dataclass
class BatchStatusResponse():
    data: List[BatchStatusResponseData] = field()
    link: str = field()

@dataclass
class StateResponse():
    data: str = field()
    head: str = field()
    link: str = field()



handle_schema = marshmallow_dataclass.class_schema(Handle)
batch_status_schema = marshmallow_dataclass.class_schema(BatchStatusResponse)
state_response_schema = marshmallow_dataclass.class_schema(StateResponse)
measurement_schema = class_schema(Measurement)
ggo_schema = class_schema(GGO)


class Ledger():

    def __init__(self, url):
        self.url = url

    def execute_batch(self, batch : Batch) -> Handle:
        signed_batch = batch.get_signed_batch()
        return self._send_batches([signed_batch])


    def get_batch_status(self, handle: Handle) -> BatchStatusResponse:
        response = requests.get(handle.link)
        return batch_status_schema().loads(response.content)


    def _send_batches(self, signed_batches):
        batch_list_bytes = BatchList(batches=signed_batches).SerializeToString()

        response = requests.post(
            f'{self.url}/batches',
            batch_list_bytes,
            headers={'Content-Type': 'application/octet-stream'})

        return handle_schema().loads(response.content)


    def _get_state(self, address) -> StateResponse:
        response = requests.get(
            f'{self.url}/state/{address}'
        )

        return state_response_schema().loads(response.content)


    def get_measurement_from_key(self, key: BIP32Key) -> Measurement:
        address = generate_address(AddressPrefix.MEASUREMENT, key)
        return self.get_measurement_from_address(address)

    def get_measurement_from_address(self, address: str) -> Measurement:
        response = self._get_state(address)
        
        body = base64.b64decode(response.data)

        measurement = measurement_schema().loads(body)
        measurement.address = address

        return measurement

    def get_ggo_from_key(self, key: BIP32Key) -> GGO:
        address = generate_address(AddressPrefix.GGO, key)
        return self.get_ggo_from_address(address)

    def get_ggo_from_address(self, address: str) -> GGO:
        response = self._get_state(address)
        
        body = base64.b64decode(response.data)

        ggo = ggo_schema().loads(body)
        ggo.address = address

        return ggo
