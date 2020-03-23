import os
import traceback
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from json import JSONDecodeError
from marshmallow_dataclass import class_schema
from marshmallow import ValidationError

from .ledger_dto import Measurement, LedgerPublishMeasurementRequest

request_schema = class_schema(LedgerPublishMeasurementRequest)
measurement_schema = class_schema(Measurement)

class PublishMeasurementTransactionHandler(TransactionHandler):

    TIMEOUT = 3

    def __init__(self):
        self._namespace_prefix = os.getenv('MEASUREMENTLEDGER_ADDRESS_PREFIX')

    @property
    def family_name(self):
        return 'MEASUREMENT'

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        return [self._namespace_prefix]

    def apply(self, transaction, context):

        try:
            self._validate_publickey(transaction.header.signer_public_key)
            self._check_address_available(transaction, context)

            request = self._validate_payload(transaction.payload)
            measurement = self._map_measurement(request)

            payload = measurement_schema(exclude=["address"]).dumps(measurement).encode('utf8')
            address = transaction.header.outputs[0]

            context.set_state(
                {address: payload}, 
                self.TIMEOUT)
            
        except InvalidTransaction as ex:
            print(ex)
            raise
            
        except Exception as ex:
            track = traceback.format_exc()
            print(track)

            raise InternalError('an error while parsing the transaction happened')

    def _check_address_available(self, transaction, context):
        address = transaction.header.outputs[0]
        states = context.get_state([address])

        if len(states) != 0:
            raise InvalidTransaction(f'Address already in use "{address}"!')

    def _validate_payload(self, payload: bytes):
        try:
            return request_schema().loads(payload.decode('utf8'))

        except ValidationError as err:
            raise InvalidTransaction(str(err))

        except JSONDecodeError as err:
            raise InvalidTransaction('The transaction payload was an invalid request. Invalid JSON.')


    def _map_measurement(self, request: LedgerPublishMeasurementRequest):
        return Measurement(
            amount=request.amount,
            type=request.type,
            begin=request.begin,
            end=request.end,
            sector=request.sector,
            key=request.key
        )

    def _validate_publickey(self, publickey):
        # TODO: validate signer is energinet!!!
        # raise InvalidTransaction('Not valid Guarantee of origin issuer!')
        pass