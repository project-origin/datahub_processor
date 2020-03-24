import os
import traceback
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from json import JSONDecodeError
from marshmallow_dataclass import class_schema
from marshmallow import ValidationError

from .generic_handler import GenericHandler
from .ledger_dto import Measurement, LedgerPublishMeasurementRequest

measurement_schema = class_schema(Measurement)

class PublishMeasurementTransactionHandler(GenericHandler):

    TIMEOUT = 3

    def __init__(self):
        self._namespace_prefix = os.getenv('MEASUREMENTLEDGER_ADDRESS_PREFIX')

    @property
    def family_name(self):
        return LedgerPublishMeasurementRequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        return [self._namespace_prefix]

    def apply(self, transaction, context):

        try:
            self._validate_publickey(transaction.header.signer_public_key)

            address = transaction.header.outputs[0]

            if self._address_not_empty(context, address):
                raise InvalidTransaction(f'Address already in use "{address}"!')

            request = self._map_request(LedgerPublishMeasurementRequest, transaction.payload)
            measurement = self._map_measurement(request)

            payload = measurement_schema(exclude=["address"]).dumps(measurement).encode('utf8')

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