import os
import hashlib
import traceback
import logging

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError
from marshmallow_dataclass import class_schema

from .generic_handler import GenericHandler
from .ledger_dto import Measurement, PublishMeasurementRequest


class PublishMeasurementTransactionHandler(GenericHandler):

    @property
    def family_name(self):
        return PublishMeasurementRequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        measurement_namespace = hashlib.sha512('MEASUREMENT'.encode('utf-8')).hexdigest()[0:6]
        return [measurement_namespace]


    def apply(self, transaction, context):

        try:
            self.validate_transaction(transaction)

            address = transaction.header.outputs[0]

            if self._addresses_not_empty(context, [address]):
                raise InvalidTransaction(f'Address already in use "{address}"!')

            request = self._map_request(PublishMeasurementRequest, transaction.payload)
            measurement = Measurement(
                    amount=request.amount,
                    type=request.type,
                    begin=request.begin,
                    end=request.end,
                    sector=request.sector
                )

            payload = Measurement.get_schema().dumps(measurement).encode('utf8')

            context.set_state(
                {address: payload}, 
                self.TIMEOUT)

            logging.info(f'PublishMeasurement - address={ address }')
            
        except InvalidTransaction as ex:
            logging.exception('InvalidException')
            raise
            
        except Exception as ex:
            logging.exception('Exception')
            raise InternalError('An unknown error has occured.')

    def validate_transaction(self, transaction):
        # TODO: validate signer is energinet!!!
        # raise InvalidTransaction('Not valid Guarantee of origin issuer!')
        pass
