import os
import hashlib
import traceback
import logging

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, IssueGGORequest, MeasurementType


class IssueGGOTransactionHandler(GenericHandler):

    @property
    def family_name(self):
        return IssueGGORequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        ggo_namespace = hashlib.sha512('GGO'.encode('utf-8')).hexdigest()[0:6]
        return [ggo_namespace]


    def apply(self, transaction, context):

        try:
            self.validate_transaction(transaction)
            
            request: IssueGGORequest = self._map_request(IssueGGORequest, transaction.payload)
            measurement = self._get_measurement(context, request.origin)

            if self._addresses_not_empty(context, [request.destination]):
                raise InvalidTransaction("GGO already issued!")

            if measurement.type != MeasurementType.PRODUCTION:
                raise InvalidTransaction("Measurement is not of type Production!")

            new_ggo = GGO(
                origin=request.origin,
                amount=measurement.amount,
                begin=measurement.begin,
                end=measurement.end,
                sector=measurement.sector,
                tech_type=request.tech_type,
                fuel_type=request.fuel_type,
                emissions=request.emissions,
            )

            payload = GGO.get_schema().dumps(new_ggo).encode('utf8')

            context.set_state(
                {request.destination: payload}, 
                self.TIMEOUT)

            logging.info(f'IssueGGOTransactionHandler - origin={ request.origin } destination={ request.destination }')
            
        except InvalidTransaction as ex:
            logging.exception('IssueGGOTransactionHandler - InvalidException')
            raise
            
        except Exception as ex:
            logging.exception('IssueGGOTransactionHandler - Exception')
            raise InternalError('An unknown error has occured.')

    def validate_transaction(self, transaction):
        # TODO: validate signer is energinet!!!
        # raise InvalidTransaction('Not valid Guarantee of origin issuer!')
        pass
