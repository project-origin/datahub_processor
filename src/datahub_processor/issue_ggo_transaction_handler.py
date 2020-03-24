import os
import traceback
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, LedgerIssueGGORequest, MeasurementType


class IssueGGOTransactionHandler(GenericHandler):

    TIMEOUT = 3

    def __init__(self):
        self._namespace_prefix = os.getenv('GGOLEDGER_ADDRESS_PREFIX')

    @property
    def family_name(self):
        return LedgerIssueGGORequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        return [self._namespace_prefix]

    def apply(self, transaction, context):

        try:
            if len(transaction.header.outputs) != 1:
                raise InvalidTransaction("Only a single output address can be specified in an IssueGGORequest")

            address = transaction.header.outputs[0]

            if self._address_not_empty(context, address):
                raise InvalidTransaction("GGO already issued!")

            request = self._map_request(LedgerIssueGGORequest, transaction.payload)
            measurement = self._get_measurement(context, request.origin)

            if measurement.type != MeasurementType.PRODUCTION:
                raise InvalidTransaction("Measurement is not of type Production!")

            new_ggo = GGO(
                amount=measurement.amount,
                begin=measurement.begin,
                end=measurement.end,
                sector=measurement.sector,
                tech_type=request.tech_type,
                fuel_type=request.fuel_type,
                key=request.key
            )

            payload = GGO.get_schema().dumps(new_ggo).encode('utf8')

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

      