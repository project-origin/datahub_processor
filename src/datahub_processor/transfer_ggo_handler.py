import os
import traceback
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, LedgerTransferGGORequest, GGONext, GGOAction


class TransferGGOTransactionHandler(GenericHandler):

    TIMEOUT = 3

    def __init__(self):
        self._namespace_prefix = os.getenv('GGOLEDGER_ADDRESS_PREFIX')

    @property
    def family_name(self):
        return LedgerTransferGGORequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        return [self._namespace_prefix]

    def apply(self, transaction, context):

        try:
            request: LedgerTransferGGORequest = self._map_request(LedgerTransferGGORequest, transaction.payload)

            current_ggo = self._get_ggo(context, request.origin)

            if current_ggo.next != None:
                raise InvalidTransaction('GGO already has been used')

            if current_ggo.key != transaction.header.signer_public_key:
                raise InvalidTransaction('Unauthorized transfer on GGO')

            if self._addresses_not_empty(context, [request.destination]):
                raise InvalidTransaction('Destination address not empty')

            current_ggo.next = GGONext(
                action=GGOAction.TRANSFER,
                addresses=[request.destination]
            )
                
            new_ggo = GGO(
                origin=request.origin,
                amount=current_ggo.amount,
                begin=current_ggo.begin,
                end=current_ggo.end,
                sector=current_ggo.sector,
                tech_type=current_ggo.tech_type,
                fuel_type=current_ggo.fuel_type,
                key=request.key
            )

            payload_current = GGO.get_schema().dumps(current_ggo).encode('utf8')
            payload_new = GGO.get_schema().dumps(new_ggo).encode('utf8')

            context.set_state(
                {
                    request.origin: payload_current,
                    request.destination: payload_new
                }, 
                self.TIMEOUT)
            
        except InvalidTransaction as ex:
            print(ex)
            raise
            
        except Exception as ex:
            track = traceback.format_exc()
            print(track)

            raise InternalError('an error while parsing the transaction happened')

      