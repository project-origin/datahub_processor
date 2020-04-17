import os
import hashlib
import traceback
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, TransferGGORequest, GGONext, GGOAction


class TransferGGOTransactionHandler(GenericHandler):

    @property
    def family_name(self):
        return TransferGGORequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        ggo_namespace = hashlib.sha512('GGO'.encode('utf-8')).hexdigest()[0:6]
        return [ggo_namespace]


    def apply(self, transaction, context):

        try:
            request: TransferGGORequest = self._map_request(TransferGGORequest, transaction.payload)

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
            track = traceback.format_exc()
            print("InvalidException", ex)
            print("InvalidTrack", track)
            raise
            
        except Exception as ex:
            track = traceback.format_exc()
            print("Exception", ex)
            print("Track", track)
            raise InternalError('An unknown error has occured.')
