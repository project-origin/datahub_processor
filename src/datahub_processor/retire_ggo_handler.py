import os
import hashlib
import traceback
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, GGONext, GGOAction, Settlement, SettlementPart, MeasurementType, generate_address, AddressPrefix
from .ledger_dto import RetireGGORequest


class RetireGGOTransactionHandler(GenericHandler):

    @property
    def family_name(self):
        return RetireGGORequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        ggo_namespace = hashlib.sha512('GGO'.encode('utf-8')).hexdigest()[0:6]
        return [ggo_namespace]


    def apply(self, transaction, context):

        try:
            request: RetireGGORequest = self._map_request(RetireGGORequest, transaction.payload)

            current_ggo = self._get_ggo(context, request.origin)

            if current_ggo.next != None:
                raise InvalidTransaction('GGO already has been used')

            public_key_bytes = bytearray.fromhex(transaction.header.signer_public_key)
            generated_address = generate_address(AddressPrefix.GGO, public_key_bytes)
            if generated_address != request.origin:
                 raise InvalidTransaction('Invalid key for GGO')

            current_ggo.next = GGONext(
                action=GGOAction.RETIRE,
                addresses=[request.settlement_address]
            )

            payload_current = GGO.get_schema().dumps(current_ggo).encode('utf8')

            context.set_state(
                {
                    request.origin: payload_current
                }, 
                self.TIMEOUT)

            print("IssueGGOTransactionHandler", "origin", request.origin, "settlement", request.settlement_address)
            

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
