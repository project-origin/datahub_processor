import os
import traceback
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, LedgerSplitGGORequest, GGONext, GGOAction


class SplitGGOTransactionHandler(GenericHandler):

    TIMEOUT = 3

    def __init__(self):
        self._namespace_prefix = os.getenv('GGOLEDGER_ADDRESS_PREFIX')

    @property
    def family_name(self):
        return LedgerSplitGGORequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        return [self._namespace_prefix]

    def apply(self, transaction, context):

        try:
            request: LedgerSplitGGORequest = self._map_request(LedgerSplitGGORequest, transaction.payload)

            current_ggo = self._get_ggo(context, request.origin)

            if current_ggo.next != None:
                raise InvalidTransaction('GGO already has been used')

            if current_ggo.key != transaction.header.signer_public_key:
                raise InvalidTransaction('Unauthorized transfer on GGO')

            if self._addresses_not_empty(context, [p.address for p in request.parts]):
                raise InvalidTransaction('Destination address not empty')

            if sum([p.amount for p in request.parts]) != current_ggo.amount:
                raise InvalidTransaction('The sum of the parts does not equal the whole')

            state_update = {}

            for part in request.parts:
                split_ggo = GGO(
                    origin=request.origin,
                    amount=part.amount,
                    begin=current_ggo.begin,
                    end=current_ggo.end,
                    sector=current_ggo.sector,
                    tech_type=current_ggo.tech_type,
                    fuel_type=current_ggo.fuel_type,
                    key=part.key
                )
                state_update[part.address] = GGO.get_schema().dumps(split_ggo).encode('utf8')

            current_ggo.next = GGONext(
                GGOAction.SPLIT,
                [p.address for p in request.parts]
            )

            state_update[request.origin] = GGO.get_schema().dumps(current_ggo).encode('utf8')

            context.set_state(
                state_update, 
                self.TIMEOUT)
            
        except InvalidTransaction as ex:
            print(ex)
            raise
            
        except Exception as ex:
            track = traceback.format_exc()
            print(track)

            raise InternalError('an error while parsing the transaction happened')

      