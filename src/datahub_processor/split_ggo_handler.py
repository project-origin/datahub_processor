import os
import hashlib
import traceback
import logging

from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, SplitGGORequest, GGONext, GGOAction, generate_address, AddressPrefix


class SplitGGOTransactionHandler(GenericHandler):

    @property
    def family_name(self):
        return SplitGGORequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        ggo_namespace = hashlib.sha512('GGO'.encode('utf-8')).hexdigest()[0:6]
        return [ggo_namespace]


    def apply(self, transaction, context):

        try:
            request: SplitGGORequest = self._map_request(SplitGGORequest, transaction.payload)

            current_ggo = self._get_ggo(context, request.origin)

            if current_ggo.next != None:
                raise InvalidTransaction('GGO already has been used')

            public_key_bytes = bytearray.fromhex(transaction.header.signer_public_key)
            generated_address = generate_address(AddressPrefix.GGO, public_key_bytes)
            if generated_address != request.origin:
                 raise InvalidTransaction('Invalid key for GGO')

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
                    emissions=current_ggo.emissions,
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

            logging.info(f'Split GGO - origin={ request.origin } parts=[{ ";".join([p.address for p in request.parts]) }]')
            
        except InvalidTransaction as ex:
            logging.exception('InvalidException')
            raise
            
        except Exception as ex:
            logging.exception('Exception')
            raise InternalError('An unknown error has occured.')
