import os
import hashlib
import traceback
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, GGONext, GGOAction, Settlement, SettlementPart, MeasurementType
from .ledger_dto import RetireGGORequest, RetireGGOPart, SignedRetireGGOPart


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
        settlement_namespace = hashlib.sha512('SETTLEMENT'.encode('utf-8')).hexdigest()[0:6]
        return [ggo_namespace, settlement_namespace]


    def apply(self, transaction, context):

        try:
            request: RetireGGORequest = self._map_request(RetireGGORequest, transaction.payload)

            measurement = self._get_measurement(context, request.measurement_address)
            settlement: Settlement = self._try_get_type(Settlement, context, request.settlement_address)

            if settlement != None:
                if settlement.measurement != request.measurement_address:
                    raise InvalidTransaction('Measurement does not equal settlement measurement')

                if settlement.key != transaction.header.signer_public_key:
                    raise InvalidTransaction('Unauthorized retire to settlement')

            else:
                if request.measurement_address[6:] != request.settlement_address[6:]:
                    raise InvalidTransaction('Not correct settlement address for measurement')
                
                if measurement.type != MeasurementType.CONSUMPTION:
                    raise InvalidTransaction('Measurment is not of type consumption')

                if measurement.key != transaction.header.signer_public_key:
                    raise InvalidTransaction('Unauthorized retire to measurement')

                settlement = Settlement(
                    measurement=request.measurement_address,
                    key=request.key,
                    parts=[]
                )

            settlement.key = request.key
            state_update = {}
            
            for part in request.parts:
                if part.content.settlement_address != request.settlement_address:
                    raise InvalidTransaction('Invalid destination, not the same as measurement')

                ggo = self._get_ggo(context, part.content.origin)

                if ggo.next != None:
                    raise InvalidTransaction('GGO already has been used')

                if not self._validate_signature(part.signature, part.content, ggo.key):
                    raise InvalidTransaction('Unauthorized retire on GGO')

                # TODO implement time and sector rules

                ggo.next = GGONext(
                    action=GGOAction.RETIRE,
                    addresses=[request.settlement_address]
                )

                settlement.parts.append(SettlementPart(
                        ggo=part.content.origin,
                        amount=ggo.amount
                    ))

                state_update[part.content.origin] = GGO.get_schema().dumps(ggo).encode('utf8')
                
            state_update[request.settlement_address] = Settlement.get_schema().dumps(settlement).encode('utf8')

            if sum([p.amount for p in settlement.parts]) > measurement.amount:
                raise InvalidTransaction('Invalid to retire more that measurement amount')

            context.set_state(
                state_update, 
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
