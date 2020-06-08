import os
import hashlib
import traceback
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

from .generic_handler import GenericHandler
from .ledger_dto import GGO, GGONext, GGOAction, Settlement, SettlementPart, MeasurementType, generate_address, AddressPrefix
from .ledger_dto import SettlementRequest


class SettlementHandler(GenericHandler):

    @property
    def family_name(self):
        return SettlementRequest.__name__

    @property
    def family_versions(self):
        return ['0.1']

    @property
    def namespaces(self):
        ggo_namespace = hashlib.sha512('GGO'.encode('utf-8')).hexdigest()[0:6]
        settlement_namespace = hashlib.sha512('SETTLEMENT'.encode('utf-8')).hexdigest()[0:6]
        measurement_namespace = hashlib.sha512('MEASUREMENT'.encode('utf-8')).hexdigest()[0:6]

        return [ggo_namespace, settlement_namespace, measurement_namespace]


    def apply(self, transaction, context):

        try:
            request: SettlementRequest = self._map_request(SettlementRequest, transaction.payload)

            measurement = self._get_measurement(context, request.measurement_address)
            settlement: Settlement = self._try_get_type(Settlement, context, request.settlement_address)

            public_key_bytes = bytearray.fromhex(transaction.header.signer_public_key)

            if settlement != None:
                if settlement.measurement != request.measurement_address:
                    raise InvalidTransaction('Measurement does not equal settlement measurement')

                generated_address = generate_address(AddressPrefix.SETTLEMENT, public_key_bytes)
                if generated_address != request.settlement_address:
                    raise InvalidTransaction('Invalid key for settlement')

            else:
                if request.measurement_address[6:-8] != request.settlement_address[6:-8]:
                    raise InvalidTransaction('Not correct settlement address for measurement')
                
                if measurement.type != MeasurementType.CONSUMPTION:
                    raise InvalidTransaction('Measurment is not of type consumption')

                generated_address = generate_address(AddressPrefix.MEASUREMENT, public_key_bytes)
                if generated_address != request.measurement_address:
                    raise InvalidTransaction('Invalid key for measurement')

                settlement = Settlement(
                    measurement=request.measurement_address,
                    parts=[]
                )

            state_update = {}
            
            for ggo_address in request.ggo_addresses:

                ggo = self._get_ggo(context, ggo_address)

                if ggo.next == None:
                    raise InvalidTransaction('Invalid retired GGO in settlement')

                if ggo.next.action != GGOAction.RETIRE:
                    raise InvalidTransaction('Invalid retired GGO in settlement')

                if len(ggo.next.addresses) != 1:
                    raise InvalidTransaction('Invalid retired GGO in settlement')

                if ggo.next.addresses[0] != request.settlement_address:
                    raise InvalidTransaction('Invalid retired GGO in settlement')

                if ggo.sector != measurement.sector:
                    raise InvalidTransaction('GGO not produced in same sector as measurement')

                if ggo.begin != measurement.begin:
                    raise InvalidTransaction('GGO not produced at the same time as measurement')

                for part in settlement.parts:
                    if part.ggo == ggo_address:
                        raise InvalidTransaction('GGO already part of settlement')

                settlement.parts.append(SettlementPart(
                        ggo=ggo_address,
                        amount=ggo.amount
                    ))
                
            if sum([p.amount for p in settlement.parts]) > measurement.amount:
                raise InvalidTransaction('Invalid to retire more that measurement amount')

            context.set_state(
                {
                    request.settlement_address:  Settlement.get_schema().dumps(settlement).encode('utf8')
                }, 
                self.TIMEOUT)

            print("SplitGGOTransactionHandler", "measurement", request.measurement_address, "settlement", request.settlement_address)
            
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
