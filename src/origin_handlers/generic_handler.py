

from sawtooth_sdk.processor.handler import TransactionHandler
from marshmallow import ValidationError
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from marshmallow_dataclass import class_schema
from json import JSONDecodeError
from ..ledger_dto import GGO, Measurement
from sawtooth_signing import create_context
from sawtooth_signing.secp256k1 import Secp256k1PublicKey as PublicKey

class GenericHandler(TransactionHandler):
 
    TIMEOUT = 3

    def _validate_signature(self, signed_message, obj, key):
        context = create_context('secp256k1')
        message = str(obj).encode('utf8')
        pubKey = PublicKey.from_hex(key)
        
        return context.verify(signed_message, message, pubKey)


    def _map_request(self, clazz: type, payload: bytes):
        try:
            data = payload.decode('utf8')
            schema = class_schema(clazz)
            return schema().loads(json_data=data)

        except ValidationError as err:
            raise InvalidTransaction(str(err))

        except JSONDecodeError as err:
            raise InvalidTransaction('The transaction payload was an invalid request. Invalid JSON.')

    
    def _addresses_not_empty(self, context, addresses):
        return len(context.get_state(addresses)) != 0

    def _try_get_type(self, clazz: type, context, address):
        try:    
            states = context.get_state([address])
            for entry in states:     
                if entry.address == address:
                    return clazz.get_schema().loads(entry.data.decode('utf8'))

        except JSONDecodeError:
            pass
        except ValidationError:
            pass

        return None

    def _get_type(self, clazz: type, context, address):
        val = self._try_get_type(clazz, context, address)
        if val:
            return val
        else:
            raise InvalidTransaction(f'Address "{address}" does not contain a valid {clazz.__name__}.')


    def _get_measurement(self, context, address) -> Measurement:
        return self._get_type(Measurement, context, address)

    def _get_ggo(self, context, address) -> GGO:
        return self._get_type(GGO, context, address)
