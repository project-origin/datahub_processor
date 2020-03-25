

from sawtooth_sdk.processor.handler import TransactionHandler
from marshmallow import ValidationError
from sawtooth_sdk.processor.exceptions import InvalidTransaction
from marshmallow_dataclass import class_schema
from json import JSONDecodeError
from .ledger_dto import GGO, Measurement

class GenericHandler(TransactionHandler):

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

    def _get_type(self, clazz: type, context, address):
        try:    
            states = context.get_state([address])
            for entry in states:     
                if entry.address == address:
                    return clazz.get_schema().loads(entry.data.decode('utf8'))

        except JSONDecodeError:
            pass
        except ValidationError:
            pass

        raise InvalidTransaction(f'Address "{address}" does not contain a valid {clazz.__name__}.')


    def _get_measurement(self, context, address) -> Measurement:
        return self._get_type(Measurement, context, address)

    def _get_ggo(self, context, address) -> GGO:
        return self._get_type(GGO, context, address)
