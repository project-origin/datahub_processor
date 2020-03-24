

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

    
    def _address_not_empty(self, context, address):
        states = context.get_state([address])

        for entry in states:     
            if entry.address == address:
                return True
        return False

    def _get_measurement(self, context, address) -> Measurement:
        try:    
            states = context.get_state([address])
            for entry in states:     
                if entry.address == address:
                    return Measurement.get_schema().loads(entry.data.decode('utf8'))

        except JSONDecodeError:
            pass
        except ValidationError:
            pass

        raise InvalidTransaction(f'Address "{address}" does not contain a valid measurement.')

