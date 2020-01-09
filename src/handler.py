import os
import traceback
from sawtooth_sdk.processor.handler import TransactionHandler
from sawtooth_sdk.processor.exceptions import InvalidTransaction, InternalError

class DHTransactionHandler(TransactionHandler):

    TIMEOUT = 3

    def __init__(self):
        self._namespace_prefix = os.getenv('LEDGER_ADDRESS_PREFIX')

    @property
    def family_name(self):
        return os.getenv('LEDGER_FAMILY_NAME')

    @property
    def family_versions(self):
        return [os.getenv('LEDGER_FAMILY_VERSION')]

    @property
    def namespaces(self):
        return [self._namespace_prefix]

    def apply(self, transaction, context):

        try:
            self._validate_publickey(transaction.header.signer_public_key)

            address = transaction.header.outputs[0]
            states = context.get_state([address])

            if len(states) != 0:
                if states[0] == transaction.payload:
                    raise InvalidTransaction(f'Address already in use "{address}" data is the same' )
                else:
                    raise InvalidTransaction(f'Address already in use "{address}" current_data: "{states[0]}"  new_data: "{transaction.payload}"' )

            else:
                #empty address
                context.set_state(
                    {address: transaction.payload},
                    self.TIMEOUT)
                
        except InvalidTransaction as ex:
            print(ex)
            raise
            
        except Exception as ex:
            track = traceback.format_exc()
            print(track)

            raise InternalError('an error while parsing the transaction happened')


    def _validate_publickey(self, publickey):
        # TODO: validate signer is energinet!!!
        # raise InvalidTransaction('Not valid Guarantee of origin issuer!')
        pass