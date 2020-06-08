import os
import logging
import sys
from sawtooth_sdk.processor.core import TransactionProcessor
from datahub_processor import PublishMeasurementTransactionHandler,  IssueGGOTransactionHandler, TransferGGOTransactionHandler, SplitGGOTransactionHandler, RetireGGOTransactionHandler, SettlementHandler

def main(url):
    processor = TransactionProcessor(url=url)
    processor.add_handler(PublishMeasurementTransactionHandler())
    processor.add_handler(IssueGGOTransactionHandler())
    processor.add_handler(TransferGGOTransactionHandler())
    processor.add_handler(SplitGGOTransactionHandler())
    processor.add_handler(RetireGGOTransactionHandler())
    processor.add_handler(SettlementHandler())
    processor.start()
    
if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG

    url = os.getenv('LEDGER_URL', default=None)

    if url is None:
        host = os.getenv('HOSTNAME', default='localhost')
        url = f'tcp://{host}:4004'

    print(f'Connecting to "{url}"')

    main(url)