import os
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
    url = os.getenv('LEDGER_URL')
    main(url)