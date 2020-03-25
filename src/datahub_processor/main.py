import os
from sawtooth_sdk.processor.core import TransactionProcessor
from publish_measurement_handler import PublishMeasurementTransactionHandler 
from issue_ggo_transaction_handler import IssueGGOTransactionHandler 


def main():
    processor = TransactionProcessor(url=os.getenv('LEDGER_URL'))
    processor.add_handler(PublishMeasurementTransactionHandler())
    processor.add_handler(IssueGGOTransactionHandler())

    
    processor.start()
    

if __name__ == "__main__":
    main()