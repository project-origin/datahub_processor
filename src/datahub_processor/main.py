import os
from sawtooth_sdk.processor.core import TransactionProcessor
from handler import PublishMeasurementTransactionHandler 


def main():
    processor = TransactionProcessor(url=os.getenv('LEDGER_URL'))
    handler = PublishMeasurementTransactionHandler()
    processor.add_handler(handler)
    processor.start()
    

if __name__ == "__main__":
    main()