# DataHub Processor

This is the transaction processor to accept DataHub changes on the Sawtooth ledger.

It should validate the structure of the incoming requests, that they conform with the message design.

It should also validate the signature, that it is done by a trusted party.

## Issues
Currently it will write anything to the ledger, no checks has been implemented!

# Environment Variables
The code requires 4 environment variables
- LEDGER_URL
- LEDGER_ADDRESS_PREFIX
- LEDGER_FAMILY_NAME
- LEDGER_FAMILY_VERSION

## LEDGER_URL
The url where the transaction processor can contact the Validator node
```
LEDGER_URL=tcp://validator:4004
```

## LEDGER_ADDRESS_PREFIX
The prefix for the addresses on the Sawtooth ledger, first 6 bytes of address.
```
LEDGER_ADDRESS_PREFIX=datahub
```

## LEDGER_FAMILY_NAME
The family name for the transactions, <a href='https://sawtooth.hyperledger.org/docs/core/releases/latest/transaction_family_specifications.html'>Sawtooth wiki</a>
```
LEDGER_FAMILY_NAME=datahub
```

## LEDGER_FAMILY_VERSION
The family version for the transactions, <a href='https://sawtooth.hyperledger.org/docs/core/releases/latest/transaction_family_specifications.html'>Sawtooth wiki</a>
```
LEDGER_FAMILY_VERSION=0.1
```



### NOTES...

address for certificate is calculated based on grsn and production time.



## Running on ubuntu 18
'''console
sudo apt-get install pkg-config
sudo apt-get install libsecp256k1-dev
'''








pipenv run pytest --cov-report=term-missing --cov-fail-under=90 --cov=src/origin_handlers test