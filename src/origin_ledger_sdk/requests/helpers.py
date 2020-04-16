
from enum import Enum
from hashlib import sha512
from bip32utils import BIP32Key
from sawtooth_signing import create_context
from sawtooth_signing import CryptoFactory, Signer
from sawtooth_signing.secp256k1 import Secp256k1PrivateKey as PrivateKey


def get_signer(signer_key: BIP32Key) -> Signer:
    context = create_context('secp256k1')
    private_key = PrivateKey.from_bytes(signer_key.PrivateKey())
    return CryptoFactory(context).new_signer(private_key)   


class AddressPrefix(Enum):
    GGO = 'GGO'
    MEASUREMENT = 'MEASUREMENT'


def generate_address(prefix: AddressPrefix, extended_key: BIP32Key) -> str:
    key_add = extended_key.Address()
    prefix_add = sha512(prefix.value.encode('utf-8')).hexdigest()[:6]
    return prefix_add + sha512(key_add.encode('utf-8')).hexdigest()[:64]
