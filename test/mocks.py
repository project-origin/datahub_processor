

from typing import List, Dict
from dataclasses import dataclass, field


@dataclass
class Entry:
    address: str = field()
    data: bytes = field()

@dataclass
class MockContext:
    states: Dict[str, bytes] = field()

    def set_state(self, new_states, timeout):
        
        for key in new_states:
            self.states[key] = new_states[key]

    def get_state(self, addresses):

        result = []

        for add in addresses:
            if add in self.states:

                result.append(Entry(
                    address=add,
                    data=self.states[add]
                ))

        return result

@dataclass
class FakeTransactionHeader:
    batcher_public_key: str = field()
    dependencies: List[str] = field()
    family_name: str = field()
    family_version: str = field()
    inputs: List[str] = field()
    outputs: List[str] = field()
    payload_sha512: str = field()
    signer_public_key: str = field()
    

@dataclass
class FakeTransaction:
    header: FakeTransactionHeader = field()
    header_signature: str = field()
    payload: bytes = field()
