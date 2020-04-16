from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List
from marshmallow import validate, validates_schema, ValidationError

from .enums import MeasurementType

@dataclass
class PublishMeasurementRequest:
    begin: datetime = field()   
    end: datetime = field()
    sector: str = field(metadata={"validate": validate.OneOf(['DK1', 'DK2'])})
    type: MeasurementType = field()
    amount: int = field(metadata={"validate": validate.Range(min=0)})
    key: str = field()

    @validates_schema
    def validate_dates(self, data, **kwargs):
        
        if  data['begin'] >= data['end']:
            raise ValidationError('Begin must be before End!')

        if (data['end'] - data['begin']) != timedelta(hours=1):
            raise ValidationError('Only positive hourly measurements are currently supported!')



@dataclass
class IssueGGORequest:
    origin: str = field()
    destination: str = field()
    tech_type: str = field()
    fuel_type: str = field()
    key: str = field()


@dataclass
class TransferGGORequest:
    origin: str = field()
    destination: str = field()
    key: str = field()
    
@dataclass
class SplitGGOPart:
    address: str = field()
    amount: int = field()
    key: str = field()

@dataclass
class SplitGGORequest:
    origin: str = field()
    parts: List[SplitGGOPart] = field()

    
@dataclass
class RetireGGOPart:
    origin: str = field()
    settlement_address: str = field()

    def __str__(self):
        return f'{self.origin}|{self.settlement_address}'

@dataclass
class SignedRetireGGOPart:
    content: RetireGGOPart = field()
    signature: str = field()

@dataclass
class RetireGGORequest:
    measurement_address: str = field()
    settlement_address: str = field()
    key: str = field()
    parts: List[SignedRetireGGOPart] = field()
