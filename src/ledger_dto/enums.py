from enum import Enum

class MeasurementType(Enum):
    CONSUMPTION = 'CONSUMPTION'
    PRODUCTION = 'PRODUCTION'
    

class GGOAction(Enum):
    TRANSFER = "TRANSFER"
    SPLIT = "SPLIT"
    RETIRE = "RETIRE"
