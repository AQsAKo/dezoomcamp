import faust


class TaxiRide(faust.Record, validation=True):
    PUlocationID: str
