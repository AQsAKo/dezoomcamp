import faust
from taxi_rides import TaxiRide


app = faust.App('dtc.stream.v2', broker='kafka://localhost:9092')
topic = app.topic('dtc.ny_taxi.json', value_type=TaxiRide)

pu_rides = app.Table('pu_rides', default=int)


@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.PULocationID):
        pu_rides[event.PULocationID] += 1

if __name__ == '__main__':
    app.main()
