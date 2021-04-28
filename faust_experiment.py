from random import random
from datetime import timedelta
import faust

app = faust.App('windowing', broker='kafka://localhost:29092', num_partitions=1)


class Model(faust.Record, serializer='json'):
    random: float


TOPIC = 'hopping_topic_6'

hopping_topic = app.topic(TOPIC, value_type=Model)
hopping_table = app.Table(
    'hopping_table_6',
    partitions=1,
    default=int).hopping(5, 1, expires=timedelta(minutes=10), key_index=True)


@app.agent(hopping_topic)
async def print_windowed_events(stream):
    async for _ in stream: # noqa
        hopping_table['counter'] += 1
        value = hopping_table['counter']
        print('-- New Event (every 2 secs) written to hopping(10, 5) --')
        print(f'now() should have values between 1-3: {value.now()}')
        print(f'current() should have values between 1-3: {value.current()}')
        print(f'value() should have values between 1-3: {value.value()}')
        print(f'delta(30) should start at 0 and after 40 secs be 5: '
              f'{value.delta(30)}')


@app.timer(1.0, on_leader=True)
async def publish_every_2secs():
    msg = Model(random=round(random(), 2))
    await hopping_topic.send(value=msg)


if __name__ == '__main__':
    app.main()