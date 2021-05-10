import os
import sys
import pandas as pd
from asyncio import gather, get_event_loop

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt.async_support as ccxt  # noqa: E402



async def symbol_loop(exchange, symbol):
    print('Starting the', exchange.id, 'symbol loop with', symbol)
    master_data = []
    start_time = exchange.milliseconds()
    end_time = start_time + 60000
    while True:
        try:
            # --------------------> DO YOUR LOGIC HERE <------------------
            orderbook = await exchange.fetch_order_book(symbol)
            now = exchange.milliseconds()
            print(now)
            print(exchange.iso8601(now), exchange.id, symbol, orderbook['asks'][0], orderbook['bids'][0])
            print(exchange.iso8601(now), exchange.id, symbol, len(orderbook['asks']), len(orderbook['bids']))

            master_data.append({'date': exchange.iso8601(now),
                                'exchange_id': exchange.id,
                                'symbol': symbol,
                                'asks': orderbook['asks'][0:40],
                                'bids': orderbook['bids'][0:40]})
            if now > end_time:
                # save to pickle with good name
                print(exchange.id, len(master_data))
                df = pd.DataFrame(master_data)
                df.to_pickle(exchange.id + "_" + symbol.replace('/', '-') + "_" + str(start_time) + "_" + str(end_time) + "_lob.pkl")
                break

        except Exception as e:
            print(str(e))
            # raise e  # uncomment to break all loops in case of an error in any one of them
            break  # you can break just this one loop if it fails


async def exchange_loop(asyncio_loop, exchange_id, symbols):
    print('Starting the', exchange_id, 'exchange loop with', symbols)
    exchange = getattr(ccxt, exchange_id)({
        'enableRateLimit': True,
        'asyncio_loop': asyncio_loop,
    })
    loops = [symbol_loop(exchange, symbol) for symbol in symbols]
    await gather(*loops)
    await exchange.close()


async def main(asyncio_loop):
    exchanges = {
        'kraken': ['DOGE/USDT'],
        'kucoin': ['DOGE/USDT'],
        'binanceus': ['DOGE/USDT']
    }
    loops = [exchange_loop(asyncio_loop, exchange_id, symbols) for exchange_id, symbols in exchanges.items()]
    await gather(*loops)


if __name__ == '__main__':
    asyncio_loop = get_event_loop()
    asyncio_loop.run_until_complete(main(asyncio_loop))