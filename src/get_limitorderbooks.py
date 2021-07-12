import os
import sys
import pandas as pd
from asyncio import gather, get_event_loop
import datetime

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt.async_support as ccxt  # noqa: E402


async def symbol_loop(exchange, symbol):
    master_data = []
    start_time = exchange.milliseconds()
    end_time = start_time + 604800000  # 43200000
    loop_counter = 0

    str_start_time = str(datetime.datetime.fromtimestamp(start_time / 1000).strftime('%Y-%m-%d %H:%M:%S.%f'))
    str_end_time = str(datetime.datetime.fromtimestamp(end_time / 1000).strftime('%Y-%m-%d %H:%M:%S.%f'))

    # replace : and . with -
    str_start_time = str_start_time.replace(":", "-")
    str_start_time = str_start_time.replace(".", "-")

    str_end_time = str_end_time.replace(":", "-")
    str_end_time = str_end_time.replace(".", "-")

    print('Starting the', exchange.id, 'symbol loop with', symbol)
    print(f"Start time: {str_start_time}\nEnd Time: {str_end_time}\n")
    # print the start and end time

    end_loops = False

    while not end_loops:
        while True:
            loop_counter += 1
            try:
                # --------------------> DO YOUR LOGIC HERE <------------------
                if exchange.id == "kucoin":
                    orderbook = await exchange.fetch_order_book(symbol, 100)
                else:
                    orderbook = await exchange.fetch_order_book(symbol)
                now = exchange.milliseconds()
                # print(now)
                # print(exchange.iso8601(now), exchange.id, symbol, orderbook['asks'][0], orderbook['bids'][0])
                # print(exchange.iso8601(now), exchange.id, symbol, len(orderbook['asks']), len(orderbook['bids']))

                master_data.append({'date': exchange.iso8601(now),
                                    'exchange_id': exchange.id,
                                    'symbol': symbol,
                                    'asks': orderbook['asks'],  # [0:40],
                                    'bids': orderbook['bids']})  # [0:40]})
                # save the file if now is later than the end time or if its been
                if now > end_time or loop_counter % 1000 == 0:
                    # save to pickle with good name
                    print(exchange.id, len(master_data))

                    df = pd.DataFrame(master_data)

                    data_path = os.path.join(os.getcwd(), "data/limit_orderbook_data/"
                                             + str_start_time + "_" + str_end_time)
                    if not os.path.exists(data_path):
                        os.mkdir(data_path)
                    df.to_pickle(data_path + "/" + exchange.id + "_"
                                 + symbol.replace('/', '-') + "_"
                                 + str_start_time + "_" + str_end_time + "_" + str(loop_counter) + "_lob.pkl")
                    print(f"saved {exchange.id} {symbol} ",
                          f"{str(datetime.datetime.fromtimestamp(now / 1000).strftime('%Y-%m-%d %H:%M:%S.%f'))}")

                    master_data = []

                    if now > end_time:
                        end_loops = True
                        break

            except ccxt.DDoSProtection as e:
                print(str(e), 'DDoS Protection (ignoring)')
                print(exchange.milliseconds())
            except ccxt.RequestTimeout as e:
                print(str(e), 'Request Timeout (ignoring)')
                print(exchange.milliseconds())
            except ccxt.ExchangeNotAvailable as e:
                print(str(e), 'Exchange Not Available due to downtime or maintenance (ignoring)')
                print(exchange.milliseconds())
            except ccxt.AuthenticationError as e:
                print(str(e), 'Authentication Error (missing API keys, ignoring)')
                print(exchange.milliseconds())
            except ccxt.BaseError as e:
                print(str(e), e.args, 'Base Error')
                print(exchange.milliseconds())
            except Exception as e:
                print(str(e), "Ignoring")
                print(exchange.milliseconds())
                # raise e  # uncomment to break all loops in case of an error in any one of them
                # break  # you can break just this one loop if it fails


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
        'kraken': ['ADA/ETH', 'ADA/USD', 'ADA/USDT', 'ETH/USDC', 'ETH/USDT', 'TBTC/ETH']
        #'kucoin': ['ETH/USDC', 'ETH/TUSD', 'ETH/USDT', 'ETH/BTC', 'ADA/BTC', 'ADA/USDT', 'WBTC/ETH', 'ADA/USDC'],
        #'binanceus': ['ETH/USD', 'ETH/USDT', 'ETH/BTC', 'ADA/USD', 'ADA/USDT', 'ADA/BTC'],
        #'coinbasepro': ['ETH/USDC', 'ETH/USDT', 'ETH/USD', 'ADA/ETH', 'ADA/USD', 'ETH/BTC', 'ADA/USDC', 'ADA/BTC']
    }
    loops = [exchange_loop(asyncio_loop, exchange_id, symbols) for exchange_id, symbols in exchanges.items()]
    await gather(*loops)


if __name__ == '__main__':
    asyncio_loop = get_event_loop()
    asyncio_loop.run_until_complete(main(asyncio_loop))
