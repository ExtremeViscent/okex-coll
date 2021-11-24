import asyncio
from numpy.lib.function_base import delete
import websocket
import pathlib
import json
import ssl
import threading
import numpy as np
from sys import getsizeof
import os
from time import time

# async def hello():
#     uri = "wss://ws.okex.com:8443/ws/v5/public"

#     async with websockets.connect(uri, ssl=True) as websocket:
#         request_book = '{"op": "subscribe","args": [{"channel": "books","instId": "ETH-USDT"}]}'
#         await websocket.send(request_book)
#         ret = await websocket.recv()
#         print(ret)
def save_data(data):
    ts = time()
    data_root = '/mnt/data/okex/ETH-USDT'
    for i,x in enumerate(data[0]):
        np.save(os.path.join(data_root,f'{ts}_{i}.npy'), x)
        del x

if __name__ == "__main__":
    uri = "wss://ws.okex.com:8443/ws/v5/public"
    ws = websocket.create_connection(uri)
    request_book = '{"op": "subscribe","args": [{"channel": "books","instId": "ETH-USDT"}]}'
    ws.send(request_book)
    rate = 0
    t0 = time()
    _t0 = t0
    ts0 = int(round(t0 * 1000))
    sizesum = 0
    data_buffer = [
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
        ]

    while True:
        ret = ws.recv()
        if ret!=None:
            print(ret)
            break
    while True:
        ret = ws.recv()
        if ret!=None:
            break
    while True:
        if time()-t0>=3600:
            temp_buffer = [np.concatenate(x).copy() for x in data_buffer]
            sizesum += sum([getsizeof(x) for x in temp_buffer])
            print(f'At {time()-_t0}s, total size {sizesum/1024/1024}MB, data rate {sizesum/1024/1024/(time()-_t0)*3600} MB/h')
            del data_buffer
            threading.Thread(target=save_data, args=([temp_buffer],)).start()
            data_buffer = [
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                [],
                ]
            t0 = time()
        ret = ws.recv()
        if ret!=None:
            try:
                # rate+=1
                asks = json.loads(ret)['data'][0]['asks']
                bids = json.loads(ret)['data'][0]['bids']
                delta = np.array([int(json.loads(ret)['data'][0]['ts'])-ts0], dtype=np.uint16)
                batch_size_ask = np.array([len(asks)], dtype=np.uint16)
                batch_size_bid = np.array([len(bids)], dtype=np.uint16)
                ts0 = int(json.loads(ret)['data'][0]['ts'])
                if asks != []:
                    asks = np.array(asks, dtype=np.float32)
                    ask_price = np.array(asks[:,0], dtype=np.float32)
                    ask_contracts = np.array(asks[:,1], dtype=np.float32)
                    ask_force = np.array(asks[:,2], dtype=np.uint8)
                    ask_vol = np.array(asks[:,3], dtype=np.uint8)
                    data_buffer[0].append(ask_price)
                    data_buffer[1].append(ask_contracts)
                    data_buffer[2].append(ask_force)
                    data_buffer[3].append(ask_vol)
                    data_buffer[4].append(batch_size_ask)
                if bids != []:
                    bids = np.array(bids, dtype=np.float32)
                    bid_price = np.array(bids[:,0], dtype=np.float32)
                    bid_contracts = np.array(bids[:,1], dtype=np.float32)
                    bid_force = np.array(bids[:,2], dtype=np.uint8)
                    bid_vol = np.array(bids[:,3], dtype=np.uint8)
                    data_buffer[5].append(bid_price)
                    data_buffer[6].append(bid_contracts)
                    data_buffer[7].append(bid_force)
                    data_buffer[8].append(bid_vol)
                    data_buffer[9].append(batch_size_bid)
                data_buffer[10].append(delta)
                # print('ask_price:', ask_price)
                # print('bid_price:', bid_price)
                # print('ask_contracts:', ask_contracts)
                # print('bid_contracts:', bid_contracts)
                # print('ask_force:', ask_force)
                # print('bid_force:', bid_force)
                # print('ask_vol:', ask_vol)
                # print('bid_vol:', bid_vol)
            except Exception as e:
                print(e)
                pass
            
        # t1 = time()
        # if t1 - t0 > 1:
        #     print(rate)
        #     rate = 0
        #     t0 = t1
