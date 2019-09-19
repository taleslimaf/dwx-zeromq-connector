# -*- coding: utf-8 -*-

"""
    @author: Darwinex Labs (www.darwinex.com)
    
    Copyright (c) 2017-2019, Darwinex. All rights reserved.
    
    Licensed under the BSD 3-Clause License, you may not use this file except 
    in compliance with the License. 
    
    You may obtain a copy of the License at:    
    https://opensource.org/licenses/BSD-3-Clause
"""

import zmq
from time import sleep
from pandas import DataFrame, Timestamp
from threading import Thread
import copy


class ZMQConnector:
    """
    Setup ZeroMQ -> MetaTrader Connector
    """

    ##########################################################################

    def __init__(self,
                 client_id='DLabs_Python',
                 host='localhost',
                 protocol='tcp',
                 push_port=32768,
                 pull_port=32769,
                 sub_port=32770,
                 delimiter=';',
                 pulldata_handlers=None,
                 subdata_handlers=None,
                 verbose=False):

        """
        Parameters
        ----------
        client_id : str
            Unique ID for this client
        host : str
            Host to connect to
        protocol : {'tcp', 'udp'}
            Connection protocol
        push_port : int
            Port for Sending commands
        pull_port : int
            Port for Receiving commands
        sub_port : int
            Port for Subscribing for prices
        delimiter : str
            command strings delimiter
        pulldata_handlers : list of class
            Handlers to process data received through PULL port
        subdata_handlers : list of class
            Handlers to process data received through SUB port
        verbose : bool
        """

        # Strategy Status (if this is False, ZeroMQ will not listen for data)
        self._ACTIVE = True

        # Client ID
        self._CLIENT_ID = client_id

        # ZeroMQ Host
        self._HOST = host

        # Connection Protocol
        self._PROTOCOL = protocol

        # ZeroMQ Context
        self.zmq_context = zmq.Context()

        # TCP Connection URL Template
        self._URL = self._PROTOCOL + "://" + self._HOST + ":"

        # Ports for PUSH, PULL and SUB sockets respectively
        self._PUSH_PORT = push_port
        self._PULL_PORT = pull_port
        self._SUB_PORT = sub_port

        # Handlers for received data (pull and sub ports)
        self.pulldata_handlers = pulldata_handlers or []
        self.subdata_handlers = subdata_handlers or []

        # Create Sockets
        self.push_socket = self.zmq_context.socket(zmq.PUSH)
        self.push_socket.setsockopt(zmq.SNDHWM, 1)

        self.pull_socket = self.zmq_context.socket(zmq.PULL)
        self.pull_socket.setsockopt(zmq.RCVHWM, 1)

        self.sub_socket = self.zmq_context.socket(zmq.SUB)

        # Bind PUSH Socket to send commands to MetaTrader
        self.push_socket.connect(self._URL + str(self._PUSH_PORT))
        print("[INIT] Ready to send commands to METATRADER (PUSH): " + str(self._PUSH_PORT))

        # Connect PULL Socket to receive command responses from MetaTrader
        self.pull_socket.connect(self._URL + str(self._PULL_PORT))
        print("[INIT] Listening for responses from METATRADER (PULL): " + str(self._PULL_PORT))

        # Connect SUB Socket to receive market data from MetaTrader
        self.sub_socket.connect(self._URL + str(self._SUB_PORT))
        print("[INIT] Listening for market data from METATRADER (SUB): " + str(self._SUB_PORT))

        # Initialize POLL set and register PULL and SUB sockets
        self.poller = zmq.Poller()
        self.poller.register(self.pull_socket, zmq.POLLIN)
        self.poller.register(self.sub_socket, zmq.POLLIN)

        # Start listening for responses to commands and new market data
        self._DELIMITER = delimiter

        # BID/ASK Market Data Subscription Threads ({SYMBOL: Thread})
        self._DATA_THREAD = None

        # Begin polling for PULL / SUB data
        self._DATA_THREAD = Thread(target=self._receive_data, args=self._DELIMITER)
        self._DATA_THREAD.start()

        # Market Data Dictionary by Symbol (holds tick data) or Instrument (holds OHLC data)
        self.Data_DB = {}  # {SYMBOL: {TIMESTAMP: (BID, ASK)}}
        # {SYMBOL: {TIMESTAMP: (TIME, OPEN, HIGH, LOW, CLOSE, TICKVOL, SPREAD, VOLUME)}}

        # Temporary Order STRUCT for convenience wrappers later.
        self.temp_order_dict = self.gen_default_order_dict()

        # Thread returns the most recently received DATA block here
        self.thread_data_output = None

        # Verbosity
        self._VERBOSE = verbose

    ##########################################################################
    # internals

    @staticmethod
    def _remote_send(socket, data):
        """
        Function to send commands to MetaTrader (PUSH)
        """

        try:
            socket.send_string(data, zmq.DONTWAIT)
        except zmq.error.Again:
            print("Resource timeout.. please try again.")
            sleep(0.000000001)

    @staticmethod
    def _remote_recv(socket):
        """
        Function to retrieve data from MetaTrader (PULL or SUB)
        """

        try:
            msg = socket.recv_string(zmq.DONTWAIT)
            return msg
        except zmq.error.Again:
            print("Resource timeout.. please try again.")
            sleep(0.000001)

        return None

    def _get_response_(self):
        return self.thread_data_output

    def _set_response_(self, resp=None):
        self.thread_data_output = resp

    def _valid_response_(self, input_='zmq'):
        # Valid data types
        _types = (dict, DataFrame)

        # If input_ = 'zmq', assume self._zmq.thread_data_output
        if isinstance(input_, str) and input_ == 'zmq':
            return isinstance(self._get_response_(), _types)
        else:
            return isinstance(input_, _types)

    def _set_status(self, new_status=False):
        self._ACTIVE = new_status
        print('**')
        print('[KERNEL] Setting Status to {} - Deactivating Threads.. please wait a bit.'.format(new_status))
        print('**')

    def _push_command(self, _action='OPEN', _type=0,
                      _symbol='EURUSD', _price=0.0,
                      _sl=50, _tp=50, _comment="Python-to-MT",
                      _lots=0.01, _magic=123456, _ticket=0):
        """ Function to construct messages for sending Trade commands to MetaTrader
        format:
            compArray[0] = TRADE
            compArray[1] = ACTION (e.g. OPEN, MODIFY, CLOSE)
            compArray[2] = TYPE (e.g. OP_BUY, OP_SELL, etc - only used when ACTION=OPEN)
            // ORDER TYPES:
            // https://docs.mql4.com/constants/tradingconstants/orderproperties
            // OP_BUY = 0
            // OP_SELL = 1
            // OP_BUYLIMIT = 2
            // OP_SELLLIMIT = 3
            // OP_BUYSTOP = 4
            // OP_SELLSTOP = 5
            compArray[3] = Symbol (e.g. EURUSD, etc.)
            compArray[4] = Open/Close Price (ignored if ACTION = MODIFY)
            compArray[5] = SL
            compArray[6] = TP
            compArray[7] = Trade Comment
            compArray[8] = Lots
            compArray[9] = Magic Number
            compArray[10] = Ticket Number (MODIFY/CLOSE)
        """

        _msg = "{};{};{};{};{};{};{};{};{};{};{}".format('TRADE', _action, _type,
                                                         _symbol, _price,
                                                         _sl, _tp, _comment,
                                                         _lots, _magic,
                                                         _ticket)

        # Send via PUSH Socket
        self._remote_send(self.push_socket, _msg)

    def _receive_data(self, string_delimiter=';'):
        """Function to check Poller for new reponses (PULL) and market data (SUB)"""

        while self._ACTIVE:
            sockets = dict(self.poller.poll())

            # Process response to commands sent to MetaTrader
            if self.pull_socket in sockets and sockets[self.pull_socket] == zmq.POLLIN:
                try:
                    msg = self.pull_socket.recv_string(zmq.DONTWAIT)

                    # If data is returned, store as pandas Series
                    if msg != '' and msg is not None:
                        try:
                            _data = eval(msg)
                            self.thread_data_output = _data
                            if self._VERBOSE:
                                print(_data)  # default logic

                            # invokes data handlers on pull port
                            for hnd in self.pulldata_handlers:
                                hnd.onPullData(_data)
                        except Exception as ex:
                            _exstr = "Exception Type {0}. Args:\n{1!r}"
                            _msg = _exstr.format(type(ex).__name__, ex.args)
                            print(_msg)
                except zmq.error.Again:
                    pass  # resource temporarily unavailable, nothing to print
                except ValueError:
                    pass  # No data returned, passing iteration.
                except UnboundLocalError:
                    pass  # _symbol may sometimes get referenced before being assigned.

            # Receive new market data from MetaTrader
            if self.sub_socket in sockets and sockets[self.sub_socket] == zmq.POLLIN:
                try:
                    msg = self.sub_socket.recv_string(zmq.DONTWAIT)
                    if msg != "":
                        _timestamp = str(Timestamp.now('UTC'))[:-6]
                        _symbol, _data = msg.split(" ")
                        if len(_data.split(string_delimiter)) == 2:
                            _bid, _ask = _data.split(string_delimiter)
                            if self._VERBOSE:
                                print("\n[" + _symbol + "] " + _timestamp + " (" + _bid + "/" + _ask + ") BID/ASK")

                                # Update Market Data DB
                            if _symbol not in self.Data_DB.keys():
                                self.Data_DB[_symbol] = {}
                            self.Data_DB[_symbol][_timestamp] = (float(_bid), float(_ask))
                        elif len(_data.split(string_delimiter)) == 8:
                            _time, _open, _high, _low, _close, _tick_vol, _spread, _real_vol = _data.split(
                                string_delimiter)
                            if self._VERBOSE:
                                print_string = "\n[" + _symbol + "] " + _timestamp + " (" + _time + "/"
                                print_string += _open + "/" + _high + "/" + _low + "/" + _close + "/" + _tick_vol + "/"
                                print_string += _spread + "/" + _real_vol
                                print_string += ") TIME/OPEN/HIGH/LOW/CLOSE/TICKVOL/SPREAD/VOLUME"
                                print(print_string)

                            # Update Market Rate DB
                            if _symbol not in self.Data_DB.keys():
                                self.Data_DB[_symbol] = {}
                            self.Data_DB[_symbol][_timestamp] = (
                                int(_time), float(_open), float(_high), float(_low), float(_close), int(_tick_vol),
                                int(_spread), int(_real_vol))

                        # invokes data handlers on sub port
                        for hnd in self.subdata_handlers:
                            hnd.onSubData(msg)
                except zmq.error.Again:
                    pass  # resource temporarily unavailable, nothing to print
                except ValueError:
                    pass  # No data returned, passing iteration.
                except UnboundLocalError:
                    pass  # _symbol may sometimes get referenced before being assigned.

    def wait_for_pull(self, timeout=1):
        wait_s = 0.01
        rec = None
        for _ in range(int(timeout / wait_s)):
            if self.thread_data_output is None:
                sleep(wait_s)
                continue
            rec = copy.deepcopy(self.thread_data_output)
            break
        self.thread_data_output = None
        return rec

    ##########################################################################
    # Default formats

    @staticmethod
    def gen_default_order_dict():
        return ({'_action': 'OPEN',
                 '_type': 0,
                 '_symbol': 'EURUSD',
                 '_price': 0.0,
                 '_SL': 500,  # SL/TP in POINTS, not pips.
                 '_TP': 500,
                 '_comment': 'DWX_Python_to_MT',
                 '_lots': 0.01,
                 '_magic': 123456,
                 '_ticket': 0})

    @staticmethod
    def gen_default_data_dict():
        return ({'_action': 'DATA',
                 '_symbol': 'EURUSD',
                 '_timeframe': 1440,  # M1 = 1, M5 = 5, and so on..
                 '_start': '2018.12.21 17:00:00',  # timestamp in MT4 recognized format
                 '_end': '2018.12.21 17:05:00'})

    @staticmethod
    def gen_default_hist_dict():
        return ({'_action': 'HIST',
                 '_symbol': 'EURUSD',
                 '_timeframe': 1,  # M1 = 1, M5 = 5, and so on..
                 '_start': '2018.12.21 17:00:00',  # timestamp in MT4 recognized format
                 '_end': '2018.12.21 17:05:00'})

    ##########################################################################
    # Convenience functions to permit easy trading via underlying functions.

    def push_new_order(self, order=None):
        """open a new order"""

        if order is None:
            order = self.gen_default_order_dict()

        # Execute
        self._push_command(**order)

    def push_modify_order(self, ticket, sl, tp):  # in points
        """ modify an order"""

        try:
            self.temp_order_dict['_action'] = 'MODIFY'
            self.temp_order_dict['_SL'] = sl
            self.temp_order_dict['_TP'] = tp
            self.temp_order_dict['_ticket'] = ticket

            # Execute
            self._push_command(**self.temp_order_dict)

        except KeyError:
            print("[ERROR] Order Ticket {} not found!".format(ticket))

    def push_close_order(self, ticket):
        """ close an order"""

        try:
            self.temp_order_dict['_action'] = 'CLOSE'
            self.temp_order_dict['_ticket'] = ticket

            # Execute
            self._push_command(**self.temp_order_dict)

        except KeyError:
            print("[ERROR] Order Ticket {} not found!".format(ticket))

    def push_close_order_partial(self, ticket, lots):
        """ close an order, partially"""

        try:
            self.temp_order_dict['_action'] = 'CLOSE_PARTIAL'
            self.temp_order_dict['_ticket'] = ticket
            self.temp_order_dict['_lots'] = lots

            # Execute
            self._push_command(**self.temp_order_dict)

        except KeyError:
            print("[ERROR] Order Ticket {} not found!".format(ticket))

    def push_close_magic(self, magic):
        """close orders by magic number"""

        try:
            self.temp_order_dict['_action'] = 'CLOSE_MAGIC'
            self.temp_order_dict['_magic'] = magic

            # Execute
            self._push_command(**self.temp_order_dict)

        except KeyError:
            pass

    def push_close_all(self):
        """close all orders"""
        try:
            self.temp_order_dict['_action'] = 'CLOSE_ALL'

            # Execute
            self._push_command(**self.temp_order_dict)

        except KeyError:
            pass

    def push_req_open_orders(self):
        """get all open orders"""

        try:
            self.temp_order_dict['_action'] = 'GET_OPEN_TRADES'

            # Execute
            self._push_command(**self.temp_order_dict)

        except KeyError:
            pass

    def push_req_market_data(self,
                             symbol='EURUSD',
                             timeframe=1,
                             start='2019.01.04 17:00:00',
                             end=Timestamp.now().strftime('%Y.%m.%d %H:%M:00')):

        _msg = "{};{};{};{};{}".format('DATA',
                                       symbol,
                                       timeframe,
                                       start,
                                       end)
        # Send via PUSH Socket
        self._remote_send(self.push_socket, _msg)

    def push_req_market_hist(self,
                             symbol='EURUSD',
                             timeframe=1,
                             start='2019.01.04 17:00:00',
                             end=Timestamp.now().strftime('%Y.%m.%d %H:%M:00')):
        # _end='2019.01.04 17:05:00'):

        _msg = "{};{};{};{};{}".format('HIST',
                                       symbol,
                                       timeframe,
                                       start,
                                       end)
        # Send via PUSH Socket
        self._remote_send(self.push_socket, _msg)

    def push_req_prices_track(self,
                              symbols=None):
        symbols = symbols or ['EURUSD']
        _msg = 'TRACK_PRICES'
        for s in symbols:
            _msg += ";{}".format(s)

        # Send via PUSH Socket
        self._remote_send(self.push_socket, _msg)

    def push_req_rates_track(self,
                             instruments=None):
        instruments = instruments or [('EURUSD_M1', 'EURUSD', 1)]
        _msg = 'TRACK_RATES'
        for i in instruments:
            _msg += ";{};{}".format(i[1], i[2])

        # Send via PUSH Socket
        self._remote_send(self.push_socket, _msg)

    ##########################################################################

    def subs_add_marketdata(self, symbol, string_delimiter=';'):

        # Subscribe to SYMBOL first.
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, symbol)

        if self._DATA_THREAD is None:
            self._DATA_THREAD = Thread(target=self._receive_data, args=string_delimiter)
            self._DATA_THREAD.start()

        print("[KERNEL] Subscribed to {} MARKET updates. See self.Data_DB.".format(symbol))

    def subs_remove_marketdata(self, symbol):
        self.sub_socket.setsockopt_string(zmq.UNSUBSCRIBE, symbol)
        print('**')
        print("[KERNEL] Unsubscribing from " + symbol)
        print('**')

    def subs_remove_marketdata_all(self):
        self._set_status(False)
        self._DATA_THREAD = None

    ##########################################################################
