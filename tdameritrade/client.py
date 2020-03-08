import json
import os
import time

import pandas as pd
import requests

from tdameritrade import auth

from .urls import (ACCOUNTS, HISTORY, INSTRUMENTS, MOVERS, OPTIONCHAIN, ORDERS,
                   ORDER_REPLACE, QUOTES, SEARCH)

class TDClient(object):

    def __init__(self, clientId=None, refreshToken=None, accountIds=[]):
        self._clientId = clientId
        self._refreshToken = {'token': refreshToken}
        self._accessToken = {'token': ''}
        self._accessToken['created_at'] = time.time()
        # Set to -1 so that it gets refreshed immediately and its age tracked.
        self._accessToken['expires_in'] = -1
        self._accountIds = accountIds

    def _headers(self):
        return {
            'Authorization': 'Bearer ' + self._accessToken['token'],
            'Content-Type': 'application/json'
        }

    def _updateAccessTokenIfExpired(self):
        # Expire the token one minute before its expiration time to
        # be safe
        if not self._accessToken['token'] or \
                self._accessTokenAgeSecs() >= self._accessToken['expires_in'] - 60:
            token = auth.access_token(self._refreshToken['token'],
                                      self._clientId)
            self._accessToken['token'] = token['access_token']
            self._accessToken['created_at'] = time.time()
            self._accessToken['expires_in'] = token['expires_in']

    def _accessTokenAgeSecs(self):
        return time.time() - self._accessToken['created_at']

    def accounts(self, positions=False, orders=False):
        ret = {}

        if positions or orders:
            fields = '?fields='
            if positions:
                fields += 'positions'
                if orders:
                    fields += ',orders'
            elif orders:
                fields += 'orders'
        else:
            fields = ''

        if self._accountIds:
            for acc in self._accountIds:
                self._updateAccessTokenIfExpired()
                resp = requests.get(ACCOUNTS + str(acc) + fields,
                                    headers=self._headers())
                if resp.status_code == 200:
                    ret[acc] = resp.json()
                else:
                    raise Exception(resp.text)
        else:
            self._updateAccessTokenIfExpired()
            resp = requests.get(ACCOUNTS + fields, headers=self._headers())
            if resp.status_code == 200:
                for account in resp.json():
                    ret[account['securitiesAccount']['accountId']] = account
            else:
                raise Exception(resp.text)

        return ret

    def accountsDF(self):
        return pd.io.json.json_normalize(self.accounts())

    def search(self, symbol, projection='symbol-search'):
        self._updateAccessTokenIfExpired()

        return requests.get(SEARCH,
                            headers=self._headers(),
                            params={'symbol': symbol,
                                    'projection': projection}).json()

    def searchDF(self, symbol, projection='symbol-search'):
        ret = []
        dat = self.search(symbol, projection)
        for symbol in dat:
            ret.append(dat[symbol])

        return pd.DataFrame(ret)

    def fundamental(self, symbol):
        return self.search(symbol, 'fundamental')

    def fundamentalDF(self, symbol):
        return self.searchDF(symbol, 'fundamental')

    def instrument(self, cusip):
        self._updateAccessTokenIfExpired()

        return requests.get(INSTRUMENTS + str(cusip),
                            headers=self._headers()).json()

    def instrumentDF(self, cusip):
        return pd.DataFrame(self.instrument(cusip))

    def quote(self, symbol):
        self._updateAccessTokenIfExpired()

        return requests.get(QUOTES,
                            headers=self._headers(),
                            params={'symbol': symbol.upper()}).json()

    def quoteDF(self, symbol):
        x = self.quote(symbol)

        return pd.DataFrame(x).T.reset_index(drop=True)

    def history(self, symbol, **kwargs):
        self._updateAccessTokenIfExpired()
        return requests.get(HISTORY % symbol,
                            headers=self._headers(),
                            params=kwargs).json()

    def historyDF(self, symbol, **kwargs):
        x = self.history(symbol, **kwargs)
        df = pd.DataFrame(x['candles'])
        df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')

        return df

    def options(self, symbol, **kwargs):
        self._updateAccessTokenIfExpired()

        return requests.get(OPTIONCHAIN,
                            headers=self._headers(),
                            params={'symbol': symbol.upper(), **kwargs}).json()

    def optionsDF(self, symbol):
        ret = []
        dat = self.options(symbol)
        for date in dat['callExpDateMap']:
            for strike in dat['callExpDateMap'][date]:
                ret.extend(dat['callExpDateMap'][date][strike])
        for date in dat['putExpDateMap']:
            for strike in dat['putExpDateMap'][date]:
                ret.extend(dat['putExpDateMap'][date][strike])

        df = pd.DataFrame(ret)
        for col in ('tradeTimeInLong', 'quoteTimeInLong',
                    'expirationDate', 'lastTradingDay'):
            df[col] = pd.to_datetime(df[col], unit='ms')

        return df

    def movers(self, index, direction='up', change_type='percent'):
        self._updateAccessTokenIfExpired()
        return requests.get(MOVERS % index,
                            headers=self._headers(),
                            params={'direction': direction,
                                    'change_type': change_type}).json()

    def place_order(self, account_id, order_dict):
        """Places an order specified by `order_dict`.

        An example of a limit order specification:

            {
                "session": "NORMAL",
                "duration": "DAY",
                "orderType": "LIMIT",
                "price": 36,
                "requestedDestination": "AUTO",
                "orderLegCollection": [
                    {
                        "orderLegType": "EQUITY",
                        "instrument": {
                            "assetType": "EQUITY",
                            "symbol": "SPCE"
                        },
                        "instruction": "BUY",
                        "quantity": 1,
                        "quantityType": "SHARES"
                    }
                ],
                "orderStrategyType": "SINGLE"
            }

        Args:
            account_id: Id of the account.
            order_dict: The order specification dictionary.
        """
        self._updateAccessTokenIfExpired()
        return requests.post(ORDERS % account_id,
                             headers=self._headers(),
                             data=json.dumps(order_dict)).json()

    def replace_order(self, account_id, order_id, order_dict):
        """Replaces the order given by `order_id` with the new order.
        The old order will be cancelled and the new order will be created.

        For order specification, see the `place_order()` function docs.

        Args:
            account_id: Id of the account.
            order_id: Id of the order to replace.
            order_dict: The order specification dictionary.
        """
        self._updateAccessTokenIfExpired()
        return requests.put(ORDER_REPLACE % (account_id, order_id),
                            headers=self._headers(),
                            data=json.dumps(order_dict)).json()

    def get_orders(self, account_id, **kwargs):
        """Returns the orders for the account.

        For allowed arguments, see https://developer.tdameritrade.com/
        account-access/apis/get/accounts/%7BaccountId%7D/orders-0

        Args:
            account_id: Id of the account.

        Returns:
            A list of orders for the account.
        """
        self._updateAccessTokenIfExpired()
        return requests.get(ORDERS % account_id,
                            headers=self._headers(),
                            params={**kwargs}).json()

