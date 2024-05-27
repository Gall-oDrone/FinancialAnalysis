#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
#The MIT License (MIT)
#
#Copyright (c) 2016 Mario Romero 
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

from __future__ import absolute_import

import hashlib
import hmac
import json
import time
import requests

from future.utils import iteritems

try:
    from urllib.parse import urlparse, urlencode
except ImportError:
    from urlparse import urlparse
    from urllib import urlencode

def current_milli_time():
    nonce =  str(int(round(time.time() * 1000000)))
    return nonce

class ApiError(Exception):
    pass

class ApiClientError(Exception):
    pass

class Api(object):
    """A python interface for the Bitso API

    Example usage:
      To create an instance of the bitso.Api class, without authentication:
      
        >>> import bitso
        >>> api = bitso.Api()
      
      To get the Bitso price ticker:
      
        >>> ticker = api.ticker()
        >>> print ticker.ask
        >>> print ticker.bid

      To use the private endpoints, initiate bitso.Api with a client_id,
      api_key, and api_secret (see https://bitso.com/developers?shell#private-endpoints):
      
        >>> api = bitso.Api(API_KEY, API_SECRET)
        >>> balance = api.balance()
        >>> print balance.btc_available
        >>> print balance.mxn_available
    """
    
    def __init__(self, key=None, secret=None, timeout=0):
        """Instantiate a bitso.Api object.
        
        Args:
          key:
            Bitso API Key 
          secret:
            Bitso API Secret

  
        """
        self.base_url_v2 = "https://bitso.com/api/v2"
        self.base_url = "https://bitso.com/api/v3"
        self.key = key
        self._secret = secret
        self.timeout = timeout

    def available_books(self):
        """
        Returns:
          A list of bitso.AvilableBook instances
        """
        url = '%s/available_books/' % self.base_url
        resp = self._request_url(url, 'GET')
        return AvailableBooks._NewFromJsonDict(resp)
    
    def _build_auth_payload(self):
        parameters = {}
        parameters['key'] = self.key
        parameters['nonce'] = str(int(time.time()))
        msg_concat = parameters['nonce']+self.client_id+self.key
        parameters['signature'] = hmac.new(self._secret.encode('utf-8'),
                                           msg_concat.encode('utf-8'),
                                           hashlib.sha256).hexdigest()
        return parameters

    def _build_auth_header(self, http_method, url, json_payload=''):
        if json_payload == {} or json_payload=='{}':
            json_payload = ''
        url_components = urlparse(url)
        request_path = url_components.path
        if url_components.query != '':
            request_path+='?'+url_components.query
        nonce = current_milli_time()
        msg_concat = nonce+http_method.upper()+request_path+json_payload
        signature = hmac.new(self._secret.encode('utf-8'),
                                 msg_concat.encode('utf-8'),
                                 hashlib.sha256).hexdigest()
        return {'Authorization': 'Bitso %s:%s:%s' % (self.key, nonce, signature)}

    
    def _request_url(self, url, verb, params=None, private=False):
        headers=None
        if params == None:
            params = {}
        params = {k: v.decode("utf-8") if isinstance(v, bytes) else v for k, v in params.items()}
        if private:
            headers = self._build_auth_header(verb, url, json.dumps(params))
        if verb == 'GET':
            url = self._build_url(url, params)
            if private:
                headers = self._build_auth_header(verb, url)
            try:
                resp = requests.get(url, headers=headers, timeout=self.timeout)
            except requests.RequestException as e:
                raise
        elif verb == 'POST':
            try:
                resp = requests.post(url, json=params, headers=headers, timeout=self.timeout)
            except requests.RequestException as e:
                raise
        elif verb == 'DELETE':
            try:
                resp = requests.delete(url, headers=headers, timeout=self.timeout)
            except requests.RequestException as e:
                raise
        content = resp.content
        data = self._parse_json(content if isinstance(content, basestring) else content.decode('utf-8'))
        return data

    def _build_url(self, url, params):
        if params and len(params) > 0:
            url = url+'?'+self._encode_parameters(params)
        return url

    def _encode_parameters(self, parameters):
        if parameters is None:
            return None
        else:
            param_tuples = []
            for k,v in parameters.items():
                if v is None:
                    continue
                if isinstance(v, (list, tuple)):
                    for single_v in v:
                        param_tuples.append((k, single_v))
                else:
                    param_tuples.append((k,v))
            return urlencode(param_tuples)


         
    def _parse_json(self, json_data):
        try:
            data = json.loads(json_data)
            self._check_for_api_error(data)
        except:
            raise
        return data

    def _check_for_api_error(self, data):
        if data['success'] != True:
            raise ApiError(data['error'])
        if 'error' in data:
            raise ApiError(data['error'])
        if isinstance(data, (list, tuple)) and len(data)>0:
            if 'error' in data[0]:
                raise ApiError(data[0]['error'])