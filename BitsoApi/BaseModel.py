try:
    basestring
except NameError:
    basestring = str

from datetime import datetime
from decimal import Decimal
from future.utils import iteritems
import dateutil.parser

class BaseModel(object):

    """ Base class for other models. """
    
    def __init__(self, **kwargs):
        self._default_params = {}

    @classmethod
    def _NewFromJsonDict(cls, data, **kwargs):
        if kwargs:
            for key, val in kwargs.items():
                data[key] = val
        return cls(**data)

class Book(BaseModel):
    """A class that represents the Bitso orderbook and it's limits"""

    def __init__(self, **kwargs):
        self._default_params = {
            'symbol': kwargs.get('book'),
            'minimum_amount': Decimal(kwargs.get('minimum_amount')),
            'maximum_amount': Decimal(kwargs.get('maximum_amount')),
            'minimum_price': Decimal(kwargs.get('minimum_price')),
            'maximum_price': Decimal(kwargs.get('maximum_price')),
            'minimum_value': Decimal(kwargs.get('minimum_value')),
            'maximum_value': Decimal(kwargs.get('maximum_value'))
        }
        
        for (param, val) in self._default_params.items():
            setattr(self, param, val)

    def __repr__(self):
        return "Book(symbol={symbol})".format(symbol=self.symbol)
    
class AvailableBooks(BaseModel):
    """A class that represents Bitso's orderbooks"""
    def __init__(self, **kwargs):
        self.books = []
        for ob in kwargs.get('payload'):
            self.books.append(ob['book'])
            setattr(self, ob['book'], Book._NewFromJsonDict(ob))

    def __repr__(self):
        return "AvilableBooks(books={books})".format(books=','.join(self.books))
