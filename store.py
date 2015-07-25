from pymongo import MongoClient

def cached(cls):
    _cache = {}
    def new(*args, **kw):
      kw_tp = tuple(sorted(kw.items()))
      args_tp = tuple(args)
      if (args_tp, kw_tp) not in _cache:
        _cache[(args_tp, kw_tp)] = cls(*args, **kw)
      return _cache[(args_tp, kw_tp)]
    return new

@cached
class Store(object):
  def __init__(self, url):
    self.client = MongoClient(url)
  
  def store(self, db, collection, doc):
    self.client[db][collection].insert(doc)
  
  def create_index(self, db, collection, key):
    self.client[db][collection].create_index(key)
 
  def find(self, db, collection, query):
    return self.client[db][collection].find(query)
     
  def __del__(self):
    self.client.close()
