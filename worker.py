from pymongo import MongoClient
from bson.code import Code

class Worker(object):
  def __init__(self, url, db, collection):
    self.client = MongoClient(url)
    self.db = db
    self.collection = collection
  def _run(self):
    return self.client[self.db][self.collection].map_reduce(self.mapper, 
                                                            self.reducer,
                                                            self.out,
                                                            **self.args)

class Sum(Worker):
  def __init__(self, url, db, collection):
    super(Sum, self).__init__(url, db, collection)
    self.mapper = Code('function() {emit(this.name, this.data)}')
    self.reducer = Code('function(key, values) {return Array.sum(values)}')
    self.out = {'reduce': 'total'}
  def run(self, begin, end):
    self.args = {'query': {'$and': [{'time': {'$gte': begin}},
                                    {'time': {'$lte': end}}]},
                 'full_response': True}
    return self._run()

