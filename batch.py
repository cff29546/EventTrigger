import rmq, sys
from datetime import datetime, timedelta
import worker

class SumJob():
  def __init__(self, wait_time, max_size):
    self.worker = worker.Sum('localhost:27017','test','doc')
    self.wait_time = wait_time
    self.max_size = max_size
    self.q = []
    self._process_count = 0
    self._process_cost = 0
    self._event_count = 0
    self._total_delay = timedelta(0)
    self._max_delay = None
  def push_event(self, event):
    self.q.append(event)
  
  def run(self):
    if len(self.q):
      res = self.worker.run(self.q[0]['time'], self.q[-1]['time'])
      self._process_count += 1
      self._process_cost += res['timeMillis']
      self._event_count += len(self.q)
      now = datetime.now()
      for e in self.q:
        cost = now - e['time']
        self._total_delay += cost
        if not self._max_delay or self._max_delay < cost:
          self._max_delay = cost
      self.q = []
      print ("events: %4d batch: %4d cost: %fms " + 
             "avg delay %s max delay %s") % (self._event_count,
            self._process_count,
            float(self._process_cost) / self._event_count,
            self._total_delay / self._event_count,
            self._max_delay)
  
  def _timer(self, connection):
    self.run()
  
  def onevent(self, connection, data):
    if len(self.q) == 0:
      connection.set_timer(self.wait_time, self._timer)
    self.push_event(data)
    if len(self.q) >= self.max_size:
       connection.clear_timer()
       self.run()

sumjob = SumJob(0,0)

connection = rmq.rmq('localhost')
event = connection.get('stats')
while event:
  sumjob.push_event(event)
  event = connection.get('stats')
sumjob.run()

