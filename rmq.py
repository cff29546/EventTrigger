import pika
import pickle

class rmq(object):
  def __init__(self, pram):
    self.connection = pika.BlockingConnection(pika.ConnectionParameters(pram))
    self.channel = self.connection.channel()
    self.timer_id = None
  
  def push(self, queue, data):
    self.channel.queue_declare(queue=queue)
    self.channel.basic_publish(exchange='',
                          routing_key=queue, body=pickle.dumps(data))
  def consume(self, queue, callback, no_ack=True):
    self.callback = callback
    self.no_ack = no_ack
    self.channel.queue_declare(queue=queue)
    self.channel.basic_consume(self._callback, queue=queue, no_ack=no_ack)
 
  def get(self, queue):
    res = self.channel.basic_get(queue=queue, no_ack=True)
    if res[0]:
      return pickle.loads(res[2])
    else:
      return None

  def _callback(self, ch, method, properties, body):
    self.callback(self, pickle.loads(body))
    if not self.no_ack:
      ch.basic_ack(delivery_tag = method.delivery_tag)
  
  def set_timer(self, seconds, timer):
    if self.timer_id:
      self.clear_timer()
    self.timer = timer
    self.timer_id = self.connection.add_timeout(seconds, self._timer)
 
  def clear_timer(self):
    if self.timer_id:
      self.connection.remove_timeout(self.timer_id)
    self.timer_id = None
     
  def _timer(self):
    self.timer(self)
   
  def start(self):
    self.channel.start_consuming()

  def __del__(self):
    self.connection.close()

