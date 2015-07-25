from flask import Flask
from store import Store
from datetime import datetime
from rmq import rmq
app = Flask(__name__)

connection = rmq('localhost')

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/insert/<string:name>/<int:data>')
def insert(name, data):
    global connection
    doc = {'time': datetime.utcnow(), 'name': name, 'data': data}
    print doc
    s = Store('localhost:27017')
    s.store('test', 'doc', doc)
    connection.push('stats', doc)
    return 'Inserted [%s:%d]' % (name, data)

@app.route('/total/')
def total():
    result = ['<head/><body><table><tr><td>Name</td><td>Total</td></tr>']
    s = Store('localhost:27017')
    for record in s.find('test', 'total', {}):
      result.append('<tr><td>%s</td><td>%d</td></tr>' % (record['_id'], record['value']))
    result.append('</table><body>')
    return '\n'.join(result)

if __name__ == '__main__':
    s = Store('localhost:27017')
    s.create_index('test', 'doc', 'time')
    app.debug = True
    app.run('0.0.0.0')

