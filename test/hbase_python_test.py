'''
Created on Dec 31, 2014

@author: keyofspectator
'''

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
transport = TSocket.TSocket('localhost', 9090)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
transport.open()
print client.getTableNames()
