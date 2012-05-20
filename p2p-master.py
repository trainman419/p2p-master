#!/usr/bin/env python
#
# Peer-to-peer ROS Master
# Proof-of-concept
#
# Theory: 
#  All nodes talk to a ROS master that lives on their local machine; this
#  master then discovers other masters on the network and builds a shared table
#  of topics, services and parameters with them. 
#
#  The master is responsible for coordinating network transport as well; it 
#  understands the network topology (via connections to its peers), subscribes
#  to machine-local topics and publishes them for remote consumption as
#  appropriate
#
# 
# For this proof-of-concept master, the following features are implemented:
# * (none)
#
# Running:
#  set ROS_MASTER_URI as appropriate
#  provide a configuration file that lists the master URIs of peers to contact
#
# Usage:
#  ROS_MASTER_URI=http://<name>:<port>
#  p2p-master.py <config.yaml>

import os
import sys

from SimpleXMLRPCServer import SimpleXMLRPCServer,SimpleXMLRPCRequestHandler

class Master(SimpleXMLRPCRequestHandler):
   def __init__(self):
      self.foo = 0

   # stub
   def shutdown(self, caller_id, msg=''):
      return 1, "shutdown", 0

   # stub
   def getUri(self, caller_id):
      return 1, "", ""

   def getPid(self, caller_id):
      return 1, "", os.getpid()

   # stub
   def deleteParam(self, caller_id, key):
      return 1, "", 0

   # stub
   def setParam(self, caller_id, key, value):
      return 1, "", 0

   # stub
   def getParam(self, caller_id, key):
      return 1, "", 0
   
   # stub
   def searchParam(self, caller_id, key):
      return 1, "", ""

   # stub
   def subscribeParam(self, caller_id, caller_api, key):
      return 2, "", ""

   # stub
   def unsubscribeParam(self, caller_id, caller_api, key):
      return 1, "", 0

   # stub
   def hasParam(self, caller_id, key):
      return 1, key, False

   # stub
   def getParamNames(self, caller_id):
      return 1, "Parameter names", []

   # TODO: stub
   def registerSubscriber(self, caller_id, topic, topic_type, caller_api):
      print "registerSubscriber"
      return 1, "Subscribed to [%s]"%topic, []
   
   # TODO: stub
   def unregisterSubscriber(self, caller_id, topic, caller_api):
      print "unregisterSubscriber"
      return 1

   # TODO: stub
   def registerPublisher(self, caller_id, topic, topic_type, caller_api):
      print "registerPublisher"
      return 1, "Registered [%s] as publisher of [%s]"%(caller_id, topic), []

   # TODO: stub
   def unregisterPublisher(self, caller_id, topic, caller_api):
      print "unregisterPublisher"
      return 1


def main():
   server = SimpleXMLRPCServer(("localhost", 11311))
   master = Master()
   server.register_multicall_functions()
   server.register_instance(master)
   print "Ready?"
   server.serve_forever()

if __name__ == '__main__':
   main()
