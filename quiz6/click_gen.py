#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""message-generation.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1TNNnrWJJWVZ_N81QcWUpig7Lk1MmlVo2

# Message Generation

An infrastructure for generating random queries from multiple senders

* **Sender** represents a user who sends requests to the collection point. A sender has 
  * a name
  * a list of queries it may send
  * &mu; and &sigma;, the mean and standard deviation of the interval between successive messages.
* **Dispatcher** is the machinery for sending messages. All the pending messages are queued up and sent at the required time. We initialize a dispatcher by adding senders to it and then `launch`ing it, as seen in cell 5, like so:
```
for ev in dispatcher.launch():
    Process event ev
    if done_processing_all_events():
        break
```
* **Event** is the actual message consisting of the sender, the message and the time at which the message is sent.
"""

import sys
import time, datetime
import random
import math
import hashlib
import heapq
from collections import Counter

random.seed(3000)

# Cell 2
class Event:
    def __init__(self, at, sender, msg):
        self.at = at
        self.sender = sender
        self.msg = msg
    def __lt__(self, other):
        return ((self.at) < (other.at))
    def __eq__(self, other):
        return ((self.sender == other.sender) and (self.msg == other.msg))
    def __str__(self):
        return ('At '+ str(self.at) + ', ' + str(self.sender) + ' said: ' + self.msg)
    def __repr__(self):
        return self.__str__()

class Sender:
    def __init__(self, name, msgs = 0, μ = 1.0, σ = 0.0):
        # print (name, msgs, μ, σ)
        self.name = name
        self.msgs  = msgs # range of message values (0 to msgs, inclusive)
        self.delay = (μ, σ,)
    def __str__(self):
        # return '%s: %d (%.3f, %.3f)' % (self.name, self.msgs, self.delay[0], self.delay[1])
        return 'sender %s ' % (self.name)
    def __repr__(self):
        return '%s: %d (%.3f, %.3f)' % (self.name, self.msgs, self.delay[0], self.delay[1])
    def new_event(self):
        utcnow = datetime.datetime.utcnow()
        Δt = self.Δt()
        event_time = utcnow + datetime.timedelta(seconds = Δt)
        event = Event(event_time, self, self.msg())
        return event
    def Δt(self):
        return random.normalvariate(self.delay[0], self.delay[1])
    def msg(self):
        return 'qry%04d'%(random.randint(0, self.msgs))

class Dispatcher:
    senders = []
    ev_next = []
    def __str__(self):
        senders = '\n'.join([str(elem) for elem in list(self.senders)])
        return '\n'.join((self.ev_next,))
    def add_sender(self, sender):
        self.senders.append(sender)
        first_event = sender.new_event()
        heapq.heappush(self.ev_next, (first_event.at, first_event))
        return sender
    def repl_event(self, event_old):
        new_event = event_old.sender.new_event()
        heapq.heapreplace(self.ev_next, (new_event.at, new_event))
        return new_event
    def launch(self):
        while True:
            utcnow = datetime.datetime.utcnow()
            # print(self.ev_next)
            # break
            next_event = heapq.nsmallest(1, self.ev_next)[0][1]
            event_at, event_sender, event_msg = next_event.at, next_event.sender, next_event.msg
            # print (event_at, event_sender, event_msg)
            if utcnow <= event_at:
                # the earliest event in the queue is yet to come, we must wait till then.
                wait_time = event_at - utcnow
                wait_secs = wait_time.total_seconds()
                print('waiting', wait_secs, file=sys.stderr)
                time.sleep(math.ceil(wait_secs))
            else:
                # the earliest event in the queue is in the past, dispatch it!
                # print('dispatch', event_at, event_sender, event_msg)
                next_next_event = next_event.sender.new_event()
                # print('next', self.ev_next)
                # print('next_next', next_next_event)
                heapq.heapreplace(self.ev_next, (next_next_event.at, next_next_event))
                yield next_event

def shuffle(list_):
    new_list = list_.copy()
    random.shuffle(new_list)
    return new_list

