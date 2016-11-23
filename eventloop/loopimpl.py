#!/usr/bin/env python
# -*- coding = utf-8 -*-
#
# Copyright 2016 Oshyn Song
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, with_statement

import sys
import time
import logging
import select

from eventloop import EVENT_NONE, EVENT_READ, EVENT_WRITE, EVENT_ERROR


class LoopImpl(object):
    """ Base class for concrete implementations of event loop class """
    def __init__(self):
        pass
    
    def close(self):
        raise NotImplementedError()
    
    def register(self, fd, event_type):
        raise NotImplementedError()
    
    def unregister(self, fd):
        raise NotImplementedError()
    
    def modify(self, fd):
        raise NotImplementedError()
    
    def pool(self, timeout):
        """ Pool the ready event """
        raise NotImplementedError()


class EpollLoop(LoopImpl):
    """
    A epoll-based event loop implementation for
    system supporting epoll system-call.
    """
    def __init__(self, ):
        super(EpollLoop, self).__init__()
        if not hasattr(select, 'epoll'):
            raise SystemError("Not support epoll for current system.")
        self._epoll = select.epoll()
    
    def close(self):
        self._epoll.close()
    
    def register(self, fd, event_type):
        self._epoll.register(fd, event_type)
    
    def unregister(self, fd):
        self._epoll.unregister(fd)
    
    def modify(self, fd, event_type):
        self._epoll.modify(fd, event_type)
    
    def poll(self, timeout):
        return self._epoll.poll(timeout)


class KQueueLoop(LoopImpl):
    """
    A KQueue-based event loop implementation for BSD/Mac systems.
    """
    # Max events can be handled by kqueue
    MAX_EVENTS = 1024
    
    def __init__(self):
        super(KQueueLoop, self).__init__()
        if not hasattr(select, 'kqueue'):
            raise SystemError("Not support kqueue for current system.")
        self._kqueue = select.kqueue()
        self._active = {}
    
    def close(self):
        self._kqueue.close()
    
    def fileno(self):
        return self._kqueue.fileno()
    
    def register(self, fd, event_type):
        if fd in self._active:
            raise IOError("fd %s already registered" % fd)
        self._control(fd, event_type, select.KQ_EV_ADD)
        self._active[fd] = event_type
    
    def unregister(self, fd):
        try:
            event_type = self._active.pop(fd)
            self._control(fd, event_type, select.KQ_EV_DELETE)
        except KeyError as e:
            # If fd not in active, just ignore
            pass
    
    def modify(self, fd, event_type):
        """ Change the given fd to the give event type """
        self.unregister(fd)
        self.register(fd, event_type)
    
    def poll(self, timeout):
        if timeout < 0:
            timeout = None
        kevents = self._kqueue.control(None, KQueueLoop.MAX_EVENTS, timeout)
        events = {}
        for ke in kevents:
            fd = ke.ident
            if ke.filter == select.KQ_FILTER_READ:
                events[fd] = events.get(fd, 0) | EVENT_READ
            if ke.filter == select.KQ_FILTER_WRITE:
                if ke.flags & select.KQ_EV_EOF:
                    # If an asynchronous connection is refused, kqueue
                    # returns a write event with the EOF flag set.
                    # Turn this into an error for consistency with the
                    # other IOLoop implementations.
                    # Note that for read events, EOF may be returned before
                    # all data has been consumed from the socket buffer,
                    # so we only check for EOF on write events.
                    events[fd] = EVENT_ERROR
                else:
                    events[fd] = events.get(fd, 0) | EVENT_WRITE
            if ke.flags & select.KQ_EV_ERROR:
                events[fd] = events.get(fd, 0) | EVENT_ERROR
        return events.iteritems()
    
    def _control(self, fd, event_type, flags):
        kevents = []
        if event_type & EVENT_WRITE:
            kevents.append(select.kevent(
                fd, filter=select.KQ_FILTER_WRITE, flags=flags))
        if events & EVENT_READ:
            kevents.append(select.kevent(
                fd, filter=select.KQ_FILTER_READ, flags=flags))
        for ke in kevents:
            self._kqueue.control([ke], 0)


class SelectLoop(LoopImpl):
    """
    A select-based event loop implementation for
    system not supporting epoll and kqueue
    """
    def __init__(self, ):
        super(SelectLoop, self).__init__()
        self._read_fd_set  = set()
        self._write_fd_set = set()
        self._error_fd_set = set()
        self._fd_sets = (self._read_fd_set,
                         self._write_fd_set,
                         self._error_fd_set)
    
    def close(self):
        self._read_fd_set.clear()
        self._write_fd_set.clear()
        self._error_fd_set.clear()
    
    def register(self, fd, event_type):
        if fd in self._read_fd_set or fd in self._write_fd_set or fd in self._error_fd_set:
            raise IOError("fd %s already registered" % fd)
        
        if event_type & EVENT_READ:
            self._read_fd_set.add(fd)
        if event_type & EVENT_WRITE:
            self._write_fd_set.add(fd)
        if event_type & EVENT_ERROR:
            self._error_fd_set.add(fd)
    
    def unregister(self, fd):
        """ Remove the watching fd """
        self._read_fd_set.discard(fd)
        self._write_fd_set.discard(fd)
        self._error_fd_set.discard(fd)
    
    def modify(self, fd, event_type):
        """ Change the given fd to the give event type """
        self.unregister(fd)
        self.register(fd, event_type)
    
    def poll(self, timeout):
        readable, writeable, errors = select.select(
            self._read_fd_set,
            self._write_fd_set,
            self._error_fd_set,
            timeout)
        events = {}
        for fd in readable:
            events[fd] = events.get(fd, 0) | EVENT_READ
        for fd in writeable:
            events[fd] = events.get(fd, 0) | EVENT_WRITE
        for fd in errors:
            events[fd] = events.get(fd, 0) | EVENT_ERROR
        return events.items()
    