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
import logging
import select
import math
import time
import threading
import numbers
import functools
import heapq
import traceback
from collections import defaultdict

from eventloop import EVENT_NONE, EVENT_READ, EVENT_WRITE, EVENT_ERROR
from eventloop.loopimpl import EpollLoop
from eventloop.loopimpl import KQueueLoop
from eventloop.loopimpl import SelectLoop

try:
    import thread  # py2
except ImportError:
    import _thread as thread  # py3


class Loop(object):
    """ Event loop class for common usage.
    
    Use ``epoll`` (Linux) or ``kqueue`` (BSD and Mac OS X) if they
    are available, or else we fall back on select().
    """
    
    _instance_lock = threading.Lock()
    _current = threading.local()
    _POLL_TIMEOUT = 3600.0
    _THREAD_POOL_SIZE = 3
    
    @staticmethod
    def instance():
        if not hasattr(Loop, "_instance"):
            with Loop._instance_lock:
                if not hasattr(Loop, "_instance"):
                    # New instance after double check
                    Loop._instance = Loop()
        return Loop._instance
    
    @staticmethod
    def is_initialized():
        return hasattr(Loop, '_instance')
    
    @staticmethod
    def del_instance():
        if hasattr(Loop, '_instance'):
            del Loop._instance
    
    @staticmethod
    def current(instance=True):
        current = getattr(Loop._current, "instance", None)
        if current is None and instance:
            return Loop.instance()
        return current
    
    def install_current(self):
        Loop._current.instance = self
    
    @staticmethod
    def del_current():
        Loop._current.instance = None
    
    def __init__(self):
        self._thread_ident = None
        if Loop.current(instance=False) is None:
            self.install_current()
        
        model = None
        if hasattr(select, 'epoll'):
            self._impl = EpollLoop()
            model = 'epoll'
        elif hasattr(select, 'kqueue'):
            self._impl = KQueueLoop()
            model = 'kqueue'
        elif hasattr(select, 'select'):
            self._impl = SelectLoop()
            model = 'select'
        else:
            raise Exception('can not find any async event stratege')
        logging.debug('using event model: %s', model)
        
        # {fd1: {event: handler, event: handler,...}, fd2: {event: handler, ...},...}
        self._poll_handlers            = defaultdict(dict)
        
        # [handler1, handler2,...]
        self._pure_handlers            = []
        self._pure_handler_lock        = threading.Lock()
        
        # [handler1, handler2,...]
        self._periodic_handlers        = []
        self._periodic_handler_cancels = 0
        
        self._running = False # for poll
        self._closing = False # for periodic
        self._stopped = False
        
    def _split_fd(self, fd):
        """ Returns an (fd, obj) pair from an ``fd`` parameter """
        try:
            return fd.fileno(), fd
        except AttributeError:
            return fd, fd
    
    def _close_fd(self, fd):
        """ Utility method to close an ``fd`` """
        try:
            try:
                fd.close()
            except AttributeError:
                os.close(fd)
        except OSError:
            pass
    
    def _setup_logging(self):
        logging.getLogger('eventloop')
    
    def _run_handler(self, handler):
        """ Runs a handler with error handling """
        logging.debug('run handler: %s', repr(handler))
        #threading.Thread(target=handler).start()
        handler()
        if handler.error is not None:
            logging.error('Running handler occurs error: %s', handler.error)
            traceback.print_exc()
        del handler
    
    def time(self):
        """Returns the current time according to the `Loop`'s clock.
        The return value is a floating-point number relative to an
        unspecified time in the past(just use time.time() here).
        """
        return time.time()
    
    def stop(self):
        self._running = False
        self._stopped = True
    
    def close(self, close_fds=False):
        """ Closes the `Loop`, freeing any resources used """
        with self._pure_handler_lock:
            self._closing = True
        if close_fds:
            for fd in self._poll_handlers.iterkeys():
                self._close_fd(fd)
        self._pure_handlers     = None
        self._periodic_handlers = None
        # Close the loopimpl
        self._impl.close()
    
    def add_poll_handler(self, fd, callback, events, *args, **kwargs):
        """
        Registers the given callback to receive the given events for ``fd``.
        
        The ``fd`` argument may either be an integer file descriptor or
        a file-like object with a ``fileno()`` method (and optionally a
        ``close()`` method, which may be called when the `Loop` is shut
        down).
        The ``events`` argument is a bitwise or of the constants
        ``EVENT_READ``, ``EVENT_WRITE``, and ``EVENT_ERROR``.
        When an event occurs, ``handler(fd, events)`` will be run.
        """
        fd, obj = self._split_fd(fd)
        handler = _LoopHandler(functools.partial(callback, *args, **kwargs),
                               fd=fd, events=events)
        if not self._poll_handlers.get(fd, None):
            self._poll_handlers[fd] = {events : handler}
            self._impl.register(fd, events)
        else:
            self._poll_handlers[fd][events] = handler
            old_event = events
            for e in self._poll_handlers[fd].iterkeys():
                old_event |= e
            self._impl.modify(fd, old_event | events)
        logging.debug('%s' % repr(self._poll_handlers))
    
    def remove_poll_handler(self, fd, events=None):
        """ Remove given listening events on ``fd`` """
        fd, obj = self._split_fd(fd)
        if events == None:    
            self._poll_handlers.pop(fd, None)
            try:
                self._impl.unregister(fd)
            except Exception as e:
                logging.error('Remove handler from Loop occurs exception: %s', e.message)
            logging.debug('%s' % repr(self._poll_handlers))
            return
        # Just remove the specific event on fd
        event_handler = self._poll_handlers.get(fd, None)
        if not event_handler:
            return
        new_event_handler = {}
        new_event = EVENT_NONE
        for e, h in event_handler.items():
            k = e & (~events)
            if k != 0:
                new_event_handler[k] = h
                new_event |= k
        # Change the poll handlers
        if len(new_event_handler) > 0:
            self._poll_handlers[fd] = new_event_handler
        else:
            self._poll_handlers.pop(fd, None)
        # Change the listening events
        if new_event != EVENT_NONE:
            self._impl.modify(fd, new_event)
        else:
            self._impl.unregister(fd)
        logging.debug('%s' % repr(self._poll_handlers))
        
    def update_poll_handler(self, fd, events):
        """ Update the events listening on fd """
        fd, obj = self._split_fd(fd)
        self._impl.modify(fd, events)
    
    def add_periodic_handler(self, callback, deadline, *args, **kwargs):
        """ Construct a periodic handler and add to the heap """
        if isinstance(deadline, numbers.Real):
            handler = _LoopHandler(
                functools.partial(callback, *args, **kwargs),
                deadline=deadline)
            heapq.heappush(self._periodic_handlers, handler)
            #logging.debug('%s(total: %d)' % (repr(handler), len(self._periodic_handlers)))
            return handler
        raise TypeError('Not valid deadline(must be a real number)')
    
    def remove_periodic_handler(self, handler):
        """ Remove a periodic pending handler as returned by `add_periodic_handler` """
        #logging.debug('%s' % repr(handler))
        if not handler:
            return
        handler.callback = None
        self._periodic_handler_cancels += 1
        # delay remove for: self._periodic_handlers.remove(handler)
    
    def add_pure_handler(self, callback, *args, **kwargs):
        """
        Add the handler to be called on the next loop iteration
        
        It is safe to call this method from any thread at any time.
        """
        handler = _LoopHandler(functools.partial(callback, *args, **kwargs))
        if thread.get_ident() != self._thread_ident:
            with self._pure_handler_lock:
                if self._closing:
                    return
                self._pure_handlers.append(handler)
        else:
            if self._closing:
                return
            self._pure_handlers.append(handler)
        logging.debug('%s(total: %d)' % (repr(handler), len(self._pure_handlers)))
        return handler
    
    def remove_pure_handler(self, handler):
        """ Remove a pure pending handler as returned by `add_pure_handler` """
        self._pure_handlers.remove(handler)
        logging.debug('%s' % repr(handler))
    
    def start(self):
        if self._running:
            raise RuntimeError("Loop is already running")
        self._setup_logging()
        if self._stopped:
            self._stopped = False
            return
        old_current = getattr(Loop._current, "instance", None)
        Loop._current.instance = self
        self._thread_ident = thread.get_ident()
        self._running = True
        
        try:
            while True:
                # Run pure handlers
                with self._pure_handler_lock:
                    pure_handlers = self._pure_handlers
                    self._pure_handlers = []
                if pure_handlers:
                    for handler in pure_handlers:
                        self._run_handler(handler)
                
                # Run periodic handlers
                periodic_handlers = []
                if self._periodic_handlers:
                    now = self.time()
                    while self._periodic_handlers:
                        if self._periodic_handlers[0].callback is None:
                            heapq.heappop(self._periodic_handlers)
                            self._periodic_handler_cancels -= 1
                        elif self._periodic_handlers[0].deadline <= now:
                            handler = heapq.heappop(self._periodic_handlers)
                            periodic_handlers.append(handler)
                        else:
                            break
                    if (self._periodic_handler_cancels > 512 and 
                        self._periodic_handler_cancels > (len(self._periodic_handlers) >> 1)):
                            self._periodic_handler_cancels = 0
                            self._periodic_handlers = [h for h in self._periodic_handlers
                                                       if h.callback is not None]
                            heapq.heapify(self._periodic_handlers)
                #logging.debug('periodic running: %s' % repr(periodic_handlers))
                for handler in periodic_handlers:
                    if handler.callback is not None:
                        self._run_handler(handler)
                
                # Release memory when necessory before running the poll events
                pure_handlers = periodic_handlers = handler = None
                
                # Generate the poll time and run poll handlers
                poll_timeout = None
                if self._pure_handlers:
                    # If adding new pure handlers, poll should return immediately
                    # without any waiting.
                    poll_timeout = 0.0
                elif self._periodic_handlers:
                    # If periodic handlers adding, get the next deeadline delta for waiting
                    next_periodic = self._periodic_handlers[0].deadline - self.time()
                    poll_timeout = max(0, min(next_periodic, Loop._POLL_TIMEOUT))
                else:
                    poll_timeout = Loop._POLL_TIMEOUT
                if not self._running or self._closing:
                    break
                ready_fd_event = self._impl.poll(poll_timeout)
                for fd, event in ready_fd_event:
                    event_handler = self._poll_handlers[fd]
                    for e, h in event_handler.items():
                        if event & e != 0:
                            self._run_handler(h)
                poll_timeout = ready_fd_event = fd = event = None
        finally:
            self._stopped = False
            Loop._current.instance = old_current


class _LoopHandler(object):
    """ Global handler for Loop """
    # Reduce memory overhead when there are lots of pending callbacks
    __slots__ = ['callback', 'fd', 'events', 'deadline', 'error']

    def __init__(self, callback, fd=None, events=None, deadline=None, error=None):
        if deadline != None and not isinstance(deadline, numbers.Real):
            raise TypeError("Unsupported deadline %r" % deadline)
        self.callback = callback
        self.fd       = fd
        self.events   = events
        self.deadline = deadline
        self.error    = None

    def __lt__(self, other):
        delta = self.deadline - other.deadline
        if delta != 0:
            return delta
        else:
            return id(self) - id(other)
    
    def __call__(self):
        try:
            self.callback()
        except Exception as e:
            self.error = e.message
    
    def __repr__(self):
        res = 'handler %s' % repr(self.callback)
        res += (' with fd=%d' % self.fd) if self.fd is not None else ''
        res += (' with deadline=%d' % self.deadline) if self.deadline is not None else ''
        res += (' with events=%d' % self.events) if self.events is not None else ''
        return res
    

