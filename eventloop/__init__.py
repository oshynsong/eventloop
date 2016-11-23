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

# Epoll constants
_EPOLLIN = 0x001
_EPOLLPRI = 0x002
_EPOLLOUT = 0x004
_EPOLLERR = 0x008
_EPOLLHUP = 0x010
_EPOLLRDHUP = 0x2000
_EPOLLONESHOT = (1 << 30)
_EPOLLET = (1 << 31)

# eventloop supported event types
EVENT_NONE  = 0
EVENT_READ  = _EPOLLIN
EVENT_WRITE = _EPOLLOUT
EVENT_ERROR = _EPOLLERR | _EPOLLHUP

from eventloop.loop import Loop

__version__ = '0.1.0'
__author__ = 'Oshyn Song (dualyangsong@gmail.com)'
__all__ = ['Loop', 'EVENT_NONE', 'EVENT_READ', 'EVENT_WRITE', 'EVENT_ERROR']

