#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2010 Watts Lab, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os.path


EFFECTIVE_TLD_NAMES = 'http://mxr.mozilla.org/mozilla-central/source/netwerk/dns/effective_tld_names.dat?raw=1'


def _normalize(s):
    s = s.strip().strip('.').split()
    if len(s) ==0:
        return ''
    s = s[0].lower()
    return s


class PrefixNode:
    '''
    An internal class, each prefix in the tree represents a part of a domain name. (e.g. '.com')
    The naming is a little confusing, a PrefixTree is the data structure, but the nodes are
    used to store the suffixes of partial domain names.
    '''

    #passing a rule lets you build on entire branch in one call
    def __init__(self, prefix, rule=None):
        if prefix is not None:
            self.is_exception = prefix.startswith('!')
            if self.is_exception:
                prefix = prefix[1:]
        self.prefix = prefix
        self.children = dict()
        if rule is not None:
            self.add(rule)

    def __repr__(self):
        return 'PrefixNode(\'%s\')' % (self.prefix,)

    def add(self, rule):
        if len(rule) == 0:
            return
        prefix = rule[0]
        if prefix.startswith('!') and '*' in self.children:
            #if this is an exception to a wildcard it should be a child of that wildcard
            self.children['*'].add(rule)
            return
        if prefix in self.children:
            self.children[prefix].add(rule[1:])
        else:
            if prefix.startswith('!'):
                prefix = prefix[1:]
            self.children[prefix] = PrefixNode(prefix, rule=rule[1:])

    def match(self, rule):
        """Given a rule, splits the tuple into a matching section and domain.

        >>> node = PrefixNode('', ['com'])
        >>> node.match( ('com', 'example', 'www') )
        (['.com'], 'example')

        >>> node = PrefixNode('', ['uk', '*'])
        >>> node.match( ('uk', 'co', 'foo') )
        (['.co', '.uk'], 'foo')

        """
        #print ' %s is matching %s' % (self.prefix, rule)
        if len(rule) == 0:
            #when a tld is also a hostname
            return ([], [])
        if self.prefix == '*':
            return self._match_as_wildcard(rule)
        prefix = rule[0]
        if prefix not in self.children:
            if '*' in self.children:
                return self.children['*'].match(rule)
            return ([], prefix)
        else:
            match = self.children[prefix].match(rule[1:])
            child_matched = match[0]
            child_matched.append('.' + prefix)
            return (child_matched, match[1])

    def _match_as_wildcard(self, rule):
        #print '  %s: matching %s as wildcard. My children are: %s' % (self.prefix, rule, self.children)
        prefix = rule[0]
        #if prefix matches no exception
        if prefix not in self.children:
            if len(rule) > 1:
                return (['.' + prefix], rule[1])
            return (['.' + prefix], None)
        else:
            return ([], prefix)


class PrefixTree(PrefixNode):
    """Helper to provide a nicer interface for dealing with the tree.

    """

    def __init__(self, seq):
        """Create from a sequence of rules.

        @param seq is a sequence of tuples representing rules of the form
        ('com'), ('uk', 'co'), etc.

        """
        PrefixNode.__init__(self, None)
        for rule in seq:
            self.add(rule)

    def __repr__(self):
        return '<PrefixTree (%s children)>' % (len(self.children),)

    def match(self, s):
        rule = tuple(reversed(s.strip().split('.')))
        match = PrefixNode.match(self, rule)
        #print 'Tree matching %s, match was %s' % (s, match)
        if len(match[0]) == 0:
            return None
        return (''.join(match[0]), match[1])

    def domain(self, s):
        if s is None:
            return None
        s = _normalize(s)
        match = self.match(s)
        if match is None or not match[1]:
            return None
        return match[1] + match[0]


def _tokenize(lines):
    rules = []
    for s in lines:
        if s and not s.isspace() and not s.startswith('//'):
            rule = tuple(reversed(_normalize(s).split('.')))
            rules.append(rule)
    return rules


def _is_ip(address):
    parts = address.split(".")
    if len(parts) != 4:
        return False
    for item in parts:
        if not item.isdigit() or not 0 <= int(item) <= 255:
            return False
    return True


suffixtree = None


def init_suffix_tree(tld_file=None):
    """Call this first to initialize the suffix tree"""
    if tld_file is None:
        tld_file = os.path.join(os.path.dirname(__file__), 'public_suffix_list.txt')
    fp = open(tld_file)
    suffix_lines = fp.readlines()
    suffix_rules = _tokenize(suffix_lines)
    fp.close()
    global suffixtree
    suffixtree = PrefixTree(suffix_rules)


def get_root_domain(domain):
    if not domain:
        return None
    domain = _normalize(domain)
    if _is_ip(domain):
        return domain
    return suffixtree.domain(domain)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
