import os
import pwd
import asyncio
import logging
import itertools
from fnmatch import fnmatch
from collections import UserDict, UserList

from mush.processes import Process, ProcessSet


class Host(UserDict):
    def __init__(self, user=None, hostname=None, meta=None,
                 loop=None, transport=None, **transport_kwargs):
        self.user = user or pwd.getpwuid(os.getuid()).pw_name
        self.hostname = hostname or 'localhost'

        super().__init__(meta)

        self.loop = (loop or asyncio.get_event_loop())
        self.logger = logging.getLogger(self.__class__.__module__)

        if not transport:
            transport = LocalhostTransport
        self.transport = transport(self, **transport_kwargs)

    def __repr__(self):
        return '<{} {}@{} {}>'.format(self.__class__.__name__, self.user,
                                      self.hostname, repr(self.data))

    def __call__(self, command, *args, **kwargs):
        return Process(self, command.format(*args, **kwargs), loop=self.loop)

    def __getitem__(self, path):
        try:
            return self.data[path]
        except KeyError:
            pass

        data = self.data
        parts = path.split('.')

        for key in parts:
            try:
                if isinstance(data, (list, tuple)):
                    data = type(data)(map(lambda data: data[key], data))
                else:
                    data = data[key]
            except (KeyError, IndexError, TypeError):
                return None

        return data

    async def connect(self):
        pass

    def disconnect(self):
        pass

    def exec_command(self, *args, **kwargs):
        return self.transport.exec_command(*args, **kwargs)

    def tagged(self, tag, *values):
        if tag not in self:
            return False
        elif tag in self:
            if not values and self[tag]:
                return True
            elif not values and not self[tag]:
                return False
            else:
                tag_values = self[tag]
                if not isinstance(tag_values, list):
                    tag_values = [tag_values]
                for value in values:
                    for tag_value in tag_values:
                        if (not callable(value) and
                                (isinstance(value, str) and
                                 isinstance(tag_value, str) and
                                 fnmatch(tag_value, value) or
                                 tag_value == value)) or \
                           (callable(value) and value(tag_value)):
                            return True
        return False


class BaseTransport:
    def __init__(self, host, loop=None):
        self.user = host.user
        self.hostname = host.hostname
        self.loop = (loop or asyncio.get_event_loop())
        self.logger = logging.getLogger(self.__class__.__module__)


class LocalhostTransport(BaseTransport):
    connection = True

    def exec_command(self, command, **kwargs):
        self.logger.debug("Exec'ing command: %s", command)
        return asyncio.create_subprocess_exec(
            *['sudo', '-u', self.user, '-i', '--', command],
            loop=self.loop, **kwargs)


class OpenSSHTransport(BaseTransport):
    default_ssh_options = {
        'StrictHostKeyChecking': 'no'
    }

    def __init__(self, host, identity=None, ssh_options=None, loop=None):
        super().__init__(host, loop=loop)

        option_pairs = itertools.chain(self.default_ssh_options.items(),
                                       (ssh_options or {}).items())
        ssh_options = {option: value for option, value in option_pairs}

        if identity:
            ssh_options['IdentityFile'] = identity

        self.ssh_options = list(
            itertools.chain.from_iterable(
                zip(['-o'] * len(ssh_options),
                    ['{}={}'.format(option, value)
                     for option, value in ssh_options.items()])))


    def exec_command(self, command=None, **kwargs):
        ssh_options = kwargs.pop('ssh_options', [])
        command = self._ssh_command(args=[command or ''],
                                    options=ssh_options)

        self.logger.debug("Exec'ing command: %s", command)

        return asyncio.create_subprocess_exec(
            *command, loop=self.loop, **kwargs)

    def _ssh_command(self, options=[], args=[]):
        where = '{}@{}'.format(self.user, self.hostname)
        return (['ssh'] + self.ssh_options + list(options) + [where, '--'] +
                list(args))


class HostSet(Host, UserList):
    def __init__(self, hosts, loop=None):
        UserList.__init__(self, hosts)
        self.loop = (loop or asyncio.get_event_loop())

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass

    def __repr__(self):
        return repr(self.data)

    async def map(self, func, *args, **kwargs):
        return await asyncio.gather(
            *[func(host, *args, **kwargs) for host in self])

    async def connect(self, *args):
        return await asyncio.gather(
            *[host.connect(*args) for host in self])

    def disconnect(self):
        return list(host.disconnect() for host in self)

    def __call__(self, *args, **kwargs):
        return ProcessSet(
            [host(*args, **kwargs) for host in self], loop=self.loop)

    def tagged(self, *args):
        return self.__class__(
            [host for host in self if host.tagged(*args)], self.loop)

    def where(self, func):
        def safe_filter(host):
            try:
                return func(host)
            except:
                return False

        return self.__class__(
            [host for host in self if safe_filter(host)], self.loop)


localhost = Host(user=None, hostname='localhost')
