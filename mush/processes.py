import asyncio
import logging
import subprocess

from mush.exceptions import CommandFailed
from mush.io import copy_string, copy_stream, HostStreamReader, \
    MultiStreamReader, MultiStreamWriter


class Process:
    def __init__(self, host, command, throw=False, combine=False,
                 loop=None, **kwargs):
        self.host = host
        self.command = command
        self.throw = throw
        self.combine = combine
        self.loop = loop or asyncio.get_event_loop()

        kwargs.setdefault('stdout', subprocess.PIPE)
        kwargs.setdefault('stderr', subprocess.PIPE)
        if self.combine:
            kwargs['stderr'] = subprocess.STDOUT
        self.exec_kwargs = kwargs

        self.process = None
        self.running = False
        self.logger = logging.getLogger(self.__class__.__module__)

    @asyncio.coroutine
    def __iter__(self):
        if self.running:
            return (yield from self.returncode)
        return (yield from self())

    @asyncio.coroutine
    def __call__(self, **kwargs):
        yield from self.exec_command(**kwargs)
        return not (yield from self.returncode)

    def __or__(self, other):
        return other.connect(self)

    def connect(self, stdin):
        kwargs = self.exec_kwargs.copy()
        kwargs['stdin'] = stdin
        return self.__class__(self.host, self.command, self.throw,
                              self.combine, self.loop, **kwargs)

    @asyncio.coroutine
    def reduce(self, func, initializer=None):
        value = initializer
        stdout, stderr = yield from self.stream()

        while stdout:
            line = (yield from stdout.readline()).decode()
            if line:
                value = func(value, line)

        return value

    @asyncio.coroutine
    def stream(self, **kwargs):
        yield from self.exec_command(**kwargs)
        return self

    @property
    def stdin(self):
        return self.process.stdin

    @property
    def stdout(self):
        return HostStreamReader(self.host, self.process.stdout)

    @property
    def stderr(self):
        return HostStreamReader(self.host, self.process.stderr)

    @property
    @asyncio.coroutine
    def returncode(self):
        returncode = yield from self.process.wait()

        if returncode and self.throw:
            raise CommandFailed(
                "'{}' exited with return code '{}'".format(
                    self.command, returncode))

        return returncode

    @asyncio.coroutine
    def exec_command(self, **kwargs):
        exec_kwargs = self.prepare_kwargs(**kwargs)

        self.logger.debug("Exec'ing command with kwargs: %s", kwargs)
        self.process = yield from self.host.exec_command(
            self.command, **exec_kwargs)
        self.running = True

        yield from self.stream_stdin(kwargs.get('stdin'))

    def prepare_kwargs(self, **kwargs):
        exec_kwargs = self.exec_kwargs.copy()
        exec_kwargs.update(kwargs)
        exec_kwargs['stdin'] = subprocess.PIPE
        return exec_kwargs

    def stream_stdin(self, stdin):
        stdin = stdin or self.exec_kwargs.get('stdin')

        self.logger.debug('Streaming stdin: %s', stdin)
        if isinstance(stdin, str):
            asyncio.async(copy_string(stdin, self.stdin), loop=self.loop)
        elif isinstance(stdin, ProcessSet) or isinstance(stdin, Process):
            stdout, stderr = yield from stdin.stream()
            asyncio.async(copy_stream(stdout, self.stdin, linewise=True),
                          loop=self.loop)


class ProcessSet(Process):
    def __init__(self, processes, loop=None, **kwargs):
        self.processes = processes
        self.loop = loop
        self.exec_kwargs = kwargs
        self.running = False
        self.logger = logging.getLogger(self.__class__.__module__)

    def connect(self, stdin):
        kwargs = self.exec_kwargs.copy()
        kwargs['stdin'] = stdin
        return self.__class__(self.processes, **kwargs)

    @asyncio.coroutine
    def map(self, func, *args, **kwargs):
        return (yield from asyncio.gather(
            *[func(process, *args, **kwargs) for process in self.processes]))

    @asyncio.coroutine
    def reduce(self, func, initializer=None):
        return (yield from asyncio.gather(
            *[process.reduce(func, initializer)
              for process in self.processes]))

    @property
    def stdin(self):
        return MultiStreamWriter(
            [process.stdin for process in self.processes])

    @property
    def stdout(self):
        return MultiStreamReader(
            [process.stdout for process in self.processes])

    @property
    def stderr(self):
        return MultiStreamReader(
            [process.stderr for process in self.processes])

    @property
    @asyncio.coroutine
    def returncode(self):
        return all((yield from asyncio.gather(
            *[process.returncode for process in self.processes])))

    @asyncio.coroutine
    def exec_command(self, **kwargs):
        exec_kwargs = self.prepare_kwargs(**kwargs)

        for process in self.processes:
            yield from process.exec_command(**exec_kwargs)
        self.running = True

        yield from self.stream_stdin(kwargs.get('stdin'))
