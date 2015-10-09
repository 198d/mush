import asyncio
import logging
import subprocess

from mush.exceptions import CommandFailed
from mush.io import copy_string, copy_stream, HostStreamReader, \
    MultiStreamReader, MultiStreamWriter


class Process:
    def __init__(self, host, command, loop=None, **kwargs):
        self.host = host
        self.command = command
        self.loop = loop or asyncio.get_event_loop()

        self.exec_kwargs = kwargs.copy()
        self.exec_kwargs['stdin'] = subprocess.PIPE

        self.process = None
        self.logger = logging.getLogger(self.__class__.__module__)

    def __repr__(self):
        return '<{} `{}` {}>'.format(
            self.__class__.__name__, self.command, self.host)

    def __iter__(self):
        return iter((self.success,
                    MultiStreamReader([self.stdout, self.stderr])))

    async def __aiter__(self):
        return self.stdout

    def __await__(self):
        return self().__await__()

    async def __call__(self):
        self.process = await self.host.exec_command(
            self.command, **self.exec_kwargs)
        return self

    def __or__(self, other):
        other < self
        return other

    def __ror__(self, other):
        self < other
        return self

    def __lt__(self, other):
        self.stream_stdin(other)
        return self

    def stream(self, combine=False):
        self.exec_kwargs['stdout'] = subprocess.PIPE
        if combine:
            self.exec_kwargs['stderr'] = subprocess.STDOUT
        else:
            self.exec_kwargs['stderr'] = subprocess.PIPE
        return self

    def devnull(self, **kwargs):
        self.exec_kwargs['stdout'] = subprocess.DEVNULL
        self.exec_kwargs['stderr'] = subprocess.DEVNULL
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
    def returncode(self):
        return self.process.wait()

    @property
    async def success(self):
        return not await self.returncode

    def stream_stdin(self, stdin):
        self.logger.debug('Streaming stdin: %s', stdin)
        if isinstance(stdin, str):
            asyncio.ensure_future(copy_string(stdin, self.stdin),
                                  loop=self.loop)
        elif isinstance(stdin, Process):
            asyncio.ensure_future(
                copy_stream(stdin.stdout, self.stdin, linewise=True),
                loop=self.loop)


class ProcessSet(Process):
    def __init__(self, processes, loop=None, **kwargs):
        self.processes = processes
        self.loop = loop or asyncio.get_event_loop()
        self.logger = logging.getLogger(self.__class__.__module__)

    def __repr__(self):
        return repr(self.processes)

    async def __call__(self):
        for process in self.processes:
            await process()
        return self

    def stream(self, *args, **kwargs):
        for proc in self.processes:
            proc.stream(*args, **kwargs)
        return self

    def devnull(self, *args, **kwargs):
        for proc in self.processes:
            proc.devnull(*args, **kwargs)
        return self

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
    def returncodes(self):
        return asyncio.gather(
            *[process.returncode for process in self.processes])

    @property
    async def success(self):
        return all(not rc for rc in await self.returncodes)
