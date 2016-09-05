import io
import asyncio
import logging
import subprocess

from mush.exceptions import CommandFailed
from mush.io import copy_string, copy_stream, copy_file, HostStreamReader, \
    MultiStreamReader, MultiStreamWriter


class Process:
    def __init__(self, host, command, loop=None, **kwargs):
        self.host = host
        self.command = command
        self.loop = loop or asyncio.get_event_loop()

        self.exec_kwargs = kwargs.copy()
        self.exec_kwargs['stdin'] = subprocess.PIPE

        self.process = None

        self._running = False
        self._ready = asyncio.Queue(1)

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
        if not self._running:
            await self._exec()
            self._running = True
            self._ready.put_nowait(None)
        return self

    def __or__(self, other):
        return Pipeline([self, other])

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
        if self.process:
            return self.process.stdin

    @property
    def stdout(self):
        if self.process and self.process.stdout:
            return HostStreamReader(self.host, self.process.stdout)

    @property
    def stderr(self):
        if self.process and self.process.stderr:
            return HostStreamReader(self.host, self.process.stderr)

    @property
    def returncode(self):
        if self.process:
            return self.process.wait()

    @property
    async def success(self):
        await self()
        return not await self.returncode

    @property
    async def ready(self):
        if not self._running:
            await self._ready.get()
        return self._running

    def stream_stdin(self, stdin):
        self.logger.debug('Streaming stdin: %s', stdin)
        stream_task = None

        if isinstance(stdin, str):
            def stream_task():
                return copy_string(stdin, self.stdin)
        elif isinstance(stdin, Process):
            async def stream_task():
                await stdin.stream()
                return await copy_stream(stdin.stdout, self.stdin,
                                         linewise=True)
        elif isinstance(stdin, io.TextIOBase) or \
                isinstance(stdin, io.BufferedIOBase):
            def stream_task():
                return copy_file(stdin, self.stdin, linewise=True)

        if stream_task:
            async def stream_when_ready():
                await self.ready
                await stream_task()
            asyncio.ensure_future(stream_when_ready(), loop=self.loop)

    async def _exec(self):
        self.process = await self.host.exec_command(
            self.command, **self.exec_kwargs)


class ProcessSet(Process):
    def __init__(self, processes, loop=None, **kwargs):
        self.processes = processes

        self._running = False
        self._ready = asyncio.Queue(1)

        self.loop = loop or asyncio.get_event_loop()
        self.logger = logging.getLogger(self.__class__.__module__)

    def __repr__(self):
        return repr(self.processes)

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
        await self()
        return all(await asyncio.gather(
            *[process.success for process in self.processes]))

    async def _exec(self):
        for process in self.processes:
            await process
        return self


class Pipeline(ProcessSet):
    def __or__(self, other):
        self.processes.append(other)
        return self

    def stream(self, *args, **kwargs):
        self.processes[-1].stream()
        return self

    def devnull(self, *args, **kwargs):
        self.processes[-1].devnull()
        return self

    @property
    def stdin(self):
        return self.processes[0].stdin

    @property
    def stdout(self):
        return self.processes[-1].stdout

    @property
    def stderr(self):
        return self.processes[-1].stderr

    async def _exec(self):
        current_proc = await self.processes[0].stream()
        for next_proc in self.processes[1:-1]:
            next_proc = await next_proc.stream()
            next_proc < current_proc
            current_proc = next_proc
        last_proc = await self.processes[-1]
        last_proc < current_proc
        return self
