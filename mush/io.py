import asyncio
import random
from concurrent import futures


async def copy_string(string, destination):
    destination.write(string.encode())
    await destination.drain()
    destination.write_eof()
    await destination.drain()


async def copy_stream(source, destination, linewise=False):
    while not source.at_eof():
        if linewise:
            destination.write(await source.readline())
        else:
            destination.write(await source.read(4096))
        await destination.drain()
    destination.write_eof()
    await destination.drain()


class MultiStreamWriter:
    def __init__(self, writers):
        self.writers = writers

    def write_eof(self):
        for writer in self.writers:
            writer.write_eof()

    def write(self, data):
        for writer in self.writers:
            writer.write(data)

    async def drain(self):
        for writer in self.writers:
            await writer.drain()


class MultiStreamReader:
    def __init__(self, readers):
        self.readers = readers
        self.current_tasks = {}

    def __bool__(self):
        return not self.at_eof()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        line = await self.readline()
        if line:
            return line
        raise StopAsyncIteration

    def __iter__(self):
        return iter(self.readers)

    def at_eof(self):
        return all([reader.at_eof() for reader in self.readers]) and \
            all([not any(tasks) for _, tasks in self.current_tasks.items()])

    async def readline(self):
        current_tasks = self.current_tasks.setdefault(
            'readline', [None] * len(self.readers))

        for index, pair in enumerate(zip(current_tasks, self.readers)):
            task, reader = pair
            if not reader.at_eof() and not task:
                current_tasks[index] = asyncio.Task(reader.readline())

        tasks = list(filter(None, current_tasks))

        if tasks:
            done, pending = await asyncio.wait(
                tasks, return_when=futures.FIRST_COMPLETED)
        else:
            return b''

        result_task = random.choice(list(done))

        for index, task in enumerate(current_tasks):
            if task == result_task:
                current_tasks[index] = None
                break

        if not result_task.result() and self:
            return await self.readline()

        return result_task.result()


class HostStreamReader:
    def __init__(self, host, reader):
        self.host = host
        self.reader = reader

    async def __aiter__(self):
        return self

    async def __anext__(self):
        line = await self.readline()
        if line:
            return line
        raise StopAsyncIteration

    def __bool__(self):
        return not self.at_eof()

    def at_eof(self):
        return self.reader.at_eof()

    async def readline(self):
        return HostBytes(self.host, await self.reader.readline())

    async def read(self, n=-1):
        return HostBytes(
            self.host, await self.reader.read(n))


class HostBytes(bytearray):
    def __init__(self, host, data):
        super().__init__(data)
        self.host = host
