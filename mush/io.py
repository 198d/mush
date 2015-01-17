import asyncio
import random
from concurrent import futures


@asyncio.coroutine
def copy_string(string, destination):
    destination.write(string.encode())
    yield from destination.drain()
    destination.write_eof()
    yield from destination.drain()


@asyncio.coroutine
def copy_stream(source, destination, linewise=False):
    while not source.at_eof():
        if linewise:
            destination.write((yield from source.readline()))
        else:
            destination.write((yield from source.read(4096)))
        yield from destination.drain()
    destination.write_eof()
    yield from destination.drain()


class MultiStreamWriter:
    def __init__(self, writers):
        self.writers = writers

    def write_eof(self):
        for writer in self.writers:
            writer.write_eof()

    def write(self, data):
        for writer in self.writers:
            writer.write(data)

    @asyncio.coroutine
    def drain(self):
        for writer in self.writers:
            yield from writer.drain()


class MultiStreamReader:
    def __init__(self, readers):
        self.readers = readers
        self.current_tasks = {}

    def __bool__(self):
        return not self.at_eof()

    def at_eof(self):
        return all([reader.at_eof() for reader in self.readers]) and \
            all([not any(tasks) for _, tasks in self.current_tasks.items()])

    @asyncio.coroutine
    def readline(self):
        current_tasks = self.current_tasks.setdefault(
            'readline', [None] * len(self.readers))

        for index, pair in enumerate(zip(current_tasks, self.readers)):
            task, reader = pair
            if not reader.at_eof() and not task:
                current_tasks[index] = asyncio.Task(reader.readline())

        tasks = list(filter(None, current_tasks))

        if tasks:
            done, pending = yield from asyncio.wait(
                tasks, return_when=futures.FIRST_COMPLETED)
        else:
            return b''

        result_task = random.choice(list(done))

        for index, task in enumerate(current_tasks):
            if task == result_task:
                current_tasks[index] = None
                break

        return result_task.result()


class HostStreamReader:
    def __init__(self, host, reader):
        self.host = host
        self.reader = reader

    def __bool__(self):
        return not self.at_eof()

    def at_eof(self):
        return self.reader.at_eof()

    @asyncio.coroutine
    def readline(self):
        return HostBytes(self.host, (yield from self.reader.readline()))

    @asyncio.coroutine
    def read(self, n=-1):
        return HostBytes(
            self.host, (yield from self.reader.read(n)))


class HostBytes(bytearray):
    def __init__(self, host, data):
        super().__init__(data)
        self.host = host
