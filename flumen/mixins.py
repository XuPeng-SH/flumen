import multiprocessing
import  threading
# from queue import Queue
import queue
from queue import Empty
import logging

logger = logging.getLogger(__name__)

class AsyncMixin:
    queue_size = None
    def _enqueue(self, event):
        self.queue.put(event)
        return True

    def enqueue(self, event):
        if not self.to_run():
            return False
        return self._enqueue(event)

    def take(self):
        try:
            event = self.queue.get(timeout=0.5) # TODO
            return event
        except Empty:
            return None

    def consume(self, event):
        if not event:
            return
        handler = self.get_bound_handler(event.event_name)
        if handler:
            resp = handler.handle_event(event) # TODO: handle resp
            return
        # logger.warn('No handler instance is bound to {}'.format(event.event_name))
        resp =  event.dispatch_to_handler() # TODO: handle resp

    def worker(self):
        event = self.take()
        self.consume(event)

    def to_run(self):
        return self._to_run()

    def stop(self):
        self._stop()
        self.join()

    def run(self):
        while self.to_run() or not self.queue.empty():
            self.worker()

class ProcessingMixin(AsyncMixin, multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.queue = multiprocessing.Queue(self.queue_size) if self.queue_size else multiprocessing.Queue()
        self.state = multiprocessing.Value('d', 0)

    def _stop(self):
        self.state.value = 1

    def _to_run(self):
        return self.state.value == 0


class ProcessingPoolMixin(AsyncMixin):
    worker_cnt = None
    def __init__(self):
        self.queue = multiprocessing.Queue(self.queue_size) if self.queue_size else multiprocessing.Queue()
        self.worker_cnt = self.worker_cnt if self.worker_cnt else multiprocessing.cpu_count()
        self.state = multiprocessing.Value('d', 0)
        self.pool = []
        self.started = False

    def set_worker_cnt(self, cnt):
        if not cnt:
            return
        if self.started:
            logger.critical('Cannot set worker cnt after started!')
            return
        if cnt > 0 and cnt <= multiprocessing.cpu_count():
            self.worker_cnt = cnt

    def start(self):
        if self.started:
            logger.critical('Already started! Call stop!')
            self.stop()
        self.pool = [multiprocessing.Process(target=self.run) for _ in range(self.worker_cnt)]
        self.started = True
        for process in self.pool:
            process.start()

    def join(self):
        for process in self.pool:
            process.join()

    def _stop(self):
        self.state.value = 1

    def _to_run(self):
        return self.state.value == 0


class ThreadingMixin(AsyncMixin, threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.queue = queue.Queue(self.queue_size) if self.queue_size else queue.Queue()
        self.terminate = False

    def _stop(self):
        self.terminate = True

    def _to_run(self):
        return not self.terminate
