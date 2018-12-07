import uuid
import logging
import typing
from contextlib import contextmanager
from .mixins import ThreadingMixin, ProcessingMixin, ProcessingPoolMixin

logging.basicConfig(level=logging.INFO,
                    format='\033[93m[%(process)d-%(threadName)s-%(asctime)s-%(levelname)s]:\033[0m %(message)s (%(filename)s:%(lineno)s)')
logger = logging.getLogger(__name__)

REGISTERRED_DISPATCHERS = []

def register_dispatcher(dispatcher):
    if dispatcher not in REGISTERRED_DISPATCHERS:
        REGISTERRED_DISPATCHERS.append(dispatcher)

class EventFactory:
    dispatcher_class = None
    dispatcher_instance = None

    def __init__(self, dispatcher_class=None, dispatcher_instance=None):
        self.dispatcher_class = dispatcher_class
        self.dispatcher_instance = dispatcher_instance

    def bind_dispatcher_class(self, dispatcher_class):
        self.dispatcher_class = dispatcher_class
        return dispatcher_class

    @property
    def dispatcher(self):
        if self.dispatcher_instance:
            return self.dispatcher_instance
        if not self.dispatcher_instance and not self.dispatcher_class:
            return None
        self.dispatcher_instance = self.dispatcher_class()
        return self.dispatcher_instance

    def take(self, message):
        if not self.dispatcher_instance:
            raise RuntimeError('dispatcher is not found')
        return self.dispatcher.dispatch_event(message)

    def start(self):
        if self.dispatcher:
            return self.dispatcher.start()
        if not self.dispatcher_instance:
            raise RuntimeError('dispatcher is not found')

    def stop(self):
        if not self.dispatcher_instance:
            raise RuntimeError('dispatcher is not found')
        return self.dispatcher.stop()

    def unbind_dispacther_class(self):
        self.dispatcher_class = None
        self.dispatcher_instance = None

    @contextmanager
    def proxy(self):
        self.start()
        yield self
        self.stop()

class EventDispatcher:
    event_classes = {}
    handler_classes = {}
    event_handler_map = {}

    def inspect(self):
        pass

    @classmethod
    def add_subscription(cls, handler, event_name):
        cls.event_handler_map[event_name] = handler
        return cls

    def dispatch_event(self, msg):
        if isinstance(msg, Event):
            self.enqueue(event)
            return
        for event_name, event_class in self.event_classes.items():
            event = event_class.make_event(msg)
            if not event:
                continue
            self.enqueue(event)

    def start(self):
        pass

    def stop(self):
        pass

    def get_bound_handler(self, event_name):
        return self.event_handler_map.get(event_name)

    def enqueue(self, event):
        # import pdb; pdb.set_trace()
        if not event:
            return False
        handler = self.get_bound_handler(event.event_name)
        if handler:
            resp =  handler.handle_event(event) # TODO: handle resp
            return True
        # logger.warn('No handler instance is bound to {}'.format(event.event_name))
        resp =  event.dispatch_to_handler() # TODO: handle resp
        return True

    @classmethod
    def register_event(cls, event_class):
        cls.event_classes[event_class.event_name] = event_class
        return event_class

    @classmethod
    def register_handler(cls, handler_class):
        cls.handler_classes[handler_class.name] = handler_class
        return handler_class

    # TODO: Here init handler with *args, **kwargs later
    @classmethod
    def use_handler(cls, handler_name):
        def inner(event_class):
            handler_class = cls.handler_classes.get(handler_name, None)
            if not handler_class:
                return event_class
            event_class.handler_class = handler_class
            return event_class
        return inner

class ThreadingEventDispatcher(ThreadingMixin, EventDispatcher): pass
class ProcessingEventDispatcher(ProcessingMixin, EventDispatcher): pass
class ProcessingPoolEventDispatcher(ProcessingPoolMixin, EventDispatcher): pass

class Event:
    event_name = 'default'
    handler_class = None
    handler = None
    def __init__(self, msg):
        self.msg = msg
        if not self.handler:
            self.handler = self.handler_class() if self.handler_class else None

    def dispatch_to_handler(self):
        return self.handler.handle_event(self) if self.handler else None

    @property
    def raw(self):
        return self.msg

    @classmethod
    def related(cls, msg):
        raise NotImplemented('Should be implemented in derived class')

    @classmethod
    def make_event(cls, msg):
        if not cls.related(msg):
            return None
        return cls.make(msg)

    @classmethod
    def make(cls, msg):
        raise NotImplemented('Should be implemented in derived class')

class EventProxy:
    def __init__(self, event : Event, processed_cnt=0):
        self.event = event
        self.processed_cnt = processed_cnt

    @property
    def event_name(self):
        return self.event.event_name

    def next(self, event):
        if not event:
            return None
        return EventProxy(event, self.processed_cnt+1)

class EventHandler:
    name=''
    on_creation_context = {}

    def __init__(self):
        self.on_creation()

    def on_creation(self):
        if not self.on_creation_context:
            return
        on_creation_cb = self.on_creation_context.get(callback, None)
        on_creation_kwargs = self.on_creation_context.get(kwargs, {})
        if not on_creation_cb:
            return
        on_creation_cb(self, **on_creation_kwargs)

    def handle_event(self, event):
        raise NotImplemented('Should be implemented in derived class')


def register_event(event_class):
    for dispacther in REGISTERRED_DISPATCHERS:
        dispacther.event_classes[event_class.event_name] = event_class
    return event_class

def subscribe(event_handler, *events):
    for dispatcher in REGISTERRED_DISPATCHERS:
        for event in events:
            if issubclass(event, Event):
                event_name = event.event_name
            else:
                event_name = event
            dispatcher.add_subscription(event_handler, event_name)

def subscribe_config(config):
    for handler, events in config.items():
        if isinstance(events, typing.List) or isinstance(events, typing.Tuple):
            subscribe(handler, *events)
        else:
            subscribe(handler, events)

sync_dispatcher = EventDispatcher()
mt_dispatcher = ThreadingEventDispatcher()
mp_dispatcher = ProcessingEventDispatcher()
mpp_dispatcher = ProcessingPoolEventDispatcher()

register_dispatcher(sync_dispatcher)
register_dispatcher(mt_dispatcher)
register_dispatcher(mp_dispatcher)
register_dispatcher(mpp_dispatcher)

def build_factory(dispatcher=sync_dispatcher, workers=None):
    if isinstance(dispatcher, str):
        if dispatcher == 'mpp':
            dispatcher = mpp_dispatcher
        elif dispatcher == 'mp':
            dispatcher = mp_dispatcher
        elif dispatcher == 'mt':
            dispatcher = mt_dispatcher
        else:
            dispatcher = sync_dispatcher

    if hasattr(dispatcher, 'set_worker_cnt'):
        dispatcher.set_worker_cnt(workers)
    return EventFactory(dispatcher_instance=dispatcher)
