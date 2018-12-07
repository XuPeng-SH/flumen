import logging
from flumen.event_base import Event, EventHandler, register_event

logger = logging.getLogger(__name__)

class DefaultEventHandler(EventHandler):
    name='default_handler'
    def handle_event(self, event):
        logger.info(f'{self.name} is handling event {event.event_name}: {event.raw}')

@register_event
class VoteEvent(Event):
    event_name = 'vote'
    # handler_class = DefaultEventHandler

    @classmethod
    def related(cls, msg):
        if not msg.startswith('vote'):
            return False
        return True

    @classmethod
    def make(cls, msg):
        return cls(msg)

@register_event
class RegisterEvent(Event):
    event_name = 'register'
    # handler_class = DefaultEventHandler
    @classmethod
    def related(cls, msg):
        if not msg.startswith('register'):
            return False
        return True

    @classmethod
    def make(cls, msg):
        return cls(msg)

class DemoHandler(EventHandler):
    def handle_event(self, event):
        logger.info(f'Always say hello to: {event.raw}')

if __name__ == '__main__':
    from flumen.event_base import subscribe, subscribe_config

    handler1 = DefaultEventHandler()
    handler2 = DemoHandler()
    config = {
        handler2: [VoteEvent, RegisterEvent]
    }
    subscribe_config(config)

    from flumen.event_base import build_factory
    # factory = build_factory(dispatcher='mpp', workers=4)
    factory = build_factory()
    with factory.proxy() as consumer:
        for idx in range(20):
            consumer.take('vote: {}'.format(idx))
            consumer.take('register: {}'.format(idx))
