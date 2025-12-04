import logging
# import asyncio


logger = logging.getLogger(__name__)


class TopicHandler:
    def __init__(self):
        """
        topic handlers
            (topic name, handle function)
        """
        self._topic_handlers = [
            ("broadcast", self.handle_broadcast, None),
            ("keepalive", self.handle_keepalive, None),
            ("monitoring", self.handle_monitoring, None),
        ]

    @property
    def topichandlers(self):
        return self._topic_handlers

    def handle_broadcast(self, request):
        """handle broadcast topic"""
        logger.debug(f"Received Broadcast: {request}")

        """ TODO: business logic """
        pass

    def handle_keepalive(self, request):
        """handle keepalive topic"""
        logger.debug(f"Received Keepalive: {request}")

        """ TODO: business logic """
        pass

    def handle_monitoring(self, request):
        """handle monitoring topic"""
        logger.debug(f"Received Monitoring: {request}")

        """ TODO: business logic """
        pass
