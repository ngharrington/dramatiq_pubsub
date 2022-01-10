from dramatiq.logging import get_logger
from google.api_core import exceptions
from google.cloud.pubsub import PublisherClient, SubscriberClient
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound as SubscriptionNotFound
import dramatiq

from typing import Optional, List, Any, Dict, Callable, Iterable
from collections import deque
import base64
from concurrent import futures

from pkg_resources import require

MAX_PREFETCH = 10

def get_pubsub_publisher_client():
    return PublisherClient()

def get_pubsub_subscription_client():
    return SubscriberClient()

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

class PubSubBroker(dramatiq.Broker):

    def __init__(
        self, *,
        topic: str,  # fully qualified topic path projects/<project>/topics/<topic>
        middleware: Optional[List[dramatiq.Middleware]] = None,
        retention: int = None
    ) -> None:
        super().__init__(middleware=middleware)

        self.topic: str = topic
        self.retention: str = str(retention)
        self._project: str = self.topic.split("/")[1]
        self._topic: str = self.topic.split("/")[-1]
        self.queues: Dict[str, Any] = {}

    def _sub_exists_for_queue(self, queue_name, client):
        sub_path = self._get_subscription_path(queue_name, client)
        try:
            client.get_subscription(subscription=sub_path)
            exists = True
        except SubscriptionNotFound:
            exists = False
        return exists

    def _create_sub_for_queue(self, queue_name, client):
        sub_path = self._get_subscription_path(queue_name, client)
        filter = f'attributes._queue="{queue_name}"'
        client.create_subscription(request={"name": sub_path, "topic": self.topic, "filter": filter})
        return sub_path

    def _get_sub_for_queue(self, queue_name):
        client = get_pubsub_subscription_client()
        if not self._sub_exists_for_queue(queue_name, client):
            self._create_sub_for_queue(queue_name, client)
        return self._get_subscription_path(queue_name, client)
        

    def _get_subscription_path(self, queue_name, client):
        subname = f"{self._topic}-{queue_name}"
        sub_path = client.subscription_path(self._project, subname)
        return sub_path

    def declare_queue(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            self.emit_before("declare_queue", queue_name)
            subscription = self._get_sub_for_queue(queue_name)
            self.queues[queue_name] = subscription
        self.emit_after("declare_queue", queue_name)

    def enqueue(self, message: dramatiq.Message, *, delay=None) -> dramatiq.Message:
        if delay:
            raise ValueError("PubSub Broker does not support delays.")
        queue_name = message.queue_name
        queue = self.queues[queue_name]
        encoded_message = base64.b64encode(message.encode())
        self.logger.debug("Enqueueing message %r on queue %r.", message.message_id, queue_name)
        self.emit_before("enqueue", message, delay)
        pubsub = get_pubsub_publisher_client()
        publish_future = pubsub.publish(self.topic, encoded_message, _queue=queue_name)
        publish_future.add_done_callback(get_callback(publish_future, message))
        futures.wait([publish_future], return_when=futures.ALL_COMPLETED)
        return message

    def get_declared_queues(self) -> Iterable[str]:
        return set(self.queues)

    def consume(self, queue_name: str, prefetch: int=1, timeout: int=30000) -> dramatiq.Consumer:
        try:
            return _PubSubConsumer(self.queues[queue_name], prefetch, timeout)
        except KeyError:
            raise dramatiq.QueueNotFound(queue_name)



class _PubSubConsumer(dramatiq.Consumer):

    def __init__(self, queue: Any, prefetch: int, timeout: int) -> None:
        self.logger = get_logger(__name__, type(self))
        self.queue = queue
        self.prefetch = min(prefetch, MAX_PREFETCH)
        self.timeout = timeout
        self.messages: deque = deque()
        self.message_refc = 0


    def __next__(self) -> Optional[dramatiq.Message]:
        client = pubsub_v1.SubscriberClient()
        with client:
            try:
                return self.messages.popleft()
            except IndexError:
                if self.message_refc < self.prefetch:
                    response = client.pull(request={"subscription": self.queue, "max_messages": self.prefetch})
                    for pubsub_message in response.received_messages:
                        try:
                            encoded_message = base64.b64decode(pubsub_message.message.data)
                            dramatiq_message = dramatiq.Message.decode(encoded_message)
                            self.messages.append(_PubSubMessage(pubsub_message, dramatiq_message))
                            self.message_refc += 1
                        except Exception:  # pragma: no cover
                            self.logger.exception("Failed to decode message: %r", pubsub_message.message.data)
    
    def ack(self, message: "_PubSubMessage"):
        client = pubsub_v1.SubscriberClient()
        client.acknowledge(request={"subscription": self.queue, "ack_ids": [message.ack_id]})

    # don't handle failures yet
    nack = ack




class _PubSubMessage(dramatiq.MessageProxy):
    def __init__(self, pubsub_message: Any, message: dramatiq.Message) -> None:
        super().__init__(message)
        self._pubsub_message = pubsub_message

    @property
    def ack_id(self):
        return self._pubsub_message.ack_id


if __name__ == "__main__":
    broker = PubSubBroker(topic="projects/ngh-sanbox/topics/test-topic")
    info = broker._get_sub_for_queue("queue")
