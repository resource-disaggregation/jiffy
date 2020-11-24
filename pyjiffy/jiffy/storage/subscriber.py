import logging
import threading

from jiffy.storage import block_request_service, block_response_service

try:
    import Queue as queue
except ImportError:
    import queue

from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket


class Notification:
    def __init__(self, op, data):
        self.op = op
        self.data = data

    def __eq__(self, other):
        return self.op == other.op and self.data == other.data

    def __str__(self):
        return "{op=%s, data=%s}" % (self.op, self.data)


class ControlMessage:
    def __init__(self, ctype, rtype, data):
        self.ctype = ctype
        self.rtype = rtype
        self.data = data

    def __eq__(self, other):
        return self.ctype == other.ctype and self.rtype == other.rtype and self.data == other.data


class Mailbox:
    def __init__(self):
        self.q = queue.Queue()

    def __call__(self, notification):
        self.q.put(notification)

    def pop(self, block=True, timeout=None):
        ret = self.q.get(block, timeout)
        return ret

    def empty(self):
        return self.q.empty()


class SubscriptionServiceHandler(block_response_service.Iface):
    def __init__(self, notification_callback, success_callback, failure_callback):
        self.notification_callback = notification_callback
        self.success_callback = success_callback
        self.error_callback = failure_callback

    def register_callback(self, callback):
        self.notification_callback = callback

    def notification(self, op, data):
        self.notification_callback(Notification(op, data))

    def control(self, rtype, ops, error):
        if error == "":
            self.success_callback(ControlMessage('success', rtype, ops))
        else:
            self.error_callback(ControlMessage('error', rtype, ops))


class SubscriptionWorker(threading.Thread):
    def __init__(self, protocols, notification_cb, control_cb):
        super(SubscriptionWorker, self).__init__()
        self.protocols = protocols
        self.handler = SubscriptionServiceHandler(notification_cb, control_cb, control_cb)
        self.processor = block_response_service.Processor(self.handler)
        self._stop_event = threading.Event()
        self.daemon = True

    def run(self):
        try:
            while not self.stopped():
                for protocol in self.protocols:
                    self.processor.process(protocol, protocol)
        except Exception:
            logging.info("Thrift transport closed")
        finally:
            self.stop()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class SubscriptionClient:
    def __init__(self, data_status, callback=Mailbox()):
        self.block_names = [block_chain.block_ids[-1].split(':')[:4] for block_chain in data_status.data_blocks]
        self.block_ids = [int(b[-1]) for b in self.block_names]
        self.transports = [TTransport.TFramedTransport(TSocket.TSocket(b[0], int(b[1]))) for b in self.block_names]
        self.protocols = [TBinaryProtocol.TBinaryProtocolAccelerated(transport) for transport in self.transports]
        self.clients = [block_request_service.Client(protocol) for protocol in self.protocols]
        for transport in self.transports:
            transport.open()
        self.notifications = callback
        self.controls = Mailbox()
        self.worker = SubscriptionWorker(self.protocols, self.notifications, self.controls)
        self.worker.start()

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        self.worker.stop()
        for transport in self.transports:
            if transport.isOpen():
                transport.close()

    def subscribe(self, ops):
        for block_id, client in zip(self.block_ids, self.clients):
            client.subscribe(block_id, ops)
        try:
            for i in range(len(self.block_ids)):
                response = self.controls.pop(True, 10)
                if response.ctype == 'error':
                    raise RuntimeError(response.data)
        except queue.Empty:
            raise RuntimeError('One or more storage servers did not respond to subscription request')

        return self

    def unsubscribe(self, ops):
        for block_id, client in zip(self.block_ids, self.clients):
            client.unsubscribe(block_id, ops)
        try:
            for i in range(len(self.block_ids)):
                response = self.controls.pop(True, 10)
                if response.ctype == 'error':
                    raise RuntimeError(response.data)
        except queue.Empty:
            raise RuntimeError('One or more storage servers did not respond to subscription request')

    def get_notification(self, block=True, timeout=None):
        if hasattr(self.notifications, 'pop'):
            n = self.notifications.pop(block, timeout)
            return n
        else:
            return None

    def has_notification(self):
        if hasattr(self.notifications, 'empty'):
            return not self.notifications.empty()
        return None
