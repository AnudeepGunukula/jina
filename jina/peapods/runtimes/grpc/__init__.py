from abc import ABC

from jina.helper import get_or_reuse_loop
from jina.peapods.grpc import Grpclet
from jina.peapods.runtimes.base import BaseRuntime


class GRPCRuntime(BaseRuntime, ABC):
    """Runtime procedure leveraging :class:`Grpclet` for sending DataRequests"""

    def __init__(self, args: 'argparse.Namespace', logger: 'JinaLogger', **kwargs):
        """Initialize grpc and data request handling.
        :param args: args from CLI
        :param logger: the logger to use
        :param kwargs: extra keyword arguments
        """
        super().__init__(args, **kwargs)
        self._loop = get_or_reuse_loop()
        self._data_request_handler = DataRequestHandler(args, logger)
        self._grpclet = Grpclet(
            args=self.args, message_callback=self._callback, logger=self.logger
        )

    def run_forever(self):
        """Start the `Grpclet`."""
        self._loop.run_until_complete(self._grpclet.start())

    def teardown(self):
        """Close the `Grpclet` and `DataRequestHandler`."""
        self._grpclet.close()
        self._data_request_handler.close()
        self._loop.close()
        super().teardown()

    def _callback(self, msg: 'Message') -> None:
        # TODO implement the message handling
        pass
