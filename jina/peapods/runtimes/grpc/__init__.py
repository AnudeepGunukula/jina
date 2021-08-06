import time
from abc import ABC
from collections import defaultdict

from jina.enums import OnErrorStrategy
from jina.excepts import NoExplicitMessage, ChainedPodException, RuntimeTerminated
from jina.helper import get_or_reuse_loop, random_identity
from jina.peapods.grpc import Grpclet
from jina.peapods.runtimes.base import BaseRuntime
from jina.peapods.runtimes.request_handlers.data_request_handler import DataRequestHandler
from jina.proto import jina_pb2


# TODO unify pre/post hook logic
class GRPCRuntime(BaseRuntime, ABC):
    """Runtime procedure leveraging :class:`Grpclet` for sending DataRequests"""

    def __init__(self, args: 'argparse.Namespace', logger: 'JinaLogger', **kwargs):
        """Initialize grpc and data request handling.
        :param args: args from CLI
        :param logger: the logger to use
        :param kwargs: extra keyword arguments
        """
        super().__init__(args, **kwargs)
        self._id = random_identity()
        self._loop = get_or_reuse_loop()
        self._last_active_time = time.perf_counter()

        self._pending_msgs = defaultdict(list)  # type: Dict[str, List['Message']]
        self._partial_requests = None

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
        try:
            msg = self._post_hook(self._handle(self._pre_hook(msg)))
            self._grpclet.send_message(msg)
        except RuntimeTerminated:
            # this is the proper way to end when a terminate signal is sent
            self._grpclet.close()
        except KeyboardInterrupt as kbex:
            self.logger.debug(f'{kbex!r} causes the breaking from the event loop')
            self._grpclet.close()
        except (SystemError) as ex:
            # save executor
            self.logger.debug(f'{ex!r} causes the breaking from the event loop')
            self._grpclet.close()
        except NoExplicitMessage:
            # silent and do not propagate message anymore
            # 1. wait partial message to be finished
            pass
        except (RuntimeError, Exception, ChainedPodException) as ex:
            if self.args.on_error_strategy == OnErrorStrategy.THROW_EARLY:
                raise
            if isinstance(ex, ChainedPodException):
                # the error is print from previous pod, no need to show it again
                # hence just add exception and propagate further
                # please do NOT add logger.error here!
                msg.add_exception()
            else:
                msg.add_exception(ex, executor=self._data_request_handler._executor)
                self.logger.error(
                    f'{ex!r}'
                    + f'\n add "--quiet-error" to suppress the exception details'
                    if not self.args.quiet_error
                    else '',
                    exc_info=not self.args.quiet_error,
                )

            self._grpclet.send_message(msg)

    def _handle(self, msg: 'Message') -> 'Message':
        """Register the current message to this pea, so that all message-related properties are up-to-date, including
        :attr:`request`, :attr:`prev_requests`, :attr:`message`, :attr:`prev_messages`. And then call the executor to handle
        this message if its envelope's  status is not ERROR, else skip handling of message.
        .. note::
            Handle does not handle explicitly message because it may wait for different messages when different parts are expected
        :param msg: received message
        :return: the transformed message.
        """

        # skip executor for non-DataRequest
        if msg.envelope.request_type != 'DataRequest':
            self.logger.debug(f'skip executor: not data request')
            return msg

        req_id = msg.envelope.request_id
        num_expected_parts = self._expect_parts(msg)
        self._data_request_handler.handle(
            msg=msg,
            partial_requests=[m.request for m in self._pending_msgs[req_id]]
            if num_expected_parts > 1
            else None,
            peapod_name=self.name,
        )

        return msg

    def _pre_hook(self, msg: 'Message') -> 'Message':
        """
        Pre-hook function, what to do after first receiving the message.
        :param msg: received message
        :return: `Message`
        """
        msg.add_route(self.name, self._id)

        expected_parts = self._expect_parts(msg)

        req_id = msg.envelope.request_id
        if expected_parts > 1:
            self._pending_msgs[req_id].append(msg)

        num_partial_requests = len(self._pending_msgs[req_id])

        if expected_parts > 1 and expected_parts > num_partial_requests:
            # NOTE: reduce priority is higher than chain exception
            # otherwise a reducer will lose its function when earlier pods raise exception
            raise NoExplicitMessage


        if (
                msg.envelope.status.code == jina_pb2.StatusProto.ERROR
                and self.args.on_error_strategy >= OnErrorStrategy.SKIP_HANDLE
        ):
            raise ChainedPodException

        return msg

    def _post_hook(self, msg: 'Message') -> 'Message':
        """
        Post-hook function, what to do before handing out the message.
        :param msg: the transformed message
        :return: `ZEDRuntime`
        """
        # do NOT access `msg.request.*` in the _pre_hook, as it will trigger the deserialization
        # all meta information should be stored and accessed via `msg.envelope`

        self._last_active_time = time.perf_counter()

        if self._expect_parts(msg) > 1:
            msgs = self._pending_msgs.pop(msg.envelope.request_id)
            msg.merge_envelope_from(msgs)

        msg.update_timestamp()
        return msg
