import inspect
from typing import Optional, Callable

import grpc
from google.protobuf import struct_pb2

from jina.helper import get_or_reuse_loop
from jina.proto import jina_pb2_grpc, jina_pb2
from jina.types.message import Message
from jina.types.message.common import ControlMessage
from jina.types.routing.table import RoutingTable

# TODO extract dynamic routing logic and merge with zmqlet
class Grpclet(jina_pb2_grpc.JinaDataRequestRPCServicer):
    """A `Grpclet` object can send/receive Messages via gRPC.

    :param async_loop: asyncio loop to use
    :param args: the parsed arguments from the CLI
    :param message_callback: the callback to call on received messages
    :param logger: the logger to use
    """

    def __init__(
        self,
        async_loop,
        args: 'argparse.Namespace',
        message_callback: Callable[['Message'], 'Message'],
        logger: Optional['JinaLogger'] = None,
    ):
        self.args = args
        self._stubs = {}
        self._logger = logger
        self.callback = message_callback
        self._loop = async_loop

    def _get_dynamic_next_routes(self, message):
        routing_table = RoutingTable(message.envelope.routing_table)
        next_targets = routing_table.get_next_targets()
        next_routes = []
        for target, send_as_bind in next_targets:
            pod_address = target.active_target_pod.full_address
            if send_as_bind:
                raise ValueError(
                    f'Grpclet can not send as bind to target {pod_address}'
                )

            next_routes.append(target)
        return next_routes

    async def send_message(self, msg: 'Message', **kwargs):
        """
        Sends a message via gRPC to the target indicated in the message's routing table
        :param msg: the protobuf message to send
        :param kwargs: Additional arguments.
        """
        routing_table = RoutingTable(msg.envelope.routing_table)
        next_targets = routing_table.get_next_targets()

        for target, send_as_bind in next_targets:
            pod_address = target.active_target_pod.full_address
            if send_as_bind:
                raise ValueError(
                    f'Grpclet can not send as bind to target {pod_address}'
                )

            new_message = await self._add_envelope(msg, target)

            if pod_address not in self._stubs:
                self._stubs[pod_address] = Grpclet._create_grpc_stub(pod_address)

            try:
                self._stubs[pod_address].Call(new_message)
            except grpc.RpcError as ex:
                self._logger.error('Sending data request via grpc failed', ex)
                raise ex

    @staticmethod
    def send_ctrl_msg(pod_address: str, command: str):
        """
        Sends a control message via gRPC to pod_address
        :param pod_address: the pod to send the command to
        :param command: the command to send (TERMINATE/ACTIVATE/...)
        :returns: Empty protobuf struct
        """
        stub = Grpclet._create_grpc_stub(pod_address, is_async=False)
        response = stub.Call(ControlMessage(command))
        return response

    @staticmethod
    def _create_grpc_stub(pod_address, is_async=True):
        if is_async:
            channel = grpc.aio.insecure_channel(
                pod_address,
                options=[
                    ('grpc.max_send_message_length', -1),
                    ('grpc.max_receive_message_length', -1),
                ],
            )
        else:
            channel = grpc.insecure_channel(
                pod_address,
                options=[
                    ('grpc.max_send_message_length', -1),
                    ('grpc.max_receive_message_length', -1),
                ],
            )

        stub = jina_pb2_grpc.JinaDataRequestRPCStub(channel)

        return stub

    async def _add_envelope(self, msg, routing_table):
        new_envelope = jina_pb2.EnvelopeProto()
        new_envelope.CopyFrom(msg.envelope)
        new_envelope.routing_table.CopyFrom(routing_table.proto)
        new_message = Message(request=msg.request, envelope=new_envelope)

        return new_message

    async def close(self, *args, **kwargs):
        """Stop the Grpc server
        :param args: Extra positional arguments
        :param kwargs: Extra key-value arguments
        """
        self._logger.debug('Close grpc server')
        await self._grpc_server.stop(grace=True)

    async def start(self):
        """
        Starts this Grpclet by starting its gRPC server
        """
        self._grpc_server = grpc.aio.server(
            options=[
                ('grpc.max_send_message_length', -1),
                ('grpc.max_receive_message_length', -1),
            ]
        )

        jina_pb2_grpc.add_JinaDataRequestRPCServicer_to_server(self, self._grpc_server)
        bind_addr = f'{self.args.host}:{self.args.port_in}'
        self._grpc_server.add_insecure_port(bind_addr)
        self._logger.debug(f'Binding gRPC server for data requests to {bind_addr}')
        await self._grpc_server.start()
        await self._grpc_server.wait_for_termination()

    async def Call(self, msg, *args):
        """Processes messages received by the GRPC server
        :param msg: The received message
        :param args: Extra positional arguments
        :return: Empty protobuf struct, necessary to return for protobuf Empty
        """
        if self.callback:
            if inspect.iscoroutinefunction(self.callback):
                self._loop.create_task(self.callback(msg))
            else:
                self.callback(msg)
        else:
            self._logger.debug(
                'Grpclet received data request, but no callback was registered'
            )

        return struct_pb2.Value()
