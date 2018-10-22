#!/usr/bin/env python3
"""
Interface on local PC to remote ARTIQ Master to start controllers.

Auto-starts any requested controllers, and restarts them if they fail.
"""

import asyncio
import atexit
import argparse
import os
import logging
import platform

from artiq.protocols.pc_rpc import Server
from artiq.protocols.logging import LogForwarder, SourceFilter
from artiq.tools import (
    simple_network_args,
    atexit_register_coroutine,
    bind_address_from_args,
    add_common_args,
)
from artiq.devices.ctlmgr import ControllerManager


def get_argparser() -> argparse.ArgumentParser:
    """Get an argument parser with all arguments for `artiq_ctlmgr`."""
    parser = argparse.ArgumentParser(
        description="ARTIQ controller manager. "
        "Launches any local controllers and exposes them to ARTIQ Master PC."
    )

    add_common_args(parser)

    parser.add_argument(
        "-s",
        "--server",
        default="::1",
        help="hostname or IP of the master to connect to (default: %(default)s)",
    )
    parser.add_argument(
        "--retry-master",
        default=5.0,
        type=float,
        help="retry timer for reconnecting to master (default: %(default)f)",
    )
    simple_network_args(
        parser,
        [
            ("control", "control", 3249),
            ("notify", "notification", 3250),
            ("logging", "logging", 1066),
        ],
    )
    return parser


def main():
    """Main function for `artiq_ctlmgr`."""
    args = get_argparser().parse_args()

    # Start Root logger. Send logs to console.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.NOTSET)
    source_adder = SourceFilter(
        logging.WARNING + args.quiet * 10 - args.verbose * 10,
        "ctlmgr({})".format(platform.node()),
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(levelname)s:%(source)s:%(name)s:%(message)s")
    )
    console_handler.addFilter(source_adder)
    root_logger.addHandler(console_handler)

    if os.name == "nt":
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()
    atexit.register(loop.close)

    # Set up log forwarding to remote PC/master
    logfwd = LogForwarder(args.server, args.port_logging, args.retry_master)
    logfwd.addFilter(source_adder)
    root_logger.addHandler(logfwd)
    logfwd.start()
    atexit_register_coroutine(logfwd.stop)

    # Start a Controller Manager. Waits to start devices until requested by Master.
    ctlmgr = ControllerManager(args.server, args.port_notify, args.retry_master)
    ctlmgr.start()
    atexit_register_coroutine(ctlmgr.stop)

    class CtlMgrRPC:
        retry_now = ctlmgr.retry_now

    # Allow remote retry_now procedure calls to the controller manager.
    rpc_target = CtlMgrRPC()
    rpc_server = Server({"ctlmgr": rpc_target}, builtin_terminate=True)
    loop.run_until_complete(
        rpc_server.start(bind_address_from_args(args), args.port_control)
    )
    atexit_register_coroutine(rpc_server.stop)

    loop.run_until_complete(rpc_server.wait_terminate())


if __name__ == "__main__":
    main()
