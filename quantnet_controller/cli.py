"""Console script for quantnet_controller."""
import sys
import click
import asyncio

from quantnet_controller.common.logging import setup_logging
from quantnet_controller.server import QuantnetServer as Quantnet
from quantnet_controller.common.config import Config
from quantnet_controller.core.abstractdatabase import AbstractDatabase as DB
from quantnet_controller.db.broker import SqlaBroker

STARTUP_FAILURE = 3

STOP = asyncio.Event()


def ask_exit(*args):
    STOP.set()


@click.command(help="Run a QUANT-NET Controller instance")
@click.option(
    "--mq-broker-host",
    "mq_broker_host",
    type=str,
    help="Specify the message queue broker host",
    show_default=True,
)
@click.option(
    "--mq-broker-port",
    "mq_broker_port",
    type=int,
    help="Specify the message queue broker port",
    show_default=True,
)
@click.option(
    "--mq-mongo-host",
    "mq_mongo_host",
    type=str,
    help="Specify a MongoDB host (if mongo configured)",
    show_default=True,
)
@click.option(
    "--mq-mongo-port",
    "mq_mongo_port",
    type=int,
    help="Specify a MongoDB port (if mongo configured)",
    show_default=True,
)
@click.option(
    "--plugin-path",
    "plugin_path",
    type=str,
    help="Specify a path containing controller plugins",
    show_default=True
)
@click.option(
    "--schema-path",
    "schema_path",
    type=str,
    help="Specify a path containing additional schema files",
    show_default=True
)
def main(
    mq_broker_host,
    mq_broker_port,
    mq_mongo_host,
    mq_mongo_port,
    plugin_path,
    schema_path
) -> None:
    run(
        mq_broker_host,
        mq_broker_port,
        mq_mongo_host,
        mq_mongo_port,
        plugin_path,
        schema_path
    )


def run(
    mq_broker_host,
    mq_broker_port,
    mq_mongo_host,
    mq_mongo_port,
    plugin_path,
    schema_path
) -> None:
    # Create config
    config = Config(
        mq_broker_host=mq_broker_host,
        mq_broker_port=mq_broker_port,
        mq_mongo_host=mq_mongo_host,
        mq_mongo_port=mq_mongo_port,
        plugin_path=plugin_path,
        schema_path=schema_path
    )

    setup_logging()

    db_broker = DB().get_broker()
    if isinstance(db_broker, SqlaBroker):
        print("Error: SQLAlchemy DB broker is not supported. Please use MongoDB.")
        sys.exit(STARTUP_FAILURE)

    # Create and start the controller
    quantnet = Quantnet(config)
    quantnet.run()

    # Exit if failed
    if not quantnet.started:
        sys.exit(STARTUP_FAILURE)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
