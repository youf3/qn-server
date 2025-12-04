# QUANT-NET Server (Controller)

![PyPI Version](https://img.shields.io/pypi/v/quantnet_controller.svg)
[![Documentation Status](https://readthedocs.org/projects/quantnet-controller/badge/?version=latest)](https://quantnet-controller.readthedocs.io/en/latest/?version=latest)

The software implements the QUANT-NET Control Plane (QNCP) Server/Controller. A centralized Controller instance
coordinates with multiple distributed Agents that communicate using a shared Message Bus.

## Development Install

After downloading the source tree, pull requirements and install package in edit mode:

```
pip3 install -e .
```

The quantnet_controller script will be available in your local path.

```
$ quantnet_controller --help
Usage: quantnet_controller [OPTIONS]

  Quantnet Controller

Options:
  --mq-broker-host TEXT     Specify the message queue broker host
  --mq-broker-port INTEGER  Specify the message queue broker port
  --mq-mongo-host TEXT      Specify a MongoDB host (if mongo configured)
  --mq-mongo-port INTEGER   Specify a MongoDB port (if mongo configured)
  --plugin-path TEXT        Specify a path containing controller plugins
  --schema-path TEXT        Specify a path containing additional schema files
  --help                    Show this message and exit.
```


## Configuration File

The controller will start with default configurations if no configuration file is specified.
A configuration file customizes the Controller behavior, and examples may be found in the
`config/` folder.

## Example Usage


An MQTT broker should be available for the agent to connect to. Docker compose configurations are available in the `quant-net/qn-docker` repository.



## Running the controller:

```
quantnet_controller --mq-broker-host <broker>
```


## How to add a CI/CD tests
Please add the test python scripts to directory:
```bash
regression_tests/scripts
```
The directory will be auto-listed by the workflow to run.
Example: regression_tests/scripts/ping.py is example to start
