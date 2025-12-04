from quantnet_controller.common.experimentdefinitions import Sequence, AgentSequences, Experiment
from datetime import timedelta


class QnodeEGP(Sequence):
    name = "experiments/single_photon_calibration.py"
    class_name = "SinglePhotonGeneration"
    duration = timedelta(microseconds=10000)
    dependency = []


class EGPQnodeSequence(AgentSequences):
    name = "Entanglement Generation sequence for Qnode"
    node_type = "QNode"
    sequences = [QnodeEGP]


class SimpleExperiment(Experiment):
    name = "Simple Experiment"
    agent_sequences = [EGPQnodeSequence]
