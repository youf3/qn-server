from quantnet_controller.common.experimentdefinitions import Sequence, AgentSequences, Experiment
from datetime import timedelta


class CalibrationSrcInit(Sequence):
    name = "calibration.srcInit"
    class_name = "CalibrationSrcInit"
    duration = timedelta(seconds=1)  # Based on timeout in calibrator.py
    dependency = []


class CalibrationGeneration(Sequence):
    name = "calibration.generation"
    class_name = "CalibrationGeneration"
    duration = timedelta(seconds=1)
    dependency = []


class CalibrationSrcCleanup(Sequence):
    name = "calibration.cleanUp"
    class_name = "CalibrationCleanup"
    duration = timedelta(seconds=1)
    dependency = []


class CalibrationSrcSequence(AgentSequences):
    name = "Calibration Source Sequence"
    node_type = "BSMNode"
    sequences = [CalibrationSrcInit, CalibrationGeneration, CalibrationSrcCleanup]


# Similar for destination
class CalibrationDstInit(Sequence):
    name = "calibration.dstInit"
    class_name = "CalibrationDstInit"
    duration = timedelta(seconds=1)
    dependency = []


class CalibrationCalibration(Sequence):
    name = "calibration.calibration"
    class_name = "CalibrationCalibration"
    duration = timedelta(seconds=1)
    dependency = []


class CalibrationDstCleanup(Sequence):
    name = "calibration.cleanUp"
    class_name = "CalibrationCleanup"
    duration = timedelta(seconds=1)
    dependency = []


class CalibrationDstSequence(AgentSequences):
    name = "Calibration Destination Sequence"
    node_type = "BSMNode"
    sequences = [CalibrationDstInit, CalibrationCalibration, CalibrationDstCleanup]


class CalibrationExperiment(Experiment):
    name = "Calibration"
    agent_sequences = [CalibrationSrcSequence, CalibrationDstSequence]

    def get_sequence(self, agent_index):
        return self.agent_sequences[agent_index]
