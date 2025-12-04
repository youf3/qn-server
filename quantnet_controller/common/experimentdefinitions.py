from abc import ABCMeta, abstractmethod
from quantnet_controller.common.constants import Constants
from datetime import timedelta
import math


class Sequence(metaclass=ABCMeta):

    @property
    @abstractmethod
    # Match with ARTIQ ExperimentInterface script name
    def name(self):
        pass

    @property
    @abstractmethod
    def duration(self) -> timedelta:
        pass

    @property
    @abstractmethod
    def dependency(self):
        pass


def get_num_timeslot(sequence: Sequence):
    num_slots = math.ceil(sequence.duration / Constants.SLOTSIZE)
    return num_slots


class AgentSequences(metaclass=ABCMeta):

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    # List of Sequence classes to be used in agent for an Experiment
    def sequences(self) -> list:
        pass


def get_timeslot_mask(sequences):
    mask = ''
    for sequence in sequences:
        mask += '1'*get_num_timeslot(sequence)
    return mask


class Experiment(metaclass=ABCMeta):

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    # List of AgentSequences classes to be used for an Experiment
    def agent_sequences(self):
        pass

    @abstractmethod
    # Return AgentSequence class for each agent
    def get_sequence(self, agent_index):
        pass
