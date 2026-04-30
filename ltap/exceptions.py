"""LTAP protocol exceptions."""


class LTAPError(Exception):
    """Base class for all LTAP errors."""


class ChannelError(LTAPError):
    """Error relating to channel operations."""


class ChannelNotFoundError(ChannelError):
    """Referenced channel does not exist."""


class ChannelAlreadyExistsError(ChannelError):
    """Channel with the given ID already exists."""


class RegistrationError(LTAPError):
    """Error during participant registration."""


class DuplicateRegistrationError(RegistrationError):
    """Participant is already registered on this channel."""


class ParticipantNotFoundError(LTAPError):
    """Referenced participant is not registered on the channel."""


class TransmissionError(LTAPError):
    """Error during transmission phase."""


class ArbiterError(LTAPError):
    """Internal arbiter error."""
