try:
    from .scheduler import JobScheduler

    SCHEDULER_AVAILABLE = True
except ImportError:
    JobScheduler = None
    SCHEDULER_AVAILABLE = False

__all__ = ["JobScheduler", "SCHEDULER_AVAILABLE"]
