"""sluice - an idempotent TRC-20 withdrawal processor.

The only promise that matters: money leaves the hot wallet at most once
per operation, no matter how many workers race for it or where they die.
"""

__version__ = "0.3.1"
