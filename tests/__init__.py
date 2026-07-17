import logging

# The pipeline narrates timeouts and rebuilds at INFO/WARNING - useful in
# production, noise in a test run where those events are the whole point.
logging.getLogger("sluice").setLevel(logging.ERROR)
