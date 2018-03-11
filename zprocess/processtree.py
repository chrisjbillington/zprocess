# Only the top-level process has a Broker for event passing, which will
# be instantiated when it first makes a subprocess. For the moment we
# assume we are the top-level process, and will set this to False if we
# discover that wer're not (by setup_connection_with_parent() being called):
we_are_the_top_process = True
