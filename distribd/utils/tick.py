import asyncio


class Tick:
    """
    A thin wrapper around loop.call_at.

    This is idempotemt, works in ns and can be updated with creating a new handle.
    """

    # How many ns are in 1s
    SCALING = 1000000000

    def __init__(self, callback):
        self._when = 0
        self._handle = None
        self._callback = callback

    def reschedule(self, when):
        """
        If when has changed, cancel the current timer handle and reschedule.

        Idempotent. Safe to call multiple times.
        """
        if self._when == when:
            return

        self.cancel()

        self._when = when

        if when > 0:
            loop = asyncio.get_event_loop()
            self._handle = loop.call_at(when / self.SCALING, self._callback)

    def cancel(self):
        """
        Cancel any pending tick.

        Idempotent. Safe to call multiple times.
        """
        if self._handle:
            self._handle.cancel()
            self._handle = None
