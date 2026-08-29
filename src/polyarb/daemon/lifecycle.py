"""Process-level shutdown budgets shared by daemon entrypoints.

Fly owns the outer termination window. Daemons must finish their cooperative
drain before that window ends so the platform retains time to reap the VM and
flush logs. Keep these values as one policy pair instead of scattering local
``wait_for`` literals through daemon implementations.
"""

PLATFORM_TERMINATION_WINDOW_SECONDS = 40.0
DAEMON_TASK_DRAIN_BUDGET_SECONDS = 30.0

if not 0 < DAEMON_TASK_DRAIN_BUDGET_SECONDS < PLATFORM_TERMINATION_WINDOW_SECONDS:
    raise RuntimeError("daemon drain budget must fit inside the platform termination window")
