from echo_tasklet import echo

import flezha


# The deployable unit. An operator declares the graph a message travels: the
# entry channel it arrives on, the steps it passes through, and the channel its
# result leaves on.
#
# The graph is checked as a whole when the runner declares it, so one that
# cannot work is refused at startup. The entry names the text codec, so the
# tasklet is handed a str, and the sink names none, so a str result leaves
# as its own bytes.
@flezha.unbounded_operator("echo")  # the stable identity `flz operator start` takes
def echo_operator(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(echo).sink("out")
