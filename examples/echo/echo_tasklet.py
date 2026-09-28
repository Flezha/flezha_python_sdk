import flezha


# A processing stage of the operator's graph, run once per message. This one
# prefixes the input and emits it through the context, the one way a value
# leaves a tasklet.
#
# `Ctx[None, str]` names no state, so it keeps nothing between messages and
# journals nothing, and a str output. Where the output goes is not decided here:
# the operator decides that, so the graph can be rewired without touching this
# class.
@flezha.tasklet(name="echo")  # the durable identity: state is filed under it
class Echo:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(f"echo: {value}")
