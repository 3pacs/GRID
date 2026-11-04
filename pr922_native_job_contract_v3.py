"""DRAFT real-driver contract, called only by disabled job supervisor.

Not executed during preparation. No automatic reconnect or replay contract.
All endpoints and credentials are created and supplied internally by that
supervisor. This module is not an independently admitted execution entrypoint.
"""
import asyncio
import json
from urllib.parse import quote

_cleanup_retained = set()


def _cleanup_done(task):
    _cleanup_retained.discard(task)
    if not task.cancelled():
        task.exception()


async def bounded_cleanup(steps, timeout):
    """Attempt every cleanup independently even if a task resists cancellation.

    Timed-out tasks remain strongly referenced and yield UNKNOWN immediately.
    The enclosing worker process has its own hard deadline: asyncio.run may wait
    forever for an uncooperative task during shutdown. No exception text is used.
    """
    results = []
    for label, op in steps:
        async def invoke(operation=op):
            await operation()
        task = asyncio.create_task(invoke())
        _cleanup_retained.add(task)
        task.add_done_callback(_cleanup_done)
        try:
            done, _ = await asyncio.wait({task}, timeout=timeout)
            if not done:
                task.cancel()
                results.append((label, "UNKNOWN"))
            elif task.cancelled() or task.exception() is not None:
                results.append((label, "UNKNOWN"))
            else:
                results.append((label, "PASS"))
        except BaseException:
            task.cancel()
            results.append((label, "UNKNOWN"))
    return results


async def run_contract(ctx):
    import asyncpg
    from events.bus import EventBus

    if asyncpg.__version__ != "0.32.0":
        raise RuntimeError("wrong driver version")
    # Attestation compares the current kernel process/socket identity with the
    # supervisor's captured instance before any client opens a connection.
    ctx.attest()
    kw = dict(host="127.0.0.1", port=ctx.port, user=ctx.role,
              password=ctx.password, database=ctx.database, ssl=False,
              timeout=4, command_timeout=4)
    dsn = ("postgresql://" + quote(ctx.role, safe="") + ":" +
           quote(ctx.password, safe="") + "@127.0.0.1:" + str(ctx.port) +
           "/" + quote(ctx.database, safe="") + "?application_name=" + ctx.app)
    buses = [EventBus(), EventBus()]
    connections = []
    queues = [asyncio.Queue(), asyncio.Queue()]
    local = []
    channel = "grid922_" + ctx.token
    other = "grid922_other_" + ctx.token
    for i, bus in enumerate(buses):
        bus.subscribe(channel, queues[i].put_nowait, remote=True)
    buses[0].subscribe(channel, local.append, remote=False)

    async def timed(coro, limit=4):
        return await asyncio.wait_for(coro, limit)

    async def assert_quiet(q):
        try:
            await asyncio.wait_for(q.get(), 0.5)
        except asyncio.TimeoutError:
            return
        raise AssertionError("unexpected notification")

    async def receive(q, expected):
        event = await timed(q.get())
        if event.channel != channel or event.payload != {"id": expected}:
            raise AssertionError("unexpected event")

    async def validate(conn):
        row = await timed(conn.fetchrow(
            "SELECT current_user u, current_database() d, inet_server_addr()::text a, "
            "inet_server_port() p, pg_postmaster_start_time()::text s, pg_backend_pid() pid, "
            "r.rolsuper su, r.rolcreatedb cd, r.rolcreaterole cr, r.rolreplication rp "
            "FROM pg_roles r WHERE rolname=current_user"))
        if (row["u"] != ctx.role or row["d"] != ctx.database or
            row["a"] not in ("127.0.0.1", "127.0.0.1/32") or row["p"] != ctx.port or
            any(row[x] for x in ("su", "cd", "cr", "rp"))):
            raise RuntimeError("invalid client identity")
        if ctx.started is None:
            ctx.started = row["s"]
        if row["s"] != ctx.started:
            raise RuntimeError("server changed")
        return row["pid"]

    async def notify(sender, name, value):
        await timed(sender.execute("SELECT pg_notify($1, $2)", name, json.dumps({"id": value})))

    # Cleanup operations are independent and bounded; no secrets or exception
    # strings are recorded. A close failure is UNKNOWN, never a passing gate.
    async def close_all():
        cleanup = await bounded_cleanup(
            [("bus0", buses[0].stop), ("bus1", buses[1].stop)] + [
                ("conn" + str(i), c.close) for i, c in enumerate(connections)], 3)
        # EventBus.stop swallows its own exceptions: verify saved references.
        if any(not c.is_closed() for c in connections):
            cleanup.append(("connection_state", "UNKNOWN"))
        return cleanup

    try:
        try:
            unexpected = await timed(asyncpg.connect(**dict(kw, password=ctx.password + "x")))
        except asyncpg.InvalidPasswordError:
            pass
        else:
            connections.append(unexpected)
            raise AssertionError("wrong password authenticated")
        ctx.attest()
        sender = await timed(asyncpg.connect(**kw)); connections.append(sender)
        sender_pid = await validate(sender)
        for bus in buses:
            ctx.attest()
            await timed(bus.start(dsn, [channel, channel]))
            if not bus.listening or bus._pg_conn is None:
                raise RuntimeError("listener did not start")
            connections.append(bus._pg_conn)
        listener_pids = [await validate(bus._pg_conn) for bus in buses]
        if len(set([sender_pid] + listener_pids)) != 3:
            raise RuntimeError("connections are not independent")
        await notify(sender, channel, "external")
        await receive(queues[0], "external"); await receive(queues[1], "external")
        if local:
            raise AssertionError("remote reached local-only subscriber")
        await notify(sender, other, "ignored")
        await assert_quiet(queues[0]); await assert_quiet(queues[1])
        await timed(buses[0].emit(channel, {"id": "emitted"}))
        await receive(queues[0], "emitted"); await receive(queues[0], "emitted")
        await receive(queues[1], "emitted")
        if len(local) != 1 or local[0].payload != {"id": "emitted"}:
            raise AssertionError("emit local fanout")
        await assert_quiet(queues[0]); await assert_quiet(queues[1])
        previous = buses[0]._pg_conn
        await timed(buses[0].stop())
        if buses[0].listening or not previous.is_closed():
            raise AssertionError("stop did not close")
        await notify(sender, channel, "while-stopped")
        await receive(queues[1], "while-stopped")
        await assert_quiet(queues[0])
        ctx.attest()
        await timed(buses[0].start(dsn, [channel]))
        if not buses[0].listening or buses[0]._pg_conn is previous:
            raise AssertionError("restart did not create listener")
        lost = buses[0]._pg_conn; connections.append(lost)
        lost_pid = await validate(lost)
        if lost_pid in (sender_pid, listener_pids[1]):
            raise RuntimeError("invalid listener target")
        await assert_quiet(queues[0])
        await notify(sender, channel, "after-stop-restart")
        await receive(queues[0], "after-stop-restart")
        await receive(queues[1], "after-stop-restart")
        # Controller obtains privileged connection via the private socket;
        # only this job-created synthetic cluster may be controlled.
        ctx.attest()
        admin = await timed(asyncpg.connect(host=ctx.socket_dir, port=ctx.port, user=ctx.admin,
                                           database="postgres", timeout=4, command_timeout=4))
        connections.append(admin)
        if await timed(admin.fetchval("SELECT pg_postmaster_start_time()::text")) != ctx.started:
            raise RuntimeError("control connected to another cluster")
        row = await timed(admin.fetchrow(
            "SELECT pid, backend_start::text bs FROM pg_stat_activity "
            "WHERE pid=$1 AND usename=$2 AND datname=$3 AND application_name=$4",
            lost_pid, ctx.role, ctx.database, ctx.app))
        if not row:
            raise RuntimeError("listener identity missing")
        ctx.attest()
        terminated = await timed(admin.fetchval(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE pid=$1 AND usename=$2 AND datname=$3 AND application_name=$4 "
            "AND backend_start::text=$5 AND pid<>pg_backend_pid()",
            lost_pid, ctx.role, ctx.database, ctx.app, row["bs"]))
        if terminated is not True:
            raise RuntimeError("listener termination uncertain")
        ctx.attest()
        async def wait_closed():
            while not lost.is_closed():
                await asyncio.sleep(0.05)
        await timed(wait_closed())
        # Record stale flag as observation, not a claim of regression.
        ctx.observed_stale_listening = buses[0].listening
        await timed(buses[0].stop())
        await timed(buses[0].start(dsn, [channel]))
        if not buses[0].listening or buses[0]._pg_conn is lost:
            raise AssertionError("post-loss restart failed")
        connections.append(buses[0]._pg_conn)
        if await validate(buses[0]._pg_conn) == lost_pid:
            raise RuntimeError("reused listener PID: uncertain")
        await notify(sender, channel, "after-loss-restart")
        await receive(queues[0], "after-loss-restart")
        await receive(queues[1], "after-loss-restart")
        ctx.contract_pass = True
    finally:
        ctx.client_cleanup = await close_all()
