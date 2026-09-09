"""Owner-thread accounting for sockets and promised query stream slots, not model budgets."""

from dataclasses import dataclass, field


@dataclass
class QuerySlots:
    count: int
    control: int
    members: set = field(default_factory=set)
    closed: bool = False


class ConnectionCapacity:
    def __init__(self, maximum):
        self.maximum = maximum
        self.handshakes = set()
        self.standalone = set()
        self.queries = {}

    @property
    def promised(self):
        return len(self.standalone) + sum(q.count for q in self.queries.values())

    def may_accept(self):
        # Keep one classifier available when queries have unopened reserved streams.
        return not self.handshakes

    def accept(self, connection):
        self.handshakes.add(connection)

    def check_query(self, flows):
        if self.promised + flows + 1 > self.maximum - 1:
            raise ValueError("query needs its streams plus a handshake slot")

    def reserve_query(self, connection, job, flows):
        self.check_query(flows)
        self.queries[job] = QuerySlots(flows + 1, connection, {connection})
        self.handshakes.discard(connection)

    def join(self, connection, job):
        query = self.queries[job]
        if query.closed or len(query.members) >= query.count:
            raise ValueError("query connection reservation unavailable")
        query.members.add(connection)
        self.handshakes.discard(connection)

    def admit_standalone(self, connection):
        if connection in self.standalone:
            return
        ceiling = self.maximum - bool(self.queries)
        if self.promised >= ceiling:
            raise ValueError("standalone would consume promised query slots")
        self.standalone.add(connection)
        self.handshakes.discard(connection)

    def release(self, connection):
        self.handshakes.discard(connection)
        self.standalone.discard(connection)
        for job, query in tuple(self.queries.items()):
            if query.control == connection:
                query.closed = True
            query.members.discard(connection)
            if query.closed and not query.members:
                del self.queries[job]
