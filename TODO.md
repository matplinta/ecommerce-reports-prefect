# todo
- [ ] composition over inheritance - decouple requests logic from ProviderClient classes 
- [ ] -> that will enable us to write unit tests, with feeding proper requests fixtures
- [ ] provide typing.Protocol for ProviderClients classes
  - [ ] maybe implement Aggregation service (combine multiple providers)
  ```
  class OrdersAggregator:
    def __init__(self, providers: list[OrdersProvider]):
        self.providers = providers

    def fetch_all(self, since=None, until=None) -> list[Order]:
        # Optionally parallelize with asyncio if providers are IO-bound.
        orders = []
        for p in self.providers:
            orders.extend(p.fetch_orders(since, until))
        return self._dedupe(orders)

    def _dedupe(self, orders: list[Order]) -> list[Order]:
        # Decide merge keys/precedence rules:
        # - If the two systems share a marketplace order number, use that.
        # - Otherwise, choose a composite key (buyer_email + created_at ± tolerance).
        by_key = {}
        for o in orders:
            k = (o.external_id,)  # or another deterministic key
            if k not in by_key:
                by_key[k] = o
            else:
                by_key[k] = self._resolve_conflict(by_key[k], o)
        return list(by_key.values())

    def _resolve_conflict(self, a: Order, b: Order) -> Order:
        # Conflict strategy examples:
        #  - Prioritize a specific provider
        #  - Prefer most recently updated (if you carry updated_at)
        #  - Merge fields when one is missing (e.g., pick non-null email)
        # Keep provenance using `raw` if needed.
        if a.provider == "baselinker":
            return a
        return b

  ```
- [ ] 