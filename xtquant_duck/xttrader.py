class XtQuantTraderCallback:
    pass


class XtQuantTrader:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.callback = None

    def register_callback(self, callback):
        self.callback = callback

    def start(self):
        raise NotImplementedError("mock XtQuantTrader.start is not implemented")

    def connect(self):
        raise NotImplementedError("mock XtQuantTrader.connect is not implemented")

    def subscribe(self, account):
        del account
        raise NotImplementedError("mock XtQuantTrader.subscribe is not implemented")

    def query_stock_asset(self, account):
        del account
        raise NotImplementedError("mock XtQuantTrader.query_stock_asset is not implemented")

    def query_stock_positions(self, account):
        del account
        raise NotImplementedError("mock XtQuantTrader.query_stock_positions is not implemented")

    def query_stock_orders(self, account, cancelable_only=False):
        del account, cancelable_only
        raise NotImplementedError("mock XtQuantTrader.query_stock_orders is not implemented")

    def query_stock_trades(self, account):
        del account
        raise NotImplementedError("mock XtQuantTrader.query_stock_trades is not implemented")
