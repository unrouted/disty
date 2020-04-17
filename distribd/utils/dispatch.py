class Dispatcher(dict):
    def register(self, name):
        def decorator(method):
            self[name] = method
            return method

        return decorator
