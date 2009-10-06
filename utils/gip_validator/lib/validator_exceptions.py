
class ValidatorException(Exception):
    def __init__(self, value):
        Exception.__init__(self, value)
        self.parameter = value
    def __str__(self):
        return repr(self.parameter)

class ConfigurationException(ValidatorException):
    pass

class EmptyNodeListException(ValidatorException):
    pass

class GenericXMLException(ValidatorException):
    pass
