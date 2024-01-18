class MissingPrimaryKey(Exception):
    def __init__(self, message='Table must have primary key.', errors=None):
        super().__init__(message, errors)