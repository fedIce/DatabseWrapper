class ServerErrors(Exception):
    """
    Error occured: check your input data
    """
    pass


class InvalidDataError(ServerErrors):
    def __init__(self, error=''):
        print(f"""
            InvalidDataError: Invalid input recognised. check your data for required fields {error}
        """)
    pass