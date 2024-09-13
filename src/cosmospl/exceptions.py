class RespFail(Exception):
    """Exception when http response isn't status code 200."""


class NoDocuments(Exception):
    """
    Couldn't find Documents in response.

    The Cosmos response is json with the data inside a Documents key and other
    superfluous meta data. This function takes out the Documents section
    without fully parsing the json.
    """


class Resp401(RespFail):
    """Unauthorized."""


class UnsupportedPartitionKey(UserWarning):
    """Unsupported Partition Key."""


class MustSpecifyPartitionKey(Exception):
    """Must Specify a Partition Key or have defaults set."""
