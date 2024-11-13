class ShanoirContext(object):
    """
    Configuration class for connection & authentication to a Shanoir instance
    """

    def __init__(self):
        self.scheme = "https"
        self.domain = "shanoir-ng-nginx"
        self.verify = True
        self.timeout = None
        self.proxies = {}
        self.username = ""
        self.clientId = "shanoir-uploader"
        self.access_token = None
        self.refresh_token = None
        self.output_folder = "./ressources/output"
        self.entry_file = ""
        self.project = ""

