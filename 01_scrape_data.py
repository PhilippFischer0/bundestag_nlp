import os

if not os.path.exists("data/") and not os.path.isdir("data/"):
    os.makedirs(os.getenv("XML_PATH"))
else:
    pass
