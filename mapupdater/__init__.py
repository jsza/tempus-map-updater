from twisted.web import client
# Prevent noisy log messages from being printed
client._HTTP11ClientFactory.noisy = False
