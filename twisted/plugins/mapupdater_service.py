from autobahn.twisted.wamp import ApplicationSessionFactory
from autobahn.twisted.websocket import WampWebSocketClientFactory
from autobahn.wamp.types import ComponentConfig
from autobahn.websocket.protocol import parseWsUrl
from twisted.application.internet import ClientService
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.internet.endpoints import HostnameEndpoint, wrapClientTLS
from twisted.internet.ssl import optionsForClientTLS

from twisted.application.service import IServiceMaker, MultiService
from twisted.plugin import IPlugin
from twisted.python import usage
from zope.interface import implements

from mapupdater.component import MapUpdaterComponent
from mapupdater.updater import S3Updater, WebListUpdater



class S3Options(usage.Options):
    optParameters = (
        [ ['list-url', None, None, 'S3 bucket URL to get file list.']
        , ['fetch-url', None, None, 'S3 bucket or cloudfront configuration URL to fetch maps from.']
        , ['key-prefix', None, None, 'S3 object prefix to match.']
        ])



class WebListOptions(usage.Options):
    optParameters = (
        [ ['fetch-url', None, None, 'URL to fetch maps from.']
        ])



class Options(usage.Options):
    optParameters = (
        [ ['notify-server', None, None, 'Notification server address.']
        , ['notify-realm', None, None, 'Notification realm.']
        , ['maps-path', None, None, 'Path to maps.']
        ])

    subCommands = (
        [ ['s3', None, S3Options, 'S3 updater.']
        , ['weblist', None, WebListOptions, 'Web list updater.']
        ])



class MapUpdaterServiceMaker(object):
    implements(IPlugin, IServiceMaker)
    tapname = 'mapupdater'
    description = 'Tempus map updater..'
    options = Options

    def makeService(self, options):
        if options.subCommand == 's3':
            subOptions = options.subOptions
            mapUpdater = S3Updater(options['maps-path'],
                                   subOptions['fetch-url'],
                                   subOptions['list-url'],
                                   subOptions['key-prefix'])
        elif options.subCommand == 'weblist':
            subOptions = options.subOptions
            mapUpdater = WebListUpdater(options['maps-path'],
                                    subOptions['fetch-url'])
        else:
            print options
            raise ValueError('Sub-command must be "s3" or "weblist".')

        s = MultiService()

        componentService = self.getMapUpdaterComponentService(
            options['notify-server'].decode(),
            options['notify-realm'].decode(), mapUpdater)
        componentService.setServiceParent(s)

        LoopingCall(mapUpdater.checkMaps).start(60)

        return s


    def getMapUpdaterComponentService(self, url, realm, mapUpdater):
        def create(config):
            try:
                session = MapUpdaterComponent(mapUpdater, config)
            except Exception as e:
                # the app component could not be created .. fatal
                print(e)
            else:
                session.debug_app = False
                return session

        sessionFactory = ApplicationSessionFactory(
            ComponentConfig(realm, None))
        sessionFactory.session = create

        transportFactory = WampWebSocketClientFactory(
            sessionFactory, url=url)
        transportFactory.noisy = False

        isSecure, host, port, resource, path, params = parseWsUrl(url)

        endpoint = HostnameEndpoint(reactor, host.encode('utf8'), port)
        if isSecure:
            contextFactory = optionsForClientTLS(hostname=host)
            endpoint = wrapClientTLS(contextFactory, endpoint)
        return ClientService(endpoint, transportFactory)



serviceMaker = MapUpdaterServiceMaker()
