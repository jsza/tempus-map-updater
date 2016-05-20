import treq

from StringIO import StringIO
from xml.etree import ElementTree

from bs4 import BeautifulSoup

from twisted.internet.defer import (
    inlineCallbacks, DeferredSemaphore, gatherResults, returnValue)
from twisted.internet.utils import getProcessOutputAndValue
from twisted.python.filepath import FilePath
from twisted.python.urlpath import URLPath
from twisted.python import log
from twisted.web.error import Error



class MapUpdater(object):
    def __init__(self, mapsPath, fetchURL, deleteIfNotPresent):
        assert isinstance(mapsPath, str) and len(mapsPath)
        assert isinstance(fetchURL, str) and len(fetchURL)
        self.mapsPath = FilePath(mapsPath)
        self.downloadTempPath = self.mapsPath.child('mapupdater')
        self.fetchURL = URLPath.fromString(fetchURL)
        self.deleteIfNotPresent = deleteIfNotPresent
        self.semaphore = DeferredSemaphore(1)
        self.downloadSemaphore = DeferredSemaphore(4)


    def checkMaps(self, *a, **kw):
        """
        Wrap self._checkMaps to prevent running multiple checks at once.
        """
        return self.semaphore.run(self._checkMaps, *a, **kw)


    def _checkMaps(self, forceDownloadMaps=None):
        def _cb(remoteMaps):
            if forceDownloadMaps:
                remoteMaps = list(set(remoteMaps + forceDownloadMaps))
            remoteMapsLower = [f.lower() for f in remoteMaps]
            ourMaps = filter(lambda p: not p.isdir() and p.path.endswith('.bsp'),
                             self.mapsPath.children())
            ourMapFilenames = [p.basename().lower() + '.bz2' for p in ourMaps]

            missing = []
            for f in remoteMaps:
                if f.lower() not in ourMapFilenames:
                    missing.append(f)

            delete = []
            for p in ourMaps:
                filename = p.basename().lower() + '.bz2'
                if filename not in remoteMapsLower:
                    delete.append(p)

            if self.deleteIfNotPresent and delete:
                for fp in delete:
                    try:
                        fp.remove()
                    # file does not exist
                    except OSError:
                        pass

                print 'Deleted {} map(s) not present at remote server:'.format(len(delete))
                print ', '.join([x.basename() for x in delete])

            if missing:
                print 'Fetching {} map(s)'.format(len(missing))

                def _allFinished(ignored):
                    self.mapsPath.child('tempus_map_updater_run_once').touch()
                    print 'Now up-to-date.'

                ds = []
                for filename in missing:
                    ds.append(self.fetchMap(filename))
                return gatherResults(ds).addCallback(_allFinished)

        return self.getMapList().addCallback(_cb)


    def fetchMap(self, *a, **kw):
        return self.downloadSemaphore.run(self._fetchMap, *a, **kw)


    def _fetchMap(self, filename):
        downloadTempPath = self.downloadTempPath
        if not downloadTempPath.exists():
            downloadTempPath.makedirs()

        def _cb(response, fn):
            tp = downloadTempPath.child(fn)
            fd = tp.open('wb')

            def _extracted(ignored):
                extractedPath = tp.sibling(tp.basename().replace('.bz2', ''))
                extractedPath.moveTo(
                    self.mapsPath.child(tp.basename().replace('.bz2', '')))
                try:
                    tp.remove()
                # File already gone
                except OSError:
                    pass
                print 'Finished downloading {}'.format(fn)

            def _finished(ignored):
                fd.close()
                d = getProcessOutputAndValue(
                    'aunpack', (tp.path, '-X', downloadTempPath.path))
                d.addErrback(log.err)
                d.addCallback(_extracted)
                return d

            def _eb(failure):
                print 'Error downloading {}:'.format(fn)
                print failure.getTraceback()
                fd.close()
                try:
                    tp.remove()
                # File already gone
                except OSError:
                    pass

            d = treq.collect(response, fd.write)
            d.addCallback(_finished)
            d.addErrback(_eb)
            return d

        d = treq.get(str(self.fetchURL.child(filename)))
        return d.addCallback(_cb, filename)


    def getMapList(self, forceDownloadMaps):
        raise NotImplementedError('Subclasses must override this method.')



class WebListUpdater(MapUpdater):
    @inlineCallbacks
    def getMapList(self):
        response = yield treq.get(str(self.fetchURL))
        html = yield response.text()

        if response.code >= 400:
            print html
            raise Error(response.code)

        remoteMaps = self.parseMapList(html)
        returnValue(remoteMaps)


    def parseMapList(self, html):
        soup = BeautifulSoup(html, 'html5lib')

        remoteMaps = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href.endswith('bsp.bz2') and '/' not in href:
                remoteMaps.append(href.encode())

        return remoteMaps



class S3Updater(MapUpdater):
    def __init__(self, mapsPath, fetchURL, deleteIfNotPresent, listURL,
            keyPrefix):
        MapUpdater.__init__(self, mapsPath, fetchURL, deleteIfNotPresent)
        assert isinstance(listURL, str) and len(listURL)
        assert isinstance(keyPrefix, str) and len(keyPrefix)
        self.listURL = URLPath.fromString(listURL)
        self.keyPrefix = keyPrefix


    @inlineCallbacks
    def getMapList(self):
        response = yield treq.get(str(self.listURL))
        xml = yield response.text()

        if response.code >= 400:
            print xml
            raise Error(response.code)

        remoteMaps = self.parseMapList(xml)
        returnValue(remoteMaps)


    def parseMapList(self, xml):
        namespace = '{http://s3.amazonaws.com/doc/2006-03-01/}'

        tree = ElementTree.parse(StringIO(xml))
        root = tree.getroot()

        if root.tag != namespace + 'ListBucketResult':
            raise ValueError('Response returned wrong XML')

        remoteMaps = []
        for contents in root.findall(namespace + 'Contents'):
            for key in contents.findall(namespace + 'Key'):
                text = key.text
                if text.startswith(self.keyPrefix) and text.lower().endswith('.bsp.bz2'):
                    filename = text.replace(self.keyPrefix, '')
                    if len(filename.split('/')) > 1:
                        continue
                    remoteMaps.append(filename)

        if not len(remoteMaps):
            # Something probably went horribly wrong if there's no result...
            print xml
            raise ValueError('No maps found at remote server.')

        return remoteMaps
