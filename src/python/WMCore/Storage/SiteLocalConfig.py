#!/usr/bin/env python
"""
_SiteLocalConfig_

Utility for reading a site local config XML file and converting it
into an object with an API for getting info from it.

"""


import os
import logging
from WMCore.Algorithms.ParseXMLFile import Node, xmlFileToNode

from WMCore.Storage.TrivialFileCatalog import tfcFilename, tfcProtocol, readTFC



def loadSiteLocalConfig():
    """
    _loadSiteLocalConfig_

    Runtime Accessor for the site local config.

    Requires that CMS_PATH is defined as an environment variable

    """
    overVarName = "WMAGENT_SITE_CONFIG_OVERRIDE"
    if os.getenv(overVarName, None):
        overridePath = os.getenv(overVarName)
        if os.path.exists(os.getenv(overVarName, None)):
            m = ("Using site-local-config.xml override due to set %s env. variable, "
                 "loading: '%s'" % (overVarName, overridePath))
            logging.log(logging.DEBUG, m)
            config = SiteLocalConfig(overridePath)
            return config
        else:
            logging.log(logging.ERROR, "%s env. var. provided but not pointing "
                        "to an existing file, ignoring." % overVarName)

    defaultPath = "$CMS_PATH/SITECONF/local/JobConfig/site-local-config.xml"
    actualPath = os.path.expandvars(defaultPath)
    if os.environ.get("CMS_PATH", None) == None:
        msg = "Unable to find site local config file:\n"
        msg += "CMS_PATH variable is not defined."
        raise SiteConfigError, msg

    if not os.path.exists(actualPath):
        msg = "Unable to find site local config file:\n"
        msg += actualPath
        raise SiteConfigError, msg

    config = SiteLocalConfig(actualPath)
    return config


class SiteConfigError(StandardError):
    """
    Exception class placeholder

    """
    pass


class SiteLocalConfig:
    """
    _SiteLocalConfig_

    Readonly API object for getting info out of the SiteLocalConfig file

    """
    def __init__(self, siteConfigXML):
        self.siteConfigFile = siteConfigXML
        self.siteName = None
        self.eventData = {}

        self.frontierProxies = []
        self.frontierServers = []

        self.localStageOut = {}
        self.fallbackStageOut = []

        self.read()
        return

    def trivialFileCatalog(self):
        """
        _trivialFileCatalog_

        Return an instance of FwkJobRep.TrivialFileCatalog

        """
        tfcUrl = self.localStageOut.get('catalog', None)
        if tfcUrl == None:
            return None
        try:
            tfcFile = tfcFilename(tfcUrl)
            tfcProto = tfcProtocol(tfcUrl)
            tfcInstance = readTFC(tfcFile)
            tfcInstance.preferredProtocol = tfcProto
        except StandardError as ex:
            msg = "Unable to load TrivialFileCatalog:\n"
            msg += "URL = %s\n" % tfcUrl
            raise SiteConfigError, msg
        return tfcInstance


    def localStageOutCommand(self):
        """
        _localStageOutCommand_

        Return the stage out command setting from local-stage-out

        """
        return self.localStageOut['command']

    def localStageOutOption(self):
        """
        _localStageOutOption_

        Return the stage out option setting from local-stage-out
        """
        return self.localStageOut['option']

    def localStageOutSEName(self):
        """
        _localStageOutSEName_

        return the local SE Name used for stage out

        """
        return self.localStageOut['se-name']


    def read(self):
        """
        _read_

        Load data from SiteLocal Config file and populate this object

        """
        try:
            node = xmlFileToNode(self.siteConfigFile)
        except StandardError as ex:
            msg = "Unable to read SiteConfigFile: %s\n" % self.siteConfigFile
            msg += str(ex)
            raise SiteConfigError, msg

        nodeResult =  nodeReader(node)

        if not nodeResult.has_key('siteName'):
            msg = "Unable to find site name in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        if not nodeResult.has_key('catalog'):
            msg = "Unable to find catalog entry for event data in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        if not nodeResult.has_key('localStageOut'):
            msg = "Error:Unable to find any local-stage-out"
            msg += "information in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg

        self.siteName             = nodeResult.get('siteName', None)
        self.eventData['catalog'] = nodeResult.get('catalog', None)
        self.localStageOut        = nodeResult.get('localStageOut', [])
        self.fallbackStageOut     = nodeResult.get('fallbackStageOut', [])
        self.frontierServers      = nodeResult.get('frontierServers', [])
        self.frontierProxies      = nodeResult.get('frontierProxies', [])
        return

def coroutine(func):
    """
    _coroutine_

    Decorator method used to prime coroutines

    """
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start



def nodeReader(node):
    """
    _nodeReader_

    Given a node, see if we can find what we're looking for
    """
    processSiteInfo = {
        'event-data': processEventData(),
        'local-stage-out': processLocalStageOut(),
        'calib-data': processCalibData(),
        'fallback-stage-out': processFallbackStageOut()
        }

    report = {}
    sProcess = processSite(processSiteInfo)
    processor = processNode(sProcess)
    processor.send((report, node))

    return report

@coroutine
def processNode(target):
    """
    Starts at the top of the tree and finds the site
    """
    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name == 'site-local-config':
                for child in subnode.children:
                    if child.name == 'site':
                        target.send((report, child))


@coroutine
def processSite(targets):
    """
    Process the site tree in a config.

    """

    while True:
        report, node = (yield)
        #Get the name first
        report['siteName'] = node.attrs.get('name', None)
        for subnode in node.children:
            if subnode.name == 'event-data':
                targets['event-data'].send((report, subnode))
            elif subnode.name == 'calib-data':
                targets['calib-data'].send((report, subnode))
            elif subnode.name == 'local-stage-out':
                targets['local-stage-out'].send((report, subnode))
            elif subnode.name == 'fallback-stage-out':
                targets['fallback-stage-out'].send((report, subnode))



@coroutine
def processEventData():
    """
    Process eventData in a site

    """

    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name == 'catalog':
                report['catalog'] = str(subnode.attrs.get('url', None))

@coroutine
def processLocalStageOut():
    """
    Find the local-stage-out directory

    """

    while True:
        report, node = (yield)
        localReport = {}
        for subnode in node.children:
            if subnode.name == 'se-name':
                localReport['se-name'] = subnode.attrs.get('value', None)
            elif subnode.name == 'command':
                localReport['command'] = subnode.attrs.get('value', None)
            elif subnode.name == 'option':
                localReport['option'] = subnode.attrs.get('value', None)
            elif subnode.name == 'catalog':
                localReport['catalog'] = subnode.attrs.get('url', None)
        report['localStageOut'] = localReport

@coroutine
def processFallbackStageOut():
    """
    Find the fallback stage out directory

    """

    while True:
        report, node = (yield)
        localReport = {}
        for subnode in node.children:
            if subnode.name == 'se-name':
                localReport['se-name'] = subnode.attrs.get('value', None)
            elif subnode.name == 'command':
                localReport['command'] = subnode.attrs.get('value', None)
            elif subnode.name == 'option':
                localReport['option'] = subnode.attrs.get('value', None)
            elif subnode.name == 'lfn-prefix':
                localReport['lfn-prefix'] = subnode.attrs.get('value', None)
        report['fallbackStageOut'] = [localReport]


@coroutine
def processCalibData():
    """
    Process calib-data

    """

    while True:
        report, node = (yield)

        frontierProxies = []
        frontierServers = []
        for subnode in node.children:
            if subnode.name == "frontier-connect":
                for frontierSubnode in subnode.children:
                    subNodeUrl = frontierSubnode.attrs.get("url", None)

                    if frontierSubnode.name == "proxy":
                        frontierProxies.append(subNodeUrl)
                    elif frontierSubnode.name == "server":
                        frontierServers.append(subNodeUrl)

        report["frontierProxies"] = frontierProxies
        report["frontierServers"] = frontierServers
