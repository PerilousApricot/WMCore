#!/usr/bin/env python
"""
_WMComponent_t_

Tests for the wmcore components.


"""
__all__ = []



import WMQuality.DatabaseCache

def setup_package():
    WMQuality.DatabaseCache.enterPackage( )
    
def teardown_package():
    WMQuality.DatabaseCache.leavePackage( )