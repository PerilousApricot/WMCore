"""
WMQuality.TestHarness - 
    Helper function to wrap around nose

Created by Andrew Melo <andrew.melo@gmail.com> on Oct 1, 2012
"""
import traceback
import nose
def main( stopOnFirstFailure = False):
    stacktrace = traceback.extract_stack()
    callingFile = stacktrace[-2][0] # get the filename from the caller
    args = ['nosetest', '-m', '(_t.py$)|(_t$)|(^test)','--tests',callingFile]
    if stopOnFirstFailure:
        args.append('-x')
    return nose.run(argv=args)