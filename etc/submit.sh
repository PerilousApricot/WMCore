#!/bin/bash

touch Report.pkl

# should be a bit nicer than before
echo "WMAgent bootstrap : `date -u` : starting..."


# validate arguments
echo "Hostname: `hostname`"
echo "  UserID: `id`"
echo "HostInfo: `uname -a`"
echo "    Time: `date -u`"
echo "Environment is:"
env

if [ "x" = "x$1" ]
then
	echo "WMAgent bootstrap : `date -u` : Error: A sandbox must be specified" >&2
	exit 1
fi

if [ "x" = "x$2" ]
then
	echo "WMAgent bootstrap : `date -u` : Error: An index must be specified" >&2
	exit 1
fi

# assign arguments

SANDBOX=$1
INDEX=$2
echo "WMAgent bootstrap : `date -u` : arguments validated..."

### source the CMSSW stuff using either OSG or LCG style entry env. variables
###    (incantations per oli's instructions)
#   LCG style --
if [ "x" != "x$VO_CMS_SW_DIR" ]
then
	. $VO_CMS_SW_DIR/cmsset_default.sh

#   OSG style --
elif [ "x" != "x$OSG_APP" ]
then
	. $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
else
	echo "WMAgent bootstrap : `date -u` : Error: neither OSG_APP nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "WMAgent bootstrap : `date -u` : Error: Because of this, we can't load CMSSW. Not good." >&2
	exit 2
fi
echo "WMAgent bootstrap : `date -u` : WMAgent thinks it found the correct CMSSW setup script"

if [ -e $VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh ]
then
	. $VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh 
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib
elif [ -e $OSG_APP/cmssoft/cms/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh ]
then
	. $OSG_APP/cmssoft/cms/COMP/slc5_amd64_gcc434/external/python/2.6.4/etc/profile.d/init.sh
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OSG_APP/cmssoft/cms/COMP/slc5_amd64_gcc434/external/openssl/0.9.7m/lib:$OSG_APP/cmssoft/cms/COMP/slc5_amd64_gcc434/external/bz2lib/1.0.5/lib
fi
command -v python2.6 > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
	echo "WMAgent bootstrap : `date -u` : Error: Python2.6 isn't available on this worker node." >&2
	echo "WMAgent bootstrap : `date -u` : Error: WMCore/WMAgent REQUIRES python2.6" >&2
	exit 3	
else
	echo "WMAgent bootstrap : `date -u` : found python2.6 at.."
	echo `which python2.6`
fi

# Should be ready to unpack and run this
echo "WMAgent is unpacking the job... (`date -u`)"
python2.6 Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export WMAGENTJOBDIR=$PWD
export PYTHONPATH=$PYTHONPATH:$PWD/WMCore.zip:$PWD
echo "WMAgent is now running the job...(`date -u`)"
python2.6 Startup.py
jobrc=$?
echo "WMAgent finished the job, is copying the pickled report (`date -u`)"
cp WMTaskSpace/Report*.pkl ../
ls -l WMTaskSpace
ls -l WMTaskSpace/*
echo "WMAgent is finished. The job had an exit code of $jobrc (`date -u`)"
exit 0


