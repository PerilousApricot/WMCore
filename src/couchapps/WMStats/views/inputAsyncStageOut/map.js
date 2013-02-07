function(doc) {
    if ((doc.type !== 'jobsummary') || ( (doc.state !== 'success') && 
                                         (doc.state !== 'transferring') &&
                                         (doc.state !== 'complete') &&
                                         (doc.state !== 'asopending'))) { 
        return;
    }   
    var workflow = doc.workflow;
    var job = doc['_id'];
    var task = doc.task;
    var job_end_time = doc.timestamp;
    var module;
    for (var module in doc['output']) {
        if ((doc['output'][module]['asyncDest']) && (doc['output'][module]['location'] != null)) {
            retval = {'workflow' : workflow,
                        'jobid' : job,
                        'task' : task,
                        '_id': doc['output'][module]['lfn'], 
                        'checksums': doc['output'][module]['checksums'],
                        'size': doc['output'][module]['size'],                  
                        'source' : doc['output'][module]['location'],
                        'job_end_time': job_end_time,
                        'asyncDest': doc['output'][module]['asyncDest'],
                        'preserveLFN': doc['output'][module]['preserveLFN'] ? true : false
                     };
            emit(doc.timestamp, retval);
        }   
    }   
}
