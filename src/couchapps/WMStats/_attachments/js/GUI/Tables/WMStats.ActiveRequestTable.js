WMStats.namespace("ActiveRequestConfig");
WMStats.namespace("ActiveRequestTable");

WMStats.ActiveRequestConfig = function(requestData) {
    
    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    var _getData = requestData.getDataByWorkflow;
    var  _WMBSJobsTotal =  requestData.getWMBSJobsTotal
    var _queuedTotal = requestData.queuedTotal;
    var _failureTotal = requestData.failureTotal;
    var _eta = requestData.estimateCompletionTime;

    var tableConfig = {
        "iDisplayLength": 25,
        "sScrollX": "",
        "sDom": 'lrtip',
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData.workflow);
                      },
              "bUseRendered": false
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              var result = _getData(o.aData.workflow, "status.inWMBS",  0) / 
                                          _getData(o.aData.workflow, 'total_jobs', 1) * 100
                              return (result.toFixed(1) + '%');
                        }
            },
            { "sDefaultContent": 0,
              "sTitle": "job progress", 
              "fnRender": function ( o, val ) {
                            var totalJobs = _WMBSJobsTotal(o.aData.workflow) || 1;
                            var result = (_getData(o.aData.workflow, "status.success", 0) + _failureTotal(o.aData.workflow)) /
                                     totalJobs * 100
                            return  (result.toFixed(1) + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "event progress", 
              "fnRender": function ( o, val ) {
                           //TODO this might not needed since input_events should be number not string. (for the regacy record)
                           var inputEvents =Number(_getData(o.aData.workflow, "input_events", 1)) || 1;
                           var result = Number(_getData(o.aData.workflow, "output_progress.0.events", 0)) /
                                      (inputEvents) * 100
                            return (result.toFixed(1) + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure rate", 
              "fnRender": function ( o, val ) {
                           var totalJobs = (_getData(o.aData.workflow, "status.success", 0) + _failureTotal(o.aData.workflow)) || 1
                           var result = _failureTotal(o.aData.workflow) / totalJobs * 100;
                           return (result.toFixed(1)  + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "Eestimated Completion", 
              "fnRender": function ( o, val ) {
                            return (WMStats.Utils.foramtDuration(_eta(o.aData.workflow)));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "cool off ", 
              "fnRender": function ( o, val ) {
                            return (_getData(o.aData.workflow, "status.cooloff", 0));
                          }
            },
            /*
            { "sDefaultContent": 0,
              "sTitle": "EAT", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            */
            //TODO add more data (consult dataops)
        ]
    }
    
    var defaultFilterConfig = {
        "sPlaceHolder": "head:before",
        "aoColumns": [
            {type: "text", bRegex: true, bSmart: true},               
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true}
        ]
    }
    
    tableConfig.aaData = requestData.getList();
    
    return {
        tableConfig : tableConfig,
        filterConfig: null,
    }
};


WMStats.ActiveRequestTable = function (requestData, containerDiv) {
        var config = WMStats.ActiveRequestConfig(requestData);
        return WMStats.Table(config.tableConfig).create(containerDiv, 
                                                 config.filterConfig);
}
