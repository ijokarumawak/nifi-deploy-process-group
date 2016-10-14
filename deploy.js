var fs = require('fs');
var path = require('path');
var request = require('request');
var yaml = require('js-yaml');
var debug = true;

function NiFiApi(conf) {
  if (conf.secure) this.cert = resolvePath(conf.certFile);
  this.getApiRoot = function() {
    return conf.secure ? conf.api.secure : conf.api.plain;
  }
  this.request = function(options, callback) {

    var o = {};
    if (typeof(options) === 'string') {
      // Only url is specified.
      o.url = this.getApiRoot() + options;
      o.method = 'GET';
    } else {
      // Initialize with the specified options.
      for (k in options) {
        o[k] = options[k];
      }
      o.url = this.getApiRoot() + options.url;
    }

    if (this.cert) o.ca = fs.readFileSync(this.cert);

    return request(o, callback);
  }
}

var uploadTemplate = function(nifiApi, pgId, filepath, callback) {
  var req = nifiApi.request({
    url: '/process-groups/' + pgId + '/templates/upload',
    method: 'POST'
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    if(debug) console.log(res.statusCode);
    switch (res.statusCode) {
      case 201:
        var match = /templates\/(.*)/.exec(res.headers.location);
        return callback(null, match[1]);
      case 200:
        // NiFi returns error message.
        return callback(res.body);
      default:
        return callback(res);
    }
  });

  var form = req.form();
  form.append('template', fs.createReadStream(filepath));
}

// http://localhost:8080/nifi-api/process-groups/b6a09099-0157-1000-9aa8-fcccef6172ac/template-instance
/*
{"templateId":"7323f263-59e2-4026-9c93-15d1f3179fa3","originX":1155.5883353676245,"originY":373.2468505218974}
*/

var instanciateTemplate = function(nifiApi, pgId, templateId, callback) {
  nifiApi.request({
    url: '/process-groups/' + pgId + '/template-instance',
    method: 'POST',
    json: {
      templateId: templateId,
      originX: 0,
      originY: 0
    }
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    if(debug) console.log(res.statusCode);
    switch (res.statusCode) {
      case 201:
        return callback(null, body.flow.processGroups[0].id);
      default:
        return callback(res);
    }
  });
}

var searchComponent = function(nifiApi, name, callback) {
  nifiApi.request('/flow/search-results?q=' + name, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    if (res.statusCode == 200) {
      var result = JSON.parse(body).searchResultsDTO;
      callback(null, result);
    } else {
      callback(res);
    }
  });
}

var updateProcessGroupState = function(nifiApi, uuid, running, callback) {
  getProcessGroupFlow(nifiApi, uuid, (err, pg) => {
    if (err) {
      callback(err);
      return;
    }
    putProcessGroupFlow(nifiApi, uuid, {
      id: uuid,
      state: running ? "RUNNING" : "STOPPED"
    }, (err) => {
      callback(err);
    })
  });
}

var moveProcessGroup = function(nifiApi, uuid, mover, callback) {
  getProcessGroup(nifiApi, uuid, (err, pg) => {
    if (err) {
      callback(err);
      return;
    }
    mover(pg.component.position);
    putProcessGroup(nifiApi, uuid, pg, (err) => {
      callback(err);
    })
  });
}

var updateProcessorState = function(nifiApi, uuid, running, callback) {
  getProcessor(nifiApi, uuid, (err, processor) => {
    if (err) {
      callback(err);
      return;
    }
    putProcessor(nifiApi, uuid, {
      revision: processor.revision,
      component: {
        id: uuid,
        state: running ? "RUNNING" : "STOPPED"
      }
    }, (err) => {
      callback(err);
    })
  });
}

var getComponent = function(nifiApi, path, callback) {
  nifiApi.request(path, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    if (res.statusCode == 200) {
      callback(null, JSON.parse(body));
    } else {
      callback(res);
    }
  });
}

var getProcessor = function(nifiApi, uuid, callback) {
  getComponent(nifiApi, '/processors/' + uuid, callback);
}

var getProcessGroup = function(nifiApi, uuid, callback) {
  getComponent(nifiApi, '/process-groups/' + uuid, callback);
}

var getProcessGroupFlow = function(nifiApi, uuid, callback) {
  getComponent(nifiApi, '/flow/process-groups/' + uuid, callback);
}


var postComponent = function(nifiApi, path, component, callback) {
  nifiApi.request({
    url: path,
    method: 'POST',
    json: component
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    if(debug) console.log(res.statusCode);
    if (res.statusCode == 201) {
      callback(null, res.headers.location);
    } else {
      callback(res);
    }
  });
}

var putComponent = function(nifiApi, path, component, callback) {
  nifiApi.request({
    url: path,
    method: 'PUT',
    json: component
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    if(debug) console.log(res.statusCode);
    if (res.statusCode == 200) {
      callback(null);
    } else {
      callback(res);
    }
  });
}

var putProcessGroup = function(nifiApi, uuid, pg, callback) {
  putComponent(nifiApi, '/process-groups/' + uuid, pg, callback);
}

var putProcessGroupFlow = function(nifiApi, uuid, pg, callback) {
  putComponent(nifiApi, '/flow/process-groups/' + uuid, pg, callback);
}

var putProcessor = function(nifiApi, uuid, processor, callback) {
  putComponent(nifiApi, '/processors/' + uuid, processor, callback);
}

var putConnection = function(nifiApi, conn, callback) {
  putComponent(nifiApi, '/connections/' + conn.component.id, conn, callback);
}

var deleteComponent = function(nifiApi, path, revision, callback) {
  var qs = {
    version: revision.version,
    clientId: revision.clientId
  };
  nifiApi.request({
    url: path,
    method: 'DELETE',
    qs: qs
  }, (err, res, body) => {
    if (err) {
      callback(err);
      return;
    }
    if(debug) console.log(res.statusCode);
    if (res.statusCode == 200) {
      callback(null);
    } else {
      callback(res);
    }
  });
}

var findInputPortIdByName = function(nifiApi, portName, targetPgId, callback) {
  searchComponent(nifiApi, portName, (err, result) => {
    if (err) return callback(err);

    var targetPorts = result.inputPortResults.filter(port => {
      return targetPgId === port.groupId;
    });
    if (targetPorts.length !== 1) {
      return callback(new Error('Could not identify target inputport by name:' + portName));
    }

    return callback(null, targetPorts[0].id);
  });
}

var findOutputPortIdByName = function(nifiApi, portName, targetPgId, callback) {
  searchComponent(nifiApi, portName, (err, result) => {
    if (err) return callback(err);

    var targetPorts = result.outputPortResults.filter(port => {
      return targetPgId === port.groupId;
    });
    if (targetPorts.length !== 1) {
      return callback(new Error('Could not identify target outputport by name:' + portName));
    }

    return callback(null, targetPorts[0].id);
  });
}

var switchSourceConnection = function(nifiApi, conn, targetPgId, inputPortId, callback) {
  conn.component.destination.id = inputPortId;
  conn.component.destination.groupId = targetPgId;
  // Update connection.
  putConnection(nifiApi, conn, callback);
}

var createConnection = function(nifiApi, clientId, parentPgId, destinationProcessorId, targetPgId, outputPortId, callback) {
  /*
   * POST
   * http://localhost:8080/nifi-api/process-groups/b6a09099-0157-1000-9aa8-fcccef6172ac/connections
   *
   * {"revision":{"clientId":"bbcdbc47-0157-1000-f0aa-95644a368716","version":0},
   * "component":{"name":"","source":{"id":"b7d14854-0157-1000-a935-145406f161cf",
   * "groupId":"b7d14852-0157-1000-0285-d7ee38fb573a","type":"OUTPUT_PORT"},
   * "destination":{"id":"b7d17e1c-0157-1000-62ed-9a7afbaca64b",
   * "groupId":"b6a09099-0157-1000-9aa8-fcccef6172ac","type":"PROCESSOR"},
   * "flowFileExpiration":"0 sec","backPressureDataSizeThreshold":"1 GB",
   * "backPressureObjectThreshold":"10000","bends":[],"prioritizers":[]}}
   */

  var conn = {
    revision: {
      clientId: clientId,
      version: 0
    },
    component: {
      name: "",
      source: {
        id: outputPortId,
        groupId: targetPgId,
      },
      destination: {
        id: destinationProcessorId,
        groupId: parentPgId,
        type: "PROCESSOR"
      },
      flowFileExpiration: "0 sec",
      backPressureObjectThreshold: "10000",
      bends: [],
      prioritizers: []
    },
  };

  console.log('update conn', conn);
  postComponent(nifiApi, '/process-groups/' + parentPgId + '/connections', conn, callback);
}

var collectComponents = function(nifiApi, ctx, callback) {
  if (!nifiApi) return callback(new Error('nifiApi is required.'));
  if (!ctx.parentPgId) return callback(new Error('parentPgId is not set in context.'));
  if (!ctx.currentPgId) return callback(new Error('currentPgId is not set in context.'));
  if (!ctx.targetPgId) return callback(new Error('targetPgId is not set in context.'));

  // Start with parent process group.
  getProcessGroupFlow(nifiApi, ctx.parentPgId, (err, parentPg) => {
    if (err) return callback(new Error('Failed to get parent processor group: ' + err));
    ctx.parentPg = parentPg;
  
    getProcessGroupFlow(nifiApi, ctx.currentPgId, (err, currentPg) => {
      if (err) return callback(new Error('Failed to get current processor group: ' + err));  
      ctx.currentPg = currentPg;

      if (debug) console.log('Current pg', currentPg);
  
      getProcessGroupFlow(nifiApi, ctx.targetPgId, (err, targetPg) => {
        if (err) return callback(new Error('Failed to get target processor group: ' + err));  
        ctx.targetPg = targetPg;

        parentPg.processGroupFlow.flow.connections.forEach((conn) => {
          if (conn.destinationGroupId === ctx.currentPgId) {
            if (debug) console.log('Source connection', conn.id);
            ctx.srcConns.push(conn);

            // Find corresponding input port in target process group.
            var portName = conn.component.destination.name;
            findInputPortIdByName(nifiApi, portName, ctx.targetPgId, (err, portId) => {
              if (err) return callback(err);
              ctx.tgtInputPortIds[portName] = portId;
            });

          } else if (conn.sourceGroupId === ctx.currentPgId) {
            if (debug) console.log('Target connection', conn.id);
            ctx.dstConns.push(conn);

            // Find corresponding output port in target process group.
            var portName = conn.component.source.name;
            findOutputPortIdByName(nifiApi, portName, ctx.targetPgId, (err, portId) => {
              if (err) return callback(err);
              ctx.tgtOutputPortIds[portName] = portId;
            });
          }
        });
 
        callback(null);
      });
    });
  });
}

var updateSourceProcessorsState = function(nifiApi, ctx, running, callback) {
  if (!nifiApi) return callback(new Error('nifiApi is required.'));
  if (!ctx.srcConns) return callback(new Error('srcConns is not set in context.'));

  if (ctx.srcConns.length === 0) {
    if (debug) console.log('There is no source connections.');
    callback(null);
  }

  var updated = 0;
  ctx.srcConns.forEach((conn) => {
    updateProcessorState(nifiApi, conn.sourceId, running, (err) => {
      if (err) return callback(err);
      console.log('Updated ', conn.sourceId);
      updated++;
      if (updated === ctx.srcConns.length) {
        console.log('All of source processors have ' + (running ? 'started.' : 'stopped.'));
        callback(null);
      }
    });
  });
}

var switchSourceConnections = function(nifiApi, ctx, callback) {
  if (!nifiApi) return callback(new Error('nifiApi is required.'));
  if (!ctx.srcConns) return callback(new Error('srcConns is not set in context.'));

  if (ctx.srcConns.length == 0) {
    console.log('There is no source connections.');
    return callback(null);
  }

  var switchedSrc = 0;
  ctx.srcConns.forEach((conn, i) => {
    var portName = conn.component.destination.name;
    var inputPortId = ctx.tgtInputPortIds[portName];
    switchSourceConnection(nifiApi, conn, ctx.targetPgId, inputPortId, (err) => {
      if (err) return callback(err);

      switchedSrc++;
      if (switchedSrc === ctx.srcConns.length) {
        console.log('All of source connections have switched.');
        return callback(null);
      }
    });
  });
}

var createDestConnections = function(nifiApi, ctx, callback) {
  if (!nifiApi) return callback(new Error('nifiApi is required.'));
  if (!ctx.dstConns) return callback(new Error('dstConns is not set in context.'));

  if (ctx.dstConns.length == 0) {
    console.log('There is no destination connections.');
    return callback(null);
  }

  var createdDst = 0;
  ctx.dstConns.forEach((conn, i) => {
    // TODO: check downstream connection existance.
    var portName = conn.component.source.name;
    var outputPortId = ctx.tgtOutputPortIds[portName];
    createConnection(nifiApi, conn.revision.clientId,
      ctx.parentPgId, conn.component.destination.id,
      ctx.targetPgId, outputPortId,
      (err) => {
        if (err) return callback(err);
  
        createdDst++;
        if (createdDst === ctx.dstConns.length) {
          console.log('All of destination connections have been created.');
          return callback(null);
        }
      });
  });
}

var deploy = function(callback) {
  collectComponents(nifiApi, ctx, (err) => {
    if (err) return callback(err);
    console.log(ctx.currentPg);
  
    // TODO: check components in ctx.
    // TODO: support dry-run.
    // Stop upstream processors.
    updateSourceProcessorsState(nifiApi, ctx, false, (err) => {
      if (err) return callback(err);
  
      switchSourceConnections(nifiApi, ctx, (err) => {
        if (err) return callback(err);
  
        createDestConnections(nifiApi, ctx, (err) => {
          if (err) return callback(err);
  
          updateProcessGroupState(nifiApi, ctx.targetPgId, true, (err) => {
            if (err) return callback(err);
  
            // Start upstream processors.
            updateSourceProcessorsState(nifiApi, ctx, true, (err) => {
              if (err) return callback(err);
              callback(null);
            });
          });
        });
      });
    });
  });
}

var conf;
try {
  conf = yaml.safeLoad(fs.readFileSync('conf.yml'));
} catch (e) {
  console.log('Failed to load config', e);
  process.exit(1);
}

// Context of the NiFi data-flow.
var ctx = {
  parentPgId: null,
  parentPg: null,
  currentPgId: null,
  currentPg: null,
  targetPgId: null,
  targetPg: null,
  srcConns: new Array(),
  dstConns: new Array(),
  tgtInputPortIds: {},
  tgtOutputPortIds: {},
  templateFilePath: null,
  templateId: null
};

var nifiApi = new NiFiApi(conf.nifi);

// TODO: provide a way to find process groups by its name.
// var parentPgName = 'root';
// var currentPgName = 'conversion:1.0';
// var targetPgName = 'conversion:2.0';
//
/*
    // Find target process group within the parent process group.
    searchComponent(nifiApi, currentPgName, (err, result) => {
      if (!result.processGroupResults.length == 1) {
        return console.log('Could not identify current process group.');
      }
  
      var pg = result.processGroupResults[0];
      console.log('Current pg', pg);

      if (pg.groupId !== parentPg.processGroupFlow.id) {
        return console.log('The process group found by name was not'
          + ' in a specified parent process group');
      }
    });
*/

var cmd = process.argv[2];
ctx.parentPgId = process.argv[3];
ctx.currentPgId = process.argv[4];
// ctx.targetPgId = process.argv[5];
ctx.templateFilePath = process.argv[5];

if (!ctx.templateFilePath) return console.log('template file path is not set in context');
uploadTemplate(nifiApi, ctx.parentPgId, ctx.templateFilePath, (err, templateId) => {
  if (err) return console.log('Failed to upload template', err);
  ctx.templateId = templateId;
  console.log('Template is uploaded', templateId);

  if (!ctx.templateId) return console.log('template id path is not set in context');
  instanciateTemplate(nifiApi, ctx.parentPgId, ctx.templateId, (err, targetPgId) => {
    if (err) return console.log('Failed to instanciate template', err);
    ctx.targetPgId = targetPgId;
    console.log('Template is initiated', targetPgId);

    var orgX = 0;
    var orgY = 0;
    moveProcessGroup(nifiApi, ctx.currentPgId, (pos) => {
      orgX = pos.x;
      orgY = pos.y;
      pos.x -= 400;
    }, (err) => {
      if (err) return console.log('Failed to move', ctx.currentPgId, err);

      moveProcessGroup(nifiApi, ctx.targetPgId, (pos) => {
        pos.x += orgX;
        pos.y += orgY;
      }, (err) => {
        if (err) return console.log('Failed to move', ctx.targetPgId, err);

        deploy((err) => {
          if (err) return console.log('Failed to deploy', err);

          updateProcessGroupState(nifiApi, ctx.currentPgId, false, (err) => {
            if (err) return console.log('Failed to stop current process group', err);

            // TODO: stop destination processors.

            // Delete downstream connections.
            ctx.dstConns.forEach((conn) => {
              console.log('####', conn);
              deleteComponent(nifiApi, '/connections/' + conn.id, conn.revision, (err) => {
                if (err) return console.log('Failed to delete conn', err);
              });
            });
          });

        });
      });
    });

  });
});
