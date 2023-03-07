//  
//  Copyright 2023 PayPal Inc.
//  
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  
  
/*

Converts etcd state into:
    {
        clusterName: clusterName,
        state: mapping|redist,
        mapping: shardMapOrRedist struct described below,
        redist: shardMapOrRedist struct described below
    }
    shardMapOrRedist struct:
    {
        zones: [ for each zone: {
            id: #,
            status: status: redistributing|abort|queued|finished,
            algVersion: #
            nodes: node => {
                id: #,
                ipport: ipport,
                shards:
                    if redist: [ shard IDs of shards being moved to/from this node ]
                    if mapping: {
                        primary: [ shard IDs ],
                        backups: [ shard IDs ] // If shard mapping alg version = 2, backups will be empty
                }
                type: added|removed|stable, // If redist mapping,
                status: redistributing|queued|finished
            }
            shards: [ for each shard: {
                id: #,
                st: Q|P|F|A,
                source: node #,
                target: node #,
                progress: #,
                total: #,
                ok: #,
                err: #,
                drop: #,
                expired: #,
                mshd: #,
                et: duration
            } ] // If redist mapping
        ]
    }
*/

var redistStateLogRetrieved = false;
var redistStateLog = null;

function getShardMap(url, async, callback, failureCallback) {
    $.ajax({
        dataType: "json",
        url: url,
        async: async,
        success: function (data) {
            var clusterName = data["clusterName"];
            var shardMap = convertState(clusterName, data);

            if (shardMap.state != "redist") {
                if (!redistStateLogRetrieved) {
                    refreshRedistStateLog(shardMap, async, callback, callback);
                    return;
                }
                shardMap.redist = redistStateLog;
            }
            callback(shardMap);
        },
        error: function (jqXHR, error) {
            if (failureCallback != null) {
                failureCallback(jqXHR, error);
            }
        }
    });
}

function refreshRedistStateLog(shardMap, async, callback, failureCallback) {
    $.ajax({
        dataType: "json",
        url: "/redist_state.json",
        async: async,
        success: function (data) {
            redistStateLog = convertRedistStateLog(shardMap, data);
            redistStateLogRetrieved = true;
            shardMap.redist = redistStateLog;
            callback(shardMap);
        },
        error: function (jqXHR, error) {
            redistStateLogRetrieved = true;
            redistStateLog = null;
            if (failureCallback != null) {
                failureCallback(shardMap, jqXHR, error);
            }
        }
    });
}

function parseEtcdKey(key, clusterName, type) {
    var prefix = clusterName + "_";
    if (type == "redist") {
        prefix += "redist_";
    }

    if (!key.startsWith(prefix)) {
        return null;
    }

    var REGEX = /([A-z_]*)(\d+)?(_(\d+))?(_(\d+))?/g;
    var match = REGEX.exec(key.substr(prefix.length));
    if (match == null) {
        return null;
    }

    var l = match[1].length;
    var t = match[1];
    if (t[l-1] == "_") {
        t = t.substr(0, l - 1);
    }

    var r = {
        type: t,
        zoneID: (match[2] != null ? parseInt(match[2]) : null),
        nodeID: (match[4] != null ? parseInt(match[4]) : null),
        shardID: (match[6] != null ? parseInt(match[6]) : null),
    }

    return r;
}


function generateEtcdKey(clusterName, type, zoneID, nodeID, shardID) {
    var node = (nodeID == null ? "" : `_${nodeID.toString().padStart(3, "0")}`);
    var shard = (shardID == null ? "" : `_${shardID.toString().padStart(5, "0")}`);
    return `${clusterName}_redist_${type}_${zoneID.toString().padStart(2, "0")}${node}${shard}`;
}


function convertState(clusterName, data) {
    if (Object.keys(data).filter(x => x.indexOf("_redist_") >= 0).length > 0) {
        return convertRedistState(clusterName, data);
    }
    return convertMappingState(clusterName, data);
}


function convertMappingState(clusterName, data) {
    var state = {
        clusterName: clusterName,
        state: "mapping",
    };
    var mapping = { zones: [] };

    Object.keys(data).forEach(function (key) {
        if (key.indexOf("_redist_") >= 0) {
            return;
        }

        var value = data[key];
        if (value == "") {
            return;
        }

        var match = parseEtcdKey(key, clusterName, "mapping");
        if (match == null) {
            return;
        }

        if (mapping.zones[match.zoneID] == null) {
            mapping.zones[match.zoneID] = {
                id: match.zoneID,
                nodes: {},
            };
        }

        var node;
        if (match.zoneID != null && match.nodeID != null) {
            if (mapping.zones[match.zoneID].nodes[match.nodeID] == null) {
                mapping.zones[match.zoneID].nodes[match.nodeID] = {
                    id: match.nodeID,
                };
            }
    
            node = mapping.zones[match.zoneID].nodes[match.nodeID];
        }

        switch (match.type) {
            case "node_shards":
                var fields = value.split("|");
                node.shards = {
                    primary: fields[0].split(",").filter(x => x != "").map(x => parseInt(x)).sort((a, b) => a - b),
                    backup: fields[1].split(",").filter(x => x != "").map(x => parseInt(x)).sort((a, b) => a - b)
                }
                break;

            case "node_ipport":
                node.ipport = value;
                break;

            case "numshards":
                mapping.numShards = parseInt(value);
                break;

            case "numzones":
                mapping.numZones = parseInt(value);
                break;

            case "version":
                mapping.version = parseInt(value);
                break;

            case "algver":
                mapping.algVersion = parseInt(value);
                break;
        }
    });

    state.mapping = mapping;
    return state;
}

var enableStateToZoneStatus = {
    "ready": "queued",
    "abort_all": "aborted",
    "abort_zone": "aborted",
    "yes_source": "redistributing",
    "yes_target": "redistributing",
};
function convertRedistState(clusterName, data) {
    var state = {
        clusterName: clusterName,
        state: "redist",
        mapping: convertMappingState(clusterName, data).mapping
    };
    var redist = { zones: [] };

    // Sort keys so we process _redist_node_shards before we process _redist_from_ and _redist_node_ipport
    var keys = Object.keys(data).sort().reverse();

    keys.forEach(function (key) {
        var value = data[key];
        if (value == "") {
            return;
        }

        var match = parseEtcdKey(key, clusterName, "redist");
        if (match == null) {
            //console.log(`Failed to parse etcd key: ${key}`);
            return;
        }

        if (redist.zones[match.zoneID] == null) {
            redist.zones[match.zoneID] = {
                id: match.zoneID,
                status: "queued",
                nodes: [],
                shards: [],
            };
        }

        var zone = redist.zones[match.zoneID];

        var createNodeIfNull = function (zone, nodeID) {
            if (zone.nodes[nodeID] == null) {
                zone.nodes[nodeID] = {
                    id: nodeID,
                    type: "stable",
                    shards: [],
                };
            }
        };

        switch (match.type) {
            case "enable":
                zone.status = enableStateToZoneStatus[value];
                break;

            case "node_shards":
                var shards = value.split("|");
                createNodeIfNull(zone, match.nodeID);
                break;

            case "from":
                var shards = value.split("|").map(x => x.split('_')); // <shardid>_<newNodeId>|...
                shards.forEach(function (shard) {
                    var shardID = parseInt(shard[0]);
                    var target = parseInt(shard[1]);
                    var removingSource = isRemovingSource(data, clusterName, match.zoneID, match.nodeID);

                    createNodeIfNull(zone, match.nodeID);
                    createNodeIfNull(zone, target);

                    if (removingSource) {
                        zone.nodes[match.nodeID].type = "removed";
                    } else {
                        zone.nodes[target].type = "added";
                    }
                    var shardState = processShardState(shardID, match.nodeID, target, data[generateEtcdKey(clusterName, "state", match.zoneID, match.nodeID, shardID)]);
                    zone.shards[shardID] = shardState;

                    zone.nodes[target].shards.push(shardID);
                    zone.nodes[match.nodeID].shards.push(shardID);
                });
                break;

            case "state":
                break;

            case "tgtstate":
                break;

            case "node_ipport":
                break;
        }
    });

    redist.zones.forEach(zone => zone.nodes.forEach(node => node.status = getNodeStatus(node, zone)));
    redist.zones.forEach(zone => zone.status = getZoneStatus(zone));
    state.redist = redist;
    return state;
}

function getZoneStatus(zone) {
    if (zone.status == "aborted") {
        return zone.status;
    }

    var finished = 0;
    for (let node of zone.nodes) {
        if (node.status == "redistributing") {
            return "redistributing";
        }
        if (node.status == "finished") {
            finished++;
        }
    }

    if (finished == zone.nodes.length) {
        return "finished";
    }

    return "queued";
}

function getNodeStatus(node, zone) {
    var state = {
        finished: 0,
        aborted: 0,
        queued: 0,
        inProgess: 0,
    };

    for (let s of node.shards) {
        if (zone.shards[s] == null) {
            continue;
        }
        switch (zone.shards[s].st) {
            case 'Q':
                state.queued++;
                break;
            case 'F':
                state.finished++;
                break;
            case 'P':
                state.inProgess++;
                break;
            case 'A':
                state.aborted++;
                break;
        }
    }

    if (state.inProgess > 0) {
        return "redistributing";
    }

    if (state.queued > 0) {
        if (state.finished > 0 || state.aborted > 0) {
            return "redistributing";
        } else {
            return "queued";
        }
    }

    if (state.finished > 0) {
        if (state.aborted > 0) {
            return "redistributing";
        } else {
            return "finished";
        }
    }

    return "queued";
}

function isRemovingSource(data, clusterName, zoneID, source) {
    // Determine only based on redist keys (redist_state.json only has redist keys)
    var shards = data[`${clusterName}_redist_node_shards_${zoneID.toString().padStart(2, "0")}_${source.toString().padStart(3, "0")}`];
    return (shards == null || shards == "");
}

function processShardState(shard, source, target, state) {
    if (state == "begin") {
        return {
            st: "Q",
            id: shard,
            source: source,
            target: target,
            progress: 0,
            total: 0,
            ok: 0,
            err: 0,
            drop: 0,
            expired: 0,
            mshd: 0,
            et: 0,
        };
    }

    var data = state.split("&").reduce(function (data, entry) {
        var fields = entry.split("=", 2);
        data[fields[0]] = (isNaN(fields[1]) ? fields[1] : parseInt(fields[1]));
        return data;
    }, {});
    data.id = shard;
    data.source = source;
    data.target = target;
    data.progress = data.st == "F" ? 100 : Math.round((data.mshd / 256) * 100);

    return data;
}

function getNodeName(zoneID, nodeID) {
    return `${zoneID.toString().padStart(2, "0")}_${nodeID.toString().padStart(3, "0")}`
}

function parseNodeName(nodeName) {
    var fields = nodeName.split('_');
    return {
        zoneID: parseInt(fields[0]),
        nodeID: parseInt(fields[1]),
    };
}


function convertRedistStateLog(shardMap, data) {
    var redistStateLog = Object.keys(data).reduce((obj, key) => {
        obj[`${shardMap.clusterName}_${key}`] = data[key];
        return obj;
    }, {});

    return convertState(shardMap.clusterName, redistStateLog).redist;
}