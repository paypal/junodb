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
  
// Redist graph view is optimized towards a max of 5 delta nodes and 30 nodes total per zone

var redistRender = {
    title: "Shard Redistribution",
    buttons: "redist",
    buttonHandler: redistButtonHandler,
    updateData: redistRender,
    getProgress: getProgress,
}

var TENSION = 0.4;
var connections = {};
var modalFuncs = { refresh: null };
var redistType = "grid";
var shardMap;

var selectedDeltas = {};

function redistButtonHandler(btn, renderFunc) {
    var txt = btn.text().toLowerCase();
    if (txt.indexOf("graph") >= 0) {
        redistType = "graph";
    } else if (txt.indexOf("list") >= 0) {
        redistType = "list";
    } else if (txt.indexOf("grid") >= 0) {
        redistType = "grid";
    }

    renderFunc();
};


function redistRender(sMap, resetZones, zonesExpanded) {
    shardMap = sMap;
    hideSVG();
    resetZones();

    shardMap.redist.zones.sort(sortZonesByStatus);

    if (redistType == 'graph') {
        renderGraph(zonesExpanded);
    } else if (redistType == 'list') {
        renderList(zonesExpanded);
    } else if (redistType == 'grid') {
        renderGrid(zonesExpanded);
    }
    refreshModal();
}


function renderGraph(zonesExpanded) {
    showSVG();
    var zonesWrapper = $('.zones');
    clearConnections();
    shardMap.redist.zones.forEach(zone => zonesWrapper.append(redistGraphCreateZone(zone, zonesExpanded)));
    drawConnections();
}

function redistGraphCreateZone(zone, zonesExpanded) {
    var zoneWrapper = initZone(zone, zonesExpanded.has(zone.id));

    var deltaNodes = Object.keys(zone.nodes).map(nodeID => zone.nodes[nodeID]).filter(node => node.type != "stable");
    var stableNodes = Object.keys(zone.nodes).map(nodeID => zone.nodes[nodeID]).filter(node => node.type == "stable");

    if (deltaNodes.length > 0) {
        if (deltaNodes.length > 2 || stableNodes.length > 10) {
            if (selectedDeltas[zone.id] == null || zone.nodes[selectedDeltas[zone.id]] == null) {
                selectedDeltas[zone.id] = deltaNodes[Math.floor(deltaNodes.length / 2)].id;
            }
        } else {
            selectedDeltas[zone.id] = null;
        }
        redistMapCreateNodes(zoneWrapper.find('.deltaNodes'), zone, deltaNodes, true);
    }

    if (stableNodes.length > 0) {
        var l = stableNodes.length;
        if (l > 9) {
            var topNodes = stableNodes.slice(0, Math.ceil(l / 2));
            var bottomNodes = stableNodes.slice(Math.ceil(l / 2));
            redistMapCreateNodes(zoneWrapper.find('#stableNodes-top'), zone, topNodes, false);
            redistMapCreateNodes(zoneWrapper.find('#stableNodes-bottom'), zone, bottomNodes, false);
        } else {
            zoneWrapper.find('#stableNodes-top').remove();
            redistMapCreateNodes(zoneWrapper.find('#stableNodes-bottom'), zone, stableNodes, false);
        }
    }

    if (!zoneWrapper.hasClass('expanded')) {
        zoneWrapper.children('.content').hide();
    }

    createConnections(zone);

    return zoneWrapper;
}

function redistMapCreateNodes(nodesWrapper, zone, nodes, isDelta) {
    Object.keys(nodes).forEach(function (nodeIndex) {
        var node = nodes[nodeIndex];
        var errors = getNodeErrors(zone, node);
        nodesWrapper.append(createNode(zone, node, isDelta, errors)
            .on('click', _ => onNodeClick(zone.id, node.id)));
    });
}

function createNode(zone, node, isDelta, errors) {
    var nodeClass = "";
    if (errors.aborts > 0) {
        nodeClass = " node-aborts";
    } else if (errors.errors > 0) {
        nodeClass = " node-err";
    } else if (errors.drops > 0) {
        nodeClass = " node-drop";
    } else if (errors.expireds > 0) {
        nodeClass = " node-expired";
    }

    var txTypeClass = (isDelta ? node.type : '');
    var nodeDiv = $('<div>', { id: getNodeName(zone.id, node.id), class: 'node ' + txTypeClass + nodeClass })
        .append($('<i>', { class: "material-icons left", text: "storage" }).css({ 'padding-right': '5px' }))
        //.append($('<i>', { class: "fas fa-server left" }).css({ 'padding-right': '5px', 'font-size': '16px' }))
        .append($('<span>', { class: "host", text: getNodeName(zone.id, node.id) }))
        .append($('<span>', { class: "shards" }))
        .append($('<span>', { class: "aborts" }))
        .append($('<div>', { class: "progress" })
            .append($('<div>', { class: "progress-bar" })));
    attachToolTip(nodeDiv, `Errors: ${errors.errors}<br>Drops: ${errors.drops}<br>Expired: ${errors.expireds}`);
    setProgress(nodeDiv, zone, node);
    return nodeDiv
}

function setProgress(nodeDiv, zone, node) {
    var finished = 0;
    var aborted = 0;
    var total = 0;
    for (let shard of node.shards) {
        var s = zone.shards[shard];
        if (s != null) {
            total++;
            if (s.st == "F") {
                finished++;
            } else if (s.st == "A") {
                aborted++;
            }
        }
    }

    nodeDiv.children('.shards').text(`${finished} / ${total} shards`);
    if (aborted > 0) {
        nodeDiv.children('.aborts').text(`${aborted} aborted shards`);
    } else {
        nodeDiv.children('.aborts').hide();
    }
    var progress = Math.round((finished / total) * 100);
    nodeDiv.find('.progress-bar').css('width', `${progress}%`)
}

function createConnections(zone) {
    if (connections[zone.id] == null) {
        connections[zone.id] = [];
    }

    var stableNodes = Object.keys(zone.nodes).map(nodeID => zone.nodes[nodeID]).filter(x => x.type != "stable");
    if (stableNodes.length == 0) {
        return;
    }
    var deltaType = stableNodes[0].type;
    var connType = (deltaType == "added" ? "toDelta" : "fromDelta");

    var nodeConnections = zone.shards.reduce(function (conns, shard) {
        var connName = `${shard.source}-${shard.target}`;
        if (conns[connName] == null) {
            conns[connName] = {
                to: shard.target,
                from: shard.source,
                finished: 0,
                total: 0,
            };
        }
        conns[connName].total++;
        if (shard.st == "F") {
            conns[connName].finished++;
        }
        return conns;
    }, {});

    Object.keys(nodeConnections).forEach(function (connName) {
        var conn = nodeConnections[connName];
        connections[zone.id].push({
            to: conn.to,
            from: conn.from,
            label: `${conn.finished} / ${conn.total}`,
            progress: conn.finished / conn.total,
            type: connType,
        });
    });
}

function clearConnections() {
    connections = {};
}

function drawConnections() {
    if (!isSVGVisible()) {
        return;
    }
    clearSVG();

    Object.keys(selectedDeltas).forEach(function (zoneID) {
        if (connections == null || connections[zoneID] == null) {
            return;
        }
        if (selectedDeltas[zoneID] != null) {
            drawNodeConnections(zoneID, selectedDeltas[zoneID]);
        } else {
            var uniqueTargets = Array.from(new Set(connections[zoneID].map(c => c.to)));
            uniqueTargets.forEach(tgt => drawNodeConnections(zoneID, tgt, true));
        }
    });
}

function drawNodeConnections(zoneID, nodeID, onlyIfTarget) {
    var visibleConnections = connections[zoneID].filter(c => c.to == nodeID || (!onlyIfTarget && c.from == nodeID));

    var zoneWrapper = $('#' + zoneIDToDivID(zoneID));
    if (zoneWrapper.hasClass('expanded')) {
        visibleConnections.forEach(function (connection) {
            var fromName = getNodeName(zoneID, connection.from);
            var toName = getNodeName(zoneID, connection.to);
            var color = $('#' + (connection.type == "toDelta" ? toName : fromName)).css('border-left-color');
            connectDivs(fromName, toName, color, TENSION, connection.label, connection.progress);
        });
    }
}

// Appends a table to the given modal with the state of each shard being transferred to/from the given nodeID
function appendNodeShards(modal, zone, nodeID) {
    var distro = getNodeDistribution(zone, nodeID);

    // Try to split evenly into 2 tables but don't split across nodes (each node has multiple rows)
    var totalShards = Object.keys(distro.nodes).reduce((sum, node) => sum += distro.nodes[node].length, 0);
    var splits = [new Set(Object.keys(distro.nodes))];
    if (totalShards > 10 && distro.nodes.length > 1) {
        var nodes = Array.from(splits[0]).sort(((a, b) => a - b)).reverse();
        var splitSize = 0;
        splits[1] = new Set();
        for (let n of nodes) {

            if (distro.nodes[n].length + splitSize > totalShards / 2) {
                break;
            }
            splits[1].add(n);
            splits[0].delete(n);
            splitSize += distro.nodes[n].length;
        }
    }

    $(modal).append($('<h4>', { text: `Zone: ${zone.id}, Node: ${nodeID}` }))
        .append($('<h4>', { text: `${distro.txType.slice(0,1).toUpperCase() + distro.txType.slice(1)} shards:` }));

    var shardTableWrapper = $('<div>', { class: 'shardTableWrapper' });

    splits.forEach(function (splitNodes) {
        var shardTable = $('<table>', { class: 'shardTable' }).append(
            '<tr><th>Node</th><th>Status</th><th>Shard</th><th>Progress</th><th>Errors</th><th>Drops</th><th>Expired</th></tr>');
        splitNodes = Array.from(splitNodes).sort((a, b) => a - b);
        splitNodes.forEach(function (other) {
            if (distro.nodes[other] == null) {
                return;
            }

            // Display shards grouped by their current state (F, P, Q, A)
            var shards = distro.nodes[other].shards;

            var nodeRow = true;
            var totalNodeShards = distro.nodes[other].length;
            Object.keys(shards).forEach(function (status) {
                shards[status].sort(((a, b) => a - b));

                var statusRow = true;
                for (let shard of shards[status]) {
                    var tdClass = shardStToState(status).toLowerCase() + '-td';

                    var row = $("<tr>");
                    if (nodeRow) {
                        nodeRow = false;
                        row.append($("<td>", { text: other, rowspan: totalNodeShards }));
                    }

                    if (statusRow) {
                        statusRow = false;
                        row.append($("<td>", { class: tdClass, text: shardStToState(status), rowspan: shards[status].length }));
                    }

                    if (shard != null) {
                        var tds = `<td class="${tdClass}">${shard.id}</td><td class="${tdClass}">${shard.progress}%</td>`;
                        for (let e of ["err", "drop", "expired"]) {
                            var rate = (shard.totla == 0 || shard[e] == 0 ? 0 : (100 * (shard[e] / shard.total)).toFixed(2));
                            var tdClass = shard[e] > 0 ? ` class=${e}-td` : '';
                            tds += `<td${tdClass}>${rate}%</td>`;
                        }
                        row.append(tds);
                    } else {
                        row.append(`<td>${shard.id}</td><td></td><td></td><td></td><td></td>`);
                    }

                    shardTable.append(row);
                }

            });
        });

        shardTableWrapper.append(shardTable);
    });

    $(modal).append(shardTableWrapper);
}

// Appends a simple table to the modal with each node involved in transferring a shard to/from nodeID shown with a list of shards for each type of shard status
function appendNodeDistribution(modal, zone, nodeID) {
    var distro = getNodeDistribution(zone, nodeID);

    var otherNodesIDs = Object.keys(distro.nodes);
    var splits = [otherNodesIDs];
    var l = otherNodesIDs.length;
    if (l > 5) {
        if (l > 10) {
            var x = Math.floor(l / 3) + (l % 3 > 0);
            var y = x + Math.floor(l / 3) + (l % 3 > 1);
            splits = [otherNodesIDs.slice(0, x), otherNodesIDs.slice(x, y), otherNodesIDs.slice(y)];
        } else {
            splits = [otherNodesIDs.slice(0, l / 2), otherNodesIDs.slice(l / 2)];
        }
    }

    $(modal).append($('<h4>', { text: `Zone: ${zone.id}, Node: ${nodeID}` }))
        .append($('<h4>', { text: `${distro.txType.slice(0,1).toUpperCase() + distro.txType.slice(1)} shards:` }));

    var shardTableWrapper = $('<div>', { class: 'shardTableWrapper' });

    for (let split of splits) {
        var shardTable = $('<table>', { class: 'shardTable' }).append('<tr><th>Node</th><th>Status</th><th>Shards</th></tr>');
        split.forEach(function (other) {
            var shards = distro.nodes[other].shards;
            var first = true;
            Object.keys(shards).forEach(function (status) {
                shards[status].sort(((a, b) => a.id - b.id));
                var tds = `<td>${shardStToState(status)}</td><td>${shards[status].map(x => x.id).join(', ')}</td>`;
                if (first) {
                    shardTable.append($(`<tr><td rowspan="${Object.keys(shards).length}">${other}</td>${tds}</tr>`));
                    first = false;
                } else {
                    shardTable.append($(`<tr>${tds}</tr>`));
                }
            });
        });

        shardTableWrapper.append(shardTable);
    }

    $(modal).append(shardTableWrapper);
}

// Return mapping of each node transferring shards to/from node ID: other node ID => { length: # of shards being transferred to/from nodeID, shards => shard.st => [ shards ] }
function getNodeDistribution(zone, nodeID) {
    // Process all shards being transferred to/from nodeID
    var localShards = zone.shards.filter(x => x.target == nodeID || x.source == nodeID);
    var otherNodes = localShards.reduce(function (nodes, shard) {
        var otherNodeID = (shard.target == nodeID ? shard.source : shard.target);
        if (nodes[otherNodeID] == null) {
            nodes[otherNodeID] = {
                length: 0,
                shards: {},
            };
        }
        if (nodes[otherNodeID].shards[shard.st] == null) {
            nodes[otherNodeID].shards[shard.st] = [];
        }
        nodes[otherNodeID].shards[shard.st].push(shard);
        nodes[otherNodeID].length++;
        return nodes;
    }, {});

    var txType = (zone.nodes[nodeID].type == 'added' ? 'incoming' : 'outgoing');
    if (zone.nodes[nodeID].type == 'stable' && localShards.length > 0) {
        var otherNodeID = (localShards[0].target != nodeID ? localShards[0].target : localShards[0].source);
        txType = (zone.nodes[otherNodeID].type == 'added' ? 'outgoing' : 'incoming');
    }

    return {
        txType: txType,
        nodes: otherNodes,
    };
}

function getProgress(shardMap) {
    var finished = 0;
    var total = 0;

    for (let zone of shardMap.redist.zones) {
        for (let shard of zone.shards) {
            if (shard != null) {
                total++;
                if (shard.st == "F") {
                    finished++;
                }
            }
        }
    }

    return finished / total;
}

function onNodeClick(zoneID, nodeID) {
    if (selectedDeltas[zoneID] != null && selectedDeltas[zoneID] != nodeID) {
        selectedDeltas[zoneID] = nodeID;
        drawConnections();
    } else {
        showNodeShardsModal(zoneID, nodeID);
    }
}

function showNodeShardsModal(zoneID, nodeID) {
    modalFuncs.refresh = () => showNodeShardsModal(zoneID, nodeID);
    var modal = $('#modal > .juno-modal-content');
    clearModalContent(modal);
    var zone = shardMap.redist.zones.filter(zone => zone.id == zoneID)[0];
    appendNodeShards(modal, zone, nodeID);
    modal.parent().show();
}

function showNodeDistributionModal(zoneID, nodeID) {
    modalFuncs.refresh = () => showNodeDistributionModal(zoneID, nodeID);
    var modal = $('#modal > .juno-modal-content');
    clearModalContent(modal);
    var zone = shardMap.redist.zones.filter(zone => zone.id == zoneID)[0];
    appendNodeDistribution(modal, zone, nodeID);
    modal.parent().show();
}

function clearModalContent(modal) {
    modal.html('<span class="juno-modal-close">&times;</span>');
}


function refreshModal() {
    if (modalFuncs.refresh != null) {
        modalFuncs.refresh();
    }
}


function initZone(zone, isExpanded) {
    var zoneID = zoneIDToDivID(zone.id);
    if (zone.status == "redistributing") {
        isExpanded = true;
    }
    
    var zoneWrapper = $('<div>', {
        id: zoneID,
        class: `zone ${zone.status} ${isExpanded ? 'expanded' : 'collapsed'}`
    })
        .append($('<h3>', {
            class: 'expander',
            text: `Zone ${zone.id}: ${zone.status[0].toUpperCase()}${zone.status.slice(1)}`
        })
            .prepend($('<i>', { class: 'material-icons md-48' })));

    if (redistType == 'list') {
        zoneWrapper.append($('<div>', { class: 'content list' })
            .append($('<div>', { class: "nodes deltaNodes-list" }))
            .append($('<div>', { id: "stableNodes-top", class: "nodes stableNodes-list" }))
            .append($('<div>', { id: "stableNodes-bottom", class: "nodes stableNodes-list" })));
    } else if (redistType == 'graph') {
        zoneWrapper.append($('<div>', { class: 'content' })
            .append($('<div>', { id: "stableNodes-top", class: "nodes stableNodes" }))
            .append($('<div>', { class: "nodes deltaNodes" }))
            .append($('<div>', { id: "stableNodes-bottom", class: "nodes stableNodes" })));
    } else if (redistType == 'grid') {
        zoneWrapper.append($('<div>', { class: 'content grid' })
            .append($('<div>', { class: "nodes deltaNodes-list" })));
    }

    return zoneWrapper;
}


function getNodeErrors(zone, node) {
    var total = 0;
    var errors = 0;
    var drops = 0;
    var expireds = 0;
    var aborts = 0;

    for (let shard of node.shards) {
        var s = zone.shards[shard];
        if (s != null) {
            total += (s.total != null ? s.total : 0);
            errors += (s.err != null ? s.err : 0);
            drops += (s.drop != null ? s.drop : 0);
            expireds += (s.expired != null ? s.expired : 0);
            aborts += (s.st == "A" ? 1 : 0);
        }
    }
    return {
        total: total,
        errors: errors,
        drops: drops,
        expireds: expireds,
        aborts: aborts,
    };
}


/* LIST VIEW */

function renderList(expandedZones) {
    shardMap.redist.zones.forEach(zone => $('.zones').append(redistListCreateZone(zone, expandedZones)));
}

function redistListCreateZone(zone, expandedZones) {
    var zoneWrapper = initZone(zone, expandedZones.has(zone.id));

    var deltaNodes = Object.keys(zone.nodes).map(nodeID => zone.nodes[nodeID]).filter(node => node.type != "stable");
    var stableNodes = Object.keys(zone.nodes).map(nodeID => zone.nodes[nodeID]).filter(node => node.type == "stable");
    if (deltaNodes == null || deltaNodes.length == 0) {
        return zoneWrapper;
    }

    var deltas = zoneWrapper.find('.deltaNodes-list');
    var deltaType = deltaNodes[0].type.substr(0, 1).toUpperCase() + deltaNodes[0].type.substr(1);
    var deltaTable = $("<table>").append(
        `<tr><th colspan="4">${deltaType} Nodes</th></tr><th>Node</th><th>Progress</th><th>Pending</th><th>Errors</th><th>Drops</th><th>Expired</th></tr>`);
    for (let node of deltaNodes) {
        var finished = node.shards.map(shardID => zone.shards[shardID]).filter(shard => shard.st == "F").length;
        var pending = node.shards.map(shardID => zone.shards[shardID]).filter(shard => shard.st == "P").length;

        var errors = getNodeErrors(zone, node);

        deltaTable.append(`<tr><td>${getNodeName(zone.id, node.id)}</td><td>${finished} / ${node.shards.length}</td><td>${pending}</td>` +
            `<td>${errors.errors}</td><td>${errors.drops}</td><td>${errors.expireds}</td></tr>`);
    }
    deltas.append(deltaTable);

    var neighborType = (deltaNodes[0].type == "added" ? "target" : "source");
    var deltaType = (neighborType == "target" ? "source" : "target");
    var firstHeader = deltaType.substr(0, 1).toUpperCase() + deltaType.substr(1);
    var secondHeader = neighborType.substr(0, 1).toUpperCase() + neighborType.substr(1);

    // Try to split evenly into 2 tables but don't split across nodes (each node has multiple rows)
    var splits = [stableNodes.slice(0)]; // slice clones the array
    if (stableNodes.length > 5) {
        var nodeSizes = stableNodes.map(
            // For each stable node, get the number of unique neighbors of the shards that are being transferred from or to this node
            node => node.shards.reduce(
                (neighbors, shard) => {
                    neighbors.add(zone.shards[shard][neighborType]);
                    return neighbors;
                }, new Set()).size
        );
        var leftSize = nodeSizes.reduce((sum, size) => sum += size, 0);
        var rightSize = 0;
        splits[1] = [];

        for (var i = stableNodes.length - 1; i >= 0; i--) {
            var s = nodeSizes[i];
            if (leftSize - s < rightSize + s) {
                break;
            }
            splits[1].push(stableNodes[i]);
            splits[0].pop();
            leftSize -= s;
            rightSize += s;
        }
        splits[1] = splits[1].reverse();
    }

    var stableDivs = [zoneWrapper.find('#stableNodes-top'), zoneWrapper.find('#stableNodes-bottom')];
    for (var i = 0; i < splits.length; i++) {
        var stableTable = $("<table>").append(`<tr><th>${firstHeader}</th><th>Progress</th><th>${secondHeader}</th>` +
            `<th>Progress</th><th>Pending</th><th>Errors</th><th>Drops</th><th>Expired</th></tr>`);

        for (let node of splits[i]) {
            var progress = {
                finished: 0,
                total: 0
            };

            var neighborsProgress = zone.shards.filter(x => x[deltaType] == node.id).reduce((neighborsProgress, shard) => {
                var neighbor = shard[neighborType];
                if (neighborsProgress[neighbor] == null) {
                    neighborsProgress[neighbor] = {
                        finished: 0,
                        total: 0,
                        pending: 0,
                        errors: 0,
                        drops: 0,
                        expireds: 0,
                    };
                }
                neighborsProgress[neighbor].total++;
                progress.total++;
                if (shard.st == "F") {
                    neighborsProgress[neighbor].finished++;
                    progress.finished++;
                }
                neighborsProgress[neighbor].errors += shard.err;
                neighborsProgress[neighbor].drops += shard.drop;
                neighborsProgress[neighbor].expireds += shard.expired;
                return neighborsProgress;
            }, {});

            var neighbors = Object.keys(neighborsProgress).sort();
            neighbors.forEach(function (neighbor, index) {
                var rowPrefix = "<tr>";
                if (index == 0) {
                    rowPrefix = `<tr><td rowspan="${neighbors.length}">${node.id}</td>` +
                        `<td rowspan="${neighbors.length}">${progress.finished} / ${progress.total}</td>`;
                }

                var neighborProgress = neighborsProgress[neighbor];

                stableTable.append(rowPrefix + `<td>${neighbor}</td><td>${neighborProgress.finished} / ${neighborProgress.total}</td><td>${neighborProgress.pending}</td>` +
                    `<td>${neighborProgress.errors}</td><td>${neighborProgress.drops}</td><td>${neighborProgress.expireds}</td></tr>`);
            });
        }

        stableDivs[i].append(stableTable);
    }

    if (!zoneWrapper.hasClass('expanded')) {
        zoneWrapper.children('.content').hide();
    }

    return zoneWrapper;
}

/* GRID VIEW */

function renderGrid(expandedZones) {
    hideSVG();

    shardMap.redist.zones.forEach(zone => $('.zones').append(redistGridCreateZone(zone, expandedZones)));
}

function createGridNode(shardGrid, zone, node, errors) {
    var errRate = (errors.errors > 0 && errors.total > 0) ? (100 * (errors.errors / errors.total)).toFixed(1) : 0;
    var dropRate = (errors.drops > 0 && errors.total > 0) ? (100 * (errors.drops / errors.total)).toFixed(1) : 0;
    var expiredRate = (errors.expireds > 0 && errors.total > 0) ? (100 * (errors.expireds / errors.total)).toFixed(1) : 0;
    var nodeDiv = $('<div>', { id: getNodeName(zone.id, node.id), class: 'node' })
        .append($('<i>', { class: "material-icons left", text: "storage" }).css({ 'padding-right': '5px' }))
        //.append($('<i>', { class: "fas fa-server left" }).css({ 'padding-right': '5px', 'font-size': '16px' }))
        .append($('<span>', { class: "host", text: getNodeName(zone.id, node.id) }))
        .append($('<span>', { class: "shards" }))
        .append($('<span>', { class: "data", text: `Errors: ${errRate}%, Drops: ${dropRate}%, Expired: ${expiredRate}%` }))
        //.append($('<span>', { class: "data", text: `Err: ${errRate}%, Drop: ${dropRate}%, Exp: ${expiredRate}%` }))
        // .append($('<span>', { class: "data", text: `Errors: ${errRate}%` }))
        // .append($('<span>', { class: "data", text: `Drops: ${dropRate}%` }))
        // .append($('<span>', { class: "data", text: `Expired: ${expiredRate}%` }))
        .append($('<div>', { class: "progress" })
            .append($('<div>', { class: "progress-bar" })))
        .append(shardGrid);
    setProgress(nodeDiv, zone, node);
    return nodeDiv
}

var shardStateToClass = {
    "Q": "shard-queued",
    "P": "shard-pending",
    "F": "shard-finished",
    "A": "shard-aborted",
};
function redistGridCreateZone(zone, expandedZones) {
    var zoneWrapper = initZone(zone, expandedZones.has(zone.id));

    var deltaNodes = Object.keys(zone.nodes).map(nodeID => zone.nodes[nodeID]).filter(node => node.type != "stable");
    if (deltaNodes == null || deltaNodes.length == 0) {
        return zoneWrapper;
    }

    var maxShards = Math.max(deltaNodes.map(n => n.shards.length));
    var gridTempCols = "auto ".repeat(maxShards < 40 || deltaNodes.length > 2 ? 10 : 20);

    var deltas = zoneWrapper.find('.deltaNodes-list');

    for (let node of deltaNodes) {
        var shardGrid = $('<div>', { class: "grid-container", style: `grid-template-columns: ${gridTempCols}` });
        var errors = getNodeErrors(zone, node);

        for (let shard of node.shards) {
            var s = zone.shards[shard];
            shardClass = shardStateToClass["Q"];
            if (s != null) {
                shardClass = shardStateToClass[s.st];
                if (s.st == "P") {
                    for (let t of ["err", "drop", "expired"]) {
                        if (s[t] != null && s[t] > 0) {
                            shardClass += ` shard-${t}`;
                            break;
                        }
                    }
                }
            }

            var shardBox = $('<div>', { class: "grid-item shard-block " + shardClass, "data-shardID": shard });
            if (s != null) {
                var tip = `Shard: ${s.id}<br>Progress: ${s.progress}%<br>` + (node.type == "added" ? `Source: ${s.source}` : `Target: ${s.target}`) +
                    `<br>OK: ${s.ok} / ${s.total}<br>Errors: ${s.err}<br>Drops: ${s.drop}<br>Expired: ${s.expired}<br>et: ${s.et}`;
                attachToolTip(shardBox, tip);
            }
            shardGrid.append(shardBox);
        }

        var nodeDiv = createGridNode(shardGrid, zone, node, errors).on('click', _ => showNodeDistributionModal(zone.id, node.id));
        deltas.append(nodeDiv);
    }

    if (!zoneWrapper.hasClass('expanded')) {
        zoneWrapper.children('.content').hide();
    }

    return zoneWrapper;
}