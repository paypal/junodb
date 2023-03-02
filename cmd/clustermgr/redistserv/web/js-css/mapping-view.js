const mappingRender = {
    title: "Shard Mapping",
    buttons: "mapping",
    buttonHandler: mappingButtonHandler,
    updateData: handleMapTypeRendering,
    getProgress: null,
};
const ADD_HOST_COLORS = true;
const PRIMARY_ALPHA = 0.9;
const BACKUP_ALPHA = 0.3;
const MAX_COLOR_VALUE = 230;
const MIN_COLOR_VALUE = 30;
const COLOR_VALUE_RANGE = 150; // Must be at most MAX_COLOR_VALUE - MIN_COLOR_VALUE

var modalFuncs = { refresh: null };
var mapType = "zone";
var cachedColors;
var cachedIsDarkmode;
var cachedNumColors = -1;


function mappingButtonHandler(btn, renderFunc) {
    var txt = btn.text().toLowerCase();
    if (txt.indexOf("zone") >= 0) {
        mapType = "zone";
    } else if (txt.indexOf("shard") >= 0) {
        mapType = "shard";
    }

    renderFunc();
};


function handleMapTypeRendering(shardMap, resetZones, expandedZones, expandAllZones) {
    hideSVG();
    resetZones();

    if (mapType == "zone") {
        renderByZone(shardMap, expandedZones, expandAllZones);
    } else if (mapType == "shard") {
        renderByShard(shardMap);
    }
}


function renderByShard(shardMap) {
    var shards = [];
    var numHosts = Object.keys(shardMap.mapping.zones).map(zoneID => Object.keys(shardMap.mapping.zones[zoneID].nodes).length).reduce((sum, v) => sum + v, 0);
    var colors = getSegmentedColors(numHosts);
    var colorIdx = 0;
    var ipportToColor = {};

    Object.keys(shardMap.mapping.zones).forEach(function (zoneID) {
        var zone = shardMap.mapping.zones[zoneID];
        Object.keys(zone.nodes).forEach(function (nodeID) {
            var node = zone.nodes[nodeID];
            Object.keys(node.shards).forEach(function (shardType) {
                node.shards[shardType].forEach(function (shard) {
                    if (shards[shard] == null) {
                        shards[shard] = [];
                    }
                    if (ipportToColor[node.ipport] == null) {
                        ipportToColor[node.ipport] = colors[++colorIdx];
                    }
                    shards[shard][zoneID] = { isPrimary: shardType == 'primary', ipport: node.ipport, color: ipportToColor[node.ipport] };
                });
            });
        });
    });

    if (shards.length == 0 || shards[0].length == 0) {
        console.log(shardMap);
        console.log(shards);
        console.error("Empty shards list")
        return;
    }

    var zonesWrapper = $('.zones');
    zonesWrapper.addClass("shardTableByShard");

    if (shards.length <= 30) {
        var shardTable = $('<table>');
        populateShardMappingTable(shardTable, shards, 0);
        zonesWrapper.append(shardTable);
    } else {
        var shardTable1 = $('<table>');
        populateShardMappingTable(shardTable1, shards.slice(0, shards.length / 2), 0);
        zonesWrapper.append(shardTable1);

        var shardTable2 = $('<table>');
        populateShardMappingTable(shardTable2, shards.slice(shards.length / 2), shards.length / 2);
        zonesWrapper.append(shardTable2);
    }
}


function populateShardMappingTable(shardTable, shards, startingIndex) {
    shardTable.append(
        "<tr><th>Shard</th><th>Zone " + shards[0].map((_, index) => index).join("</th><th>Zone ") + "</th></tr>");

    shards.forEach(function (shard, index) {
        var row = $("<tr>");
        row.append(`<td>${index + startingIndex}</td>`);
        shard.forEach(function (host) {
            var css = {};
            if (ADD_HOST_COLORS) {
                css = {
                    background: `rgba(${host.color}, ${host.isPrimary ? PRIMARY_ALPHA : BACKUP_ALPHA})`,
                };
            } else if (host.isPrimary) {
                css = {
                    background: "rgba(var(--font-color), 0.2)"
                };
            }
            row.append($("<td>", {
                class: (host.isPrimary ? "primary" : "backup"),
                text: host.ipport,
                css: css,
            }));
        });
        shardTable.append(row);
    });
}


function renderByZone(shardMap, expandedZones, expandAllZones) {
    var zonesWrapper = $('.zones');
    shardMap.mapping.zones.forEach(zone => zonesWrapper.append(mappingCreateZone(zone, expandedZones.has(zone.id) || expandAllZones, shardMap.mapping.algVersion)));
    refreshModal();
}


function mappingCreateZone(zone, isExpanded, algVersion) {
    var zoneWrapper = $('<div>', {
        id: zoneIDToDivID(zone.id),
        class: `zone mapping ${isExpanded ? 'expanded' : 'collapsed'}`
    })
        .append($('<h3>', {
            class: 'expander',
            text: `Zone ${zone.id}`
        })
            .prepend($('<i>', { class: 'material-icons md-48' })))
        .append($('<div>', { class: 'content' })
            .append($('<div>', { class: 'nodes' })));

    mappingCreateNodes(zoneWrapper.find('.nodes'), zone.id, zone.nodes, algVersion);

    if (!isExpanded) {
        zoneWrapper.children('.content').hide();
    }

    return zoneWrapper;
}


function mappingCreateNodes(nodesWrapper, zoneID, nodes, algVersion) {
    Object.keys(nodes).forEach(nodeID => nodesWrapper.append(mappingCreateNode(zoneID, nodes[nodeID], algVersion)));
}


function mappingCreateNode(zoneID, node, algVersion) {
    var nodeName = getNodeName(zoneID, node.id);
    var nodeDiv = $('<div>', { id: nodeName, class: "node" }).on('click', _ => mappingShowNodeModal(zoneID, node, algVersion))
        .append($('<i>', { class: "material-icons left", text: "storage" }).css({ 'padding-right': '5px' }))
        .append($('<span>', { class: "host", text: nodeName }));
    if (algVersion < 2) {
        nodeDiv.append($('<span>', { class: "data", text: `Primaries: ${node.shards.primary.length}` }))
            .append($('<span>', { class: "data", text: `Backups: ${node.shards.backup.length}` }));
    } else {
        nodeDiv.append($('<span>', { class: "data", text: `Shards: ${node.shards.primary.length}` }));
    }
    return nodeDiv
}


function mappingAppendNodeShards(modal, zoneID, node, algVersion) {
    $(modal).append($('<h4>', { text: `Zone: ${zoneID}, Node: ${node.id}` }))
        .append($('<span>', { text: node.ipport })).append('<br>');

    var shardTable = $('<table>', { class: 'shardTable' });
    if (algVersion < 2) {
        shardTable.append(`<tr><td>Primaries</td><td>${node.shards.primary.join(', ')}</td>`)
            .append(`<tr><td>Backups</td><td>${node.shards.backup.join(', ')}</td>`);
    } else {
        shardTable.append(`<tr><td>Shards</td><td>${node.shards.primary.join(', ')}</td>`)
    }

    $(modal).append(shardTable);
}


function mappingShowNodeModal(zoneID, node, algVersion) {
    modalFuncs.refresh = () => mappingShowNodeModal(zoneID, node, algVersion);
    var modal = $('#modal > .juno-modal-content');
    clearModalContent(modal);
    mappingAppendNodeShards(modal, zoneID, node, algVersion);
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


function getSegmentedColors(numColors) {
    var darkmode = $('body').hasClass('darkmode');
    if (numColors == cachedNumColors && darkmode == cachedIsDarkmode) {
        return cachedColors;
    }

    // Need enough unique colors for numColors. Colors should be visually distinct (as possible).
    // Therefore, generate a set of possible values to use for red, green, and blue, that are as evenly spaced apart as possible.
    // If there are x values to generate red, green, and blue from, there are x^3 unique colors.
    // Find x given numColors such that x^3 >= y, or cube root of y <= x.
    var x = Math.ceil(Math.cbrt(numColors));

    // Split range of rgb into x values, but avoid colors that might be difficult to distinguish with the background or font.
    var interval = Math.floor(COLOR_VALUE_RANGE / x);
    var values = Array.apply(null, Array(x)).map((_, index) => index * interval + (darkmode ? MIN_COLOR_VALUE : MAX_COLOR_VALUE - COLOR_VALUE_RANGE));

    // Generate colors from values
    var colors = new Set();
    values.forEach(function (r) {
        values.forEach(function (g) {
            values.forEach(function (b) {
                colors.add(`${r}, ${g}, ${b}`);
            });
        });
    });

    cachedColors = Array.from(colors);
    cachedNumColors = numColors;
    cachedIsDarkmode = darkmode;

    return cachedColors;
}