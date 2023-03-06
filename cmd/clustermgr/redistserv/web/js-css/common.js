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
  
function parseDateTime(time) {
    if (time.indexOf("/") < 0) {
        var t = parseInt(time);
        if (t < 1000000000000) {
            t *= 1000;
        }
        return t;
    }
    var datetime = time.split('-');
    var date = new Date(datetime[0]);
    var timeElements = datetime[1].split(':');
    date.setHours(timeElements[0]);
    date.setMinutes(timeElements[1]);
    date.setSeconds(timeElements[2]);
    return date.getTime();
}

function getURLParam(url, param, defaultVal) {
    var v = url.searchParams.get(param);
    return (v == null) ? defaultVal : v;
}

// Attaches tooltip show and hide functions to the element with the given toolTipMsg.
function attachToolTip(elem, toolTipMsg) {
    elem.mouseenter(function (e) {
        var toolTip = $('#juno-tooltip');
        var toolTipText = toolTip.find('#juno-tooltiptext');
        toolTip.show();
        toolTipText.attr('display', 'none'); // So we can use width() and height()
        toolTipText.html(toolTipMsg);
        var x = e.pageX + 20;
        var y = e.pageY + 20;
        if (x < toolTipText.width()) {
            x += toolTipText.width();
        }
        if (y + toolTipText.height() > $(window).height() - 40) {
            y = e.pageY - 20 - toolTipText.height();
        }
        toolTip.css('top', y + 'px');
        toolTip.css('left', x + 'px');
        toolTip.show();
    });

    elem.mouseleave(function (e) {
        $('#juno-tooltip').hide();
    });

    return elem;
}

function zoneIDToDivID(zoneID) {
    return `zone-${zoneID}`;
}

function divIDToZoneID(divID) {
    var f = divID.split("-");
    return parseInt(f[f.length-1]);
}

var statusSortOrder = ["redistributing", "queued", "aborted", "finished"];
function sortZonesByStatus(a, b) {
    var aPos = statusSortOrder.indexOf(a.status);
    var bPos = statusSortOrder.indexOf(b.status);

    if (aPos != bPos) {
        return aPos - bPos;
    } else {
        return a.nodeID - b.nodeID;
    }
}

var mapShardStToState = {
    "Q": "Queued",
    "P": "Pending",
    "F": "Finished",
    "A": "Aborted",
};
function shardStToState(st) {
    return mapShardStToState[st];
}