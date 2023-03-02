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