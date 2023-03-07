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
  
// https://www.beyondjava.net/how-to-connect-html-elements-with-an-arrow-using-svg

const MIN_OPACITY = 0.15;

function findAbsolutePosition(htmlElement) {
    var x = htmlElement.offsetLeft;
    var y = htmlElement.offsetTop;
    for (var x = 0, y = 0, el = htmlElement;
        el != null;
        el = el.offsetParent) {
        x += el.offsetLeft;
        y += el.offsetTop;
    }
    return {
        "x": x,
        "y": y
    };
}

function drawCurvedLine(id, x1, y1, x2, y2, color, tension, opacity) {
    var svg = document.getElementById('svg-canvas');
    var shape = document.createElementNS("http://www.w3.org/2000/svg", "path");

    var delta = (x2 - x1) * tension;
    var hx1 = x1 + delta;
    var hy1 = y1;
    var hx2 = x2 - delta;
    var hy2 = y2;
    var path = "M " + x1 + " " + y1 + " C " + hx1 + " " + hy1 + " " + hx2 + " " + hy2 + " " + x2 + " " + y2;
    shape.setAttributeNS(null, "id", id);
    //shape.setAttributeNS(null, "class", "node-path");
    shape.setAttributeNS(null, "d", path);
    shape.setAttributeNS(null, "fill", "none");
    shape.setAttributeNS(null, "stroke", color);
    shape.setAttributeNS(null, "stroke-width", 2);
    shape.setAttributeNS(null, "stroke-opacity", opacity);
    svg.appendChild(shape);
}

function addArrows(id, points, color, opacity) {
    var svg = document.getElementById('svg-canvas');
    var text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttributeNS(null, "style", `font-size:19px;fill:${color};dominant-baseline:middle;font-weight:bold`);
    text.setAttributeNS(null, "fill-opacity", opacity);
    var offset = 100 / (points + 2);
    for (var pos = offset; pos < 100; pos += offset) {
        var textPath = document.createElementNS("http://www.w3.org/2000/svg", "textPath");
        textPath.setAttributeNS("http://www.w3.org/1999/xlink", "xlink:href", "#" + id);
        textPath.setAttributeNS(null, "startOffset", pos + "%");
        var textNode = document.createTextNode(">");
        textPath.appendChild(textNode);
        text.appendChild(textPath);
    }
    svg.appendChild(text);
}

function addLabel(id, label, x1, y1, x2, y2) {
    var svg = document.getElementById('svg-canvas');
    var text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    var textPath = document.createElementNS("http://www.w3.org/2000/svg", "textPath");

    textPath.setAttributeNS("http://www.w3.org/1999/xlink", "xlink:href", "#" + id);
    if (x1 <= x2) {
        textPath.setAttributeNS(null, "startOffset", "25%");
    } else {
        textPath.setAttributeNS(null, "startOffset", "60%");
        textPath.setAttributeNS(null, "side", "right");
    }
    text.setAttributeNS(null, "dy", -5);

    var textNode = document.createTextNode(label);
    textPath.appendChild(textNode);
    text.appendChild(textPath);

    svg.appendChild(text);
}

function connectDivs(firstId, secondId, color, tension, text, progress) {
    var first = document.getElementById(firstId);
    var second = document.getElementById(secondId);

    if (first == null) {
        console.log(firstId);
    }
    if (second == null) {
        console.log(secondId);
    }

    var firstPos = findAbsolutePosition(first);
    var x1 = firstPos.x + (first.offsetWidth / 2);
    var y1 = firstPos.y;

    var secondPos = findAbsolutePosition(second);
    var x2 = secondPos.x + (second.offsetWidth / 2);
    var y2 = secondPos.y;

    if (y1 > y2) {
        y2 += second.offsetHeight;
    } else {
        y1 += first.offsetHeight;
    }

    var svg = $('#svg-canvas');
    if (svg.height() < Math.max(y1, y2)) {
        svg.height(Math.max(y1, y2));
    }

    var opacity = MIN_OPACITY + (1 - MIN_OPACITY) * (1 - progress);
    opacity = 1;

    drawCurvedLine(firstId + secondId, x1, y1, x2, y2, color, tension, opacity);
    addArrows(firstId + secondId, 4, color, opacity);
    addLabel(firstId + secondId, text, x1, y1, x2, y2);
}

function clearSVG() {
    var svg = document.getElementById('svg-canvas');
    while (svg.lastChild) {
        svg.removeChild(svg.lastChild);
    }
}

function hideSVG() {
    $('#svg-canvas').hide();
}

function showSVG() {
    $('#svg-canvas').show();
}

function isSVGVisible() {
    return $('#svg-canvas').is(":visible");
}