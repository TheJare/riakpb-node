var net = require('net');

// Only varints in the Riak protocol buffers are bools and uint8s.
// So we just use this to read them.
var readVarInt = function(stream) {
    var idx = stream.idx;
    var buf = stream.buf;
    var c = 0;
    var nb = 0;
    do {
        var b = buf[idx++];
        c = c + ((b & 0x7F) << nb);
        nb += 7;
    } while ((b & 0x80) != 0);
    stream.idx = idx;
    return c;
}

var readString = function(stream) {
    var l = readVarInt(stream);
    var s = stream.buf.toString('utf8', stream.idx, stream.idx+l);
    stream.idx += l;
    return s;
}

var readValue = function(type, stream) {
    if (type == 0) {
        return readVarInt(stream);
    } else if (t == 2) {
        return readString(stream);
    }
}

var decodeLoop = function(stream, cb) {
    while (stream.idx < stream.len) {
        var t = stream.buf[stream.idx++];
        var fieldnum = t >> 3;
        var type = t & 0x07;
        cb(type, fieldnum);
    }
}

var decode_RpbGetServerInfoReq  = function(stream, res) {
    res = res || {node:"", server_version:"", done : false};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.node = readString(stream);
        } else if (fieldnum == 2 && type == 2) {
            res.server_version = readString(stream);
        }
    });
    res.done = true;
    return res;
}

var decode_RpbListBucketsResp = function(stream, res) {
    res = res || {buckets:[], done : false};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.buckets.push(readString(stream));
        }
    });
    res.done = true;
    return res;
}

var decode_RpbListKeysResp = function(stream, res) {
    res = res || {keys:[], done : false};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.keys.push(readString(stream));
        } else if (fieldnum == 2 && type == 0) {
            res.done = true;
        }
    });
    return res;
}

var addMessageHeader = function(buf, len, msg) {
    buf[0] = (len >> 24) & 0xFF;
    buf[1] = (len >> 16) & 0xFF;
    buf[2] = (len >>  8) & 0xFF;
    buf[3] = (len      ) & 0xFF;
    buf[4] = msg;
}

var addParameters = function(buf, idx, params) {
    var n = params.length;
    for (var i = 0; i < n; i++) {
        var p = params[i];
        var type = typeof p;
        if (type == "string") {
            buf[idx] = ((i+1) << 3) + 2;
            buf[idx+1] = p.length;
            buf.write(p, idx+2, p.length, 'utf8');
            idx += 2 + p.length;
        } else if (type == "boolean") {
            buf[idx] = (i+1) << 3;
            buf[idx+1] = p? 1 : 0;
            idx += 2;
        }
    }
    return idx;
}

var makeMessage = function(msg, params) {
    var b0 = new Buffer(4096);
    var idx = 5;
    if (params) idx = addParameters(b0, idx, params);
    addMessageHeader(b0, idx - 4, msg);
    return b0.slice(0, idx);
}

var encode_ListBucketsReq = function() {
    return new Buffer([0,0,0,1,15]);
}

var encode_GetServerInfoReq = function() {
    return new Buffer([0,0,0,1,7]);
}

var encode_ListKeysReq = function(bucket) {
    return makeMessage(17, [bucket]);
}


// ---------------------------
var res;
var cbs = [];

var clientWrite = function(client, data, cb) {
    //console.log("sending");
    cbs.push(cb);
    client.write(data);
}

var client = net.connect(18087, 'localhost', function() { //'connect' listener
    console.log('client connected');
    client.setKeepAlive(true);

    clientWrite(client, encode_GetServerInfoReq(), function(res) { console.log("respuesta al encode_GetServerInfoReq"); });
    clientWrite(client, encode_ListBucketsReq(), function(res) { console.log("respuesta al encode_ListBucketsReq"); });
    clientWrite(client, encode_ListKeysReq("users"), function(res) { console.log("respuesta a los users"); });
    clientWrite(client, encode_ListKeysReq("flights"), function(res) { console.log("respuesta a los flights"); client.end(); });
});

client.on('data', function(data) {
    console.log(data.toString('hex'));
    var stream = {buf:data, idx:0};
    while (stream.idx < data.length) {
        var len = data.readUInt32BE(stream.idx);
        var msg = data.readUInt8(stream.idx+4);
        stream.idx += 5;
        stream.len = stream.idx + len-1;
        console.log("RECEIVED: message " + msg + ", length " + len);
        if (msg == 8) {
            res = decode_RpbGetServerInfoReq(stream, res);
        } else if (msg == 16) {
            res = decode_RpbListBucketsResp(stream, res);
        } else if (msg == 18) {
            res = decode_RpbListKeysResp(stream, res);
        } else {
            stream.idx += len;
        }
        if (res && res.done) {
            var cb = cbs.shift();
            if (cb) cb(res);
            console.log(res);
            res = undefined;
        }
    }
});

client.on('end', function() {
    console.log('client disconnected');
});
