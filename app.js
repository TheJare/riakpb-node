var net = require('net');

// -----------------------------
// Reading protocol buffers
// Only varints in the Riak protocol buffers are bools and uint8s.
// So we just use readVarInt to read all of them.
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

var decodeLoop = function(stream, cb, len) {
    if (len)
        len += stream.idx;
    else
        len = stream.len
    while (stream.idx < len) {
        var t = stream.buf[stream.idx++];
        var fieldnum = t >> 3;
        var type = t & 0x07;
        cb(type, fieldnum);
    }
}

// -----------------------------
// Parsing Riak responses

var decode_RpbErrorResp  = function(stream, res) {
    res = res || {errmsg:"", errcode:0};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.errmsg = readString(stream);
        } else if (fieldnum == 2 && type == 0) {
            res.errcode = readVarInt(stream);
        }
    });
    return res;
}

var decode_RpbGetClientIdResp = function(stream, res) {
    res = res || {client_id:""};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.client_id = readString(stream);
        }
    });
    return res;
}

var decode_RpbGetServerInfoResp  = function(stream, res) {
    res = res || {node:"", server_version:""};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.node = readString(stream);
        } else if (fieldnum == 2 && type == 2) {
            res.server_version = readString(stream);
        }
    });
    return res;
}

var decode_RpbGetResp = function(stream, res) {
    res = res || {content:[], vclock:"", unchanged:true};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.content.push(decode_RpbContent(stream));
        } else if (fieldnum == 2 && type == 2) {
            res.vclock = readString(stream);
        } else if (fieldnum == 3 && type == 0) {
            res.unchanged = readVarInt(stream);
        }
    });
    return res;
}

var decode_RpbPutResp = function(stream, res) {
    res = res || {content:[], vclock:"", key:""};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.content.push(decode_RpbContent(stream));
        } else if (fieldnum == 2 && type == 2) {
            res.vclock = readString(stream);
        } else if (fieldnum == 3 && type == 2) {
            res.key = readString(stream);
        }
    });
    return res;
}

var decode_RpbListBucketsResp = function(stream, res) {
    res = res || {buckets:[]};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.buckets.push(readString(stream));
        }
    });
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

var decode_RpbGetBucketResp = function(stream, res) {
    res = res || {props : {}};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.props = decode_RpbBucketProps(stream);
        }
    });
    return res;
}

var decode_RpbBucketProps = function(stream, res) {
    var len = readVarInt(stream);
    res = res || {n_val : 0, allow_mult: false};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 0) {
            res.n_val = readVarInt(stream);
        } else if (fieldnum == 2 && type == 0) {
            res.allow_mult = readVarInt(stream);
        }
    }, len);
    return res;
}

var decode_RpbPair = function(stream, res) {
    var len = readVarInt(stream);
    res = res || {key:"", value : "" };
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.key = readString(stream);
        } else if (fieldnum == 2 && type == 2) {
            res.value = readString(stream);
        }
    }, len);
    console.log("pair: ", res);
    return res;
}

var decode_RpbLink = function(stream, res) {
    var len = readVarInt(stream);
    res = res || {bucket: "", key:"", tag : "" };
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.bucket = readString(stream);
        } else if (fieldnum == 2 && type == 2) {
            res.key = readString(stream);
        } else if (fieldnum == 3 && type == 2) {
            res.tag = readString(stream);
        }
    }, len);
    console.log("link: ", res);
    return res;
}

var decode_RpbContent = function(stream, res) {
    var len = readVarInt(stream);
    res = res || {value : "", content_type:"", charset:"", content_encoding:"", vtag:"", links:[], last_mod:0, last_mod_usecs:0, usermeta:[], indexes:[] };
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.value = readString(stream);
        } else if (fieldnum == 2 && type == 2) {
            res.content_type = readString(stream);
        } else if (fieldnum == 3 && type == 2) {
            res.charset = readString(stream);
        } else if (fieldnum == 4 && type == 2) {
            res.content_type = readString(stream);
        } else if (fieldnum == 5 && type == 2) {
            res.vtag = readString(stream);
        } else if (fieldnum == 6 && type == 2) {
            res.links.push(decode_RpbLink(stream));
        } else if (fieldnum == 7 && type == 0) {
            res.last_mod = readVarInt(stream);
        } else if (fieldnum == 8 && type == 0) {
            res.last_mod_usecs = readVarInt(stream);
        } else if (fieldnum == 9 && type == 2) {
            res.usermeta.push(decode_RpbPair(stream));
        } else if (fieldnum == 10 && type == 2) {
            res.indexes.push(decode_RpbPair(stream));
        }
    }, len);
    return res;
}

// ----------------------
// Writing protocol buffers

var addMessageHeader = function(buf, len, msg) {
    buf[0] = (len >> 24) & 0xFF;
    buf[1] = (len >> 16) & 0xFF;
    buf[2] = (len >>  8) & 0xFF;
    buf[3] = (len      ) & 0xFF;
    buf[4] = msg;
}

var addVarInt = function(buf, idx, value) {
    while (value >= 0x80) {
        buf[idx++] = (value & 0x7F);
        value = value >> 7;
    }
    buf[idx++] = (value & 0x7F);
    return idx;
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
        } else if (type == "object") {
            buf[idx] = ((i+1) << 3) + 2;
            var tempv = new Buffer(4096);
            var l = addParameters(tempv, 0, p);
            idx = addVarInt(buf, idx+1, l);
            tempv.copy(buf, idx, 0, l);
            idx += l;
        } else if (type == "boolean") {
            buf[idx] = (i+1) << 3;
            buf[idx+1] = p? 1 : 0;
            idx += 2;
        } else if (type == "number") {
            buf[idx] = (i+1) << 3;
            idx = addVarInt(buf, idx+1, p);
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

// ----------------------
// Encoding Riak requests

var encode_PingReq = function() {
    return new Buffer([0,0,0,1,1]);
}

var encode_GetClientIdReq = function() {
    return new Buffer([0,0,0,1,3]);
}

var encode_SetClientIdReq = function(client_id) {
    return makeMessage(5, [client_id]);
}

var encode_GetServerInfoReq = function() {
    return new Buffer([0,0,0,1,7]);
}

var encode_GetReq = function(bucket, key, options) {
    options = options || {};
    return makeMessage(9, [bucket, key, options.r, options.pr, options.basic_quorum, options.notfound_ok, options.if_modified, options.head, options.deletedvclock]);
}

var encode_PutReq = function(bucket, key, data, options) {
    options = options || {};
    options.content = options.content || {};
    return makeMessage(11, [bucket, key, options.vclock,
        [data, options.content.content_type, options.content.charset, options.content.content_encoding, options.content.vtag, options.content.links,
         options.content.last_mod, options.content.last_mod_usecs, options.content.usermeta, options.content.indexes ],
        options.w, options.dw, options.return_body, options.pw, options.if_not_modified, options.if_none_match, options.return_head]);
}

var encode_PutNokeyReq = function(bucket, data, options) {
    return encode_PutReq(bucket, undefined, data, options);
}

var encode_DeleteReq = function(bucket, key, options) {
    options = options || {};
    return makeMessage(11, [bucket, key, options.rw, options.vclock, options.r, options.w, options.pr, options.pw, options.dw ]);
}

var encode_ListBucketsReq = function() {
    return new Buffer([0,0,0,1,15]);
}

var encode_ListKeysReq = function(bucket) {
    return makeMessage(17, [bucket]);
}

var encode_GetBucketReq = function(bucket) {
    return makeMessage(19, [bucket]);
}

var encode_SetBucketReq = function(bucket, props) {
    return makeMessage(21, [bucket, [props.n_val, props.allow_mult]]);
}

// ---------------------------
// Test program
// ---------------------------
var res; // Response accumulated over multiple packets. Only used for decode_RpbListKeysResp
var cbs = []; // Queue of callbacks to requests

var clientWrite = function(client, data, cb) {
    //console.log("sending");
    cbs.push(cb);
    client.write(data);
}

var client = net.connect(8087, 'localhost', function() { //'connect' listener
    console.log('client connected');
    client.setKeepAlive(true);

    clientWrite(client, encode_GetServerInfoReq(), function(err, res) { console.log("respuesta al encode_GetServerInfoReq"); });
    clientWrite(client, encode_GetClientIdReq(), function(err, res) { console.log("respuesta al encode_GetClientIdReq"); });
    clientWrite(client, encode_SetClientIdReq("Jarete"), function(err, res) { console.log("respuesta al encode_SetClientIdReq"); });
    clientWrite(client, encode_GetClientIdReq(), function(err, res) { console.log("respuesta al encode_GetClientIdReq"); });
    clientWrite(client, encode_ListBucketsReq(), function(err, res) { console.log("respuesta al encode_ListBucketsReq"); });
    clientWrite(client, encode_GetBucketReq("users"), function(err, res) { console.log("respuesta al encode_GetBucketReq de los users"); });
    clientWrite(client, encode_SetBucketReq("users", {allow_mult:false, n_val:4}), function(err, res) { console.log("respuesta al encode_SetBucketReq de los users"); });
    clientWrite(client, encode_GetBucketReq("users"), function(err, res) { console.log("respuesta al encode_GetBucketReq de los users"); });
    clientWrite(client, encode_ListKeysReq("users"), function(err, res) { console.log("respuesta a los users"); });
    clientWrite(client, encode_ListKeysReq("flights"), function(err, res) { console.log("respuesta a los flights"); });
    clientWrite(client, encode_GetReq("flights", "KLM-5034"), function(err, res) { console.log("respuesta al get KLM"); });
    clientWrite(client, encode_GetReq("users", "jarelol"), function(err, res) { console.log("respuesta al get KLM"); });

    clientWrite(client, encode_PutReq("users", "nodetestuser", "Hola soy yo"), function(err, res) { console.log("respuesta al put nodetestuser"); });
    clientWrite(client, encode_GetReq("users", "nodetestuser"), function(err, res) { console.log("respuesta al get nodetestuser"); });

    clientWrite(client, encode_PutNokeyReq("users", "Autogen Key", {return_body:true}), function(err, res) { console.log("respuesta al put Autogen key"); });

    clientWrite(client, encode_PingReq("users", "nodetestuser"), function(err, res) { client.end(); });
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
        var err = null;
        if (msg == 0) {
            res = decode_RpbErrorResp(stream, res);
            err = res;
            res = null;
        } else if (msg == 2) {
            res = {done:true}; // nothing to decode for RpbPingResp 
        } else if (msg == 4) {
            res = decode_RpbGetClientIdResp(stream, res);
        } else if (msg == 6) {
            res = {done:true}; // nothing to decode for RpbSetClientIdResp 
        } else if (msg == 8) {
            res = decode_RpbGetServerInfoResp(stream, res);
        } else if (msg == 10) {
            res = decode_RpbGetResp(stream, res);
        } else if (msg == 12) {
            res = decode_RpbPutResp(stream, res);
        } else if (msg == 14) {
            res = {done:true}; // nothing to decode for RpbDelResp
        } else if (msg == 16) {
            res = decode_RpbListBucketsResp(stream, res);
        } else if (msg == 18) {
            res = decode_RpbListKeysResp(stream, res);
        } else if (msg == 20) {
            res = decode_RpbGetBucketResp(stream, res);
        } else if (msg == 22) {
            res = {done:true}; // nothing to decode for RpbSetBucketResp
        } else {
            stream.idx += len;
        }
        if (!res || res.done === undefined || res.done === true) {
            var cb = cbs.shift();
            if (cb) cb(err, res);
            console.log(err? err : res);
            res = undefined;
        }
    }
});

client.on('end', function() {
    console.log('client disconnected');
});
