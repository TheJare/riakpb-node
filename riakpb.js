// ---------------------------
// Riakpb module
//
// Copyright by Javier Arevalo in 2012.
// - http://www.iguanademos.com/Jare/
// - @TheJare on twitter
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
// ---------------------------

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

var decode_ErrorResp  = function(stream, res) {
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

var decode_GetClientIdResp = function(stream, res) {
    res = res || {client_id:""};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.client_id = readString(stream);
        }
    });
    return res;
}

var decode_GetServerInfoResp  = function(stream, res) {
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

var decode_GetResp = function(stream, res) {
    res = res || {content:[], vclock:"", unchanged:true};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.content.push(decode_Content(stream));
        } else if (fieldnum == 2 && type == 2) {
            res.vclock = readString(stream);
        } else if (fieldnum == 3 && type == 0) {
            res.unchanged = readVarInt(stream);
        }
    });
    return res;
}

var decode_PutResp = function(stream, res) {
    res = res || {content:[], vclock:"", key:""};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.content.push(decode_Content(stream));
        } else if (fieldnum == 2 && type == 2) {
            res.vclock = readString(stream);
        } else if (fieldnum == 3 && type == 2) {
            res.key = readString(stream);
        }
    });
    return res;
}

var decode_ListBucketsResp = function(stream, res) {
    res = res || {buckets:[]};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.buckets.push(readString(stream));
        }
    });
    return res;
}

var decode_ListKeysResp = function(stream, res) {
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

var decode_GetBucketResp = function(stream, res) {
    res = res || {props : {}};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.props = decode_BucketProps(stream);
        }
    });
    return res;
}

var decode_MapReduceResp = function(stream, res) {
    res = res || {phase:0, response:[], done:false};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 0) {
            res.phase = readVarInt(stream);
        } else if (fieldnum == 2 && type == 2) {
            res.response.push(readString(stream));
        } else if (fieldnum == 3 && type == 0) {
            res.done = true;
        }
    });
    return res;
}

var decode_BucketProps = function(stream, res) {
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

var decode_Pair = function(stream, res) {
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

var decode_Link = function(stream, res) {
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

var decode_Content = function(stream, res) {
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
            res.links.push(decode_Link(stream));
        } else if (fieldnum == 7 && type == 0) {
            res.last_mod = readVarInt(stream);
        } else if (fieldnum == 8 && type == 0) {
            res.last_mod_usecs = readVarInt(stream);
        } else if (fieldnum == 9 && type == 2) {
            res.usermeta.push(decode_Pair(stream));
        } else if (fieldnum == 10 && type == 2) {
            res.indexes.push(decode_Pair(stream));
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
        buf[idx++] = (value & 0x7F) | 0x80;
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
            idx = addVarInt(buf, idx+1, p.length);
            buf.write(p, idx, p.length, 'utf8');
            idx += p.length;
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
    var b0 = new Buffer(8192);
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

var encode_MapReduceReq = function(request, content_type) {
    return makeMessage(23, [request, content_type]);
}


var Client = function(host, port, cb) {
    this.state = 'disconnected';
    this.connect(host, port, cb);
}

Client.prototype.connect = function(host, port, cb) {
    this.host = host || "localhost";
    this.port = port || 8087;
    this.queue = [];
    this.state = 'connecting';
    this.res = undefined;
    var self = this;
    this.client = net.connect(this.port, this.host, function() {
        console.log('client connected at host', self.host, 'port', self.port);
        self.client.setKeepAlive(true);
        self.client.setNoDelay(true);
        self.client.on('data', function(data) { self.onData(data);});
        self.client.on('end', function(data) { self.onEnd();});
        self.state = 'connected';
        self.nextRequest();
    });
}

Client.prototype.end = function() {
    if (this.state != 'disconnected') {
        this.client.end();
        //this.state = 'disconnected';
    }
}

Client.prototype.send = function(request, cb) {
    this.queue.push({request:request, cb:cb});
    if (this.res === undefined) {
        this.nextRequest();
    }
}

Client.prototype.nextRequest = function() {
    if (this.queue.length > 0 && this.state == 'connected') {
        this.client.write(this.queue[0].request);
    }
}

Client.prototype.onEnd = function(cb) {
    this.state = 'disconnected';
    console.log('client disconnected');
}

Client.prototype.onData = function(data) {
    //console.log(data.toString('hex'));
    var stream = {buf:data, idx:0};
    while (stream.idx < data.length) {
        var len = data.readUInt32BE(stream.idx);
        var msg = data.readUInt8(stream.idx+4);
        stream.idx += 5;
        stream.len = stream.idx + len-1;
        //console.log("RECEIVED: message " + msg + ", length " + len);
        var err = null;
        if (msg == 0) {
            this.res = decode_ErrorResp(stream, this.res);
            err = this.res;
            this.res = {};
        } else if (msg == 2) {
            this.res = {}; // nothing to decode for RpbPingResp 
        } else if (msg == 4) {
            this.res = decode_GetClientIdResp(stream, this.res);
        } else if (msg == 6) {
            this.res = {}; // nothing to decode for RpbSetClientIdResp 
        } else if (msg == 8) {
            this.res = decode_GetServerInfoResp(stream, this.res);
        } else if (msg == 10) {
            this.res = decode_GetResp(stream, this.res);
        } else if (msg == 12) {
            this.res = decode_PutResp(stream, this.res);
        } else if (msg == 14) {
            this.res = {}; // nothing to decode for RpbDelResp
        } else if (msg == 16) {
            this.res = decode_ListBucketsResp(stream, this.res);
        } else if (msg == 18) {
            this.res = decode_ListKeysResp(stream, this.res);
        } else if (msg == 20) {
            this.res = decode_GetBucketResp(stream, this.res);
        } else if (msg == 22) {
            this.res = {}; // nothing to decode for RpbSetBucketResp
        } else if (msg == 24) {
            this.res = decode_MapReduceResp(stream, this.res);
        } else {
            stream.idx += len;
            this.res = {};
        }
        if (this.res && (this.res.done === undefined || this.res.done === true)) {
            if (this.queue[0].cb) this.queue[0].cb(err, this.res);
            this.queue.shift();
            //console.log(err? err.errmsg : this.res);
            this.res = undefined;
            this.nextRequest();
        }
    }
}

// Proxies to request builders
Client.prototype.Ping = function(cb) {
    this.send(encode_PingReq(), cb);
}

Client.prototype.GetClientId = function(cb) {
    this.send(encode_GetClientIdReq(), cb);
}

Client.prototype.SetClientId = function(client_id, cb) {
    this.send(encode_SetClientIdReq(client_id), cb);
}

Client.prototype.GetServerInfo = function(cb) {
    this.send(encode_GetServerInfoReq(), cb);
}

Client.prototype.Get = function(bucket, key, options, cb) {
    this.send(encode_GetReq(bucket, key, options), cb);
}

Client.prototype.Put = function(bucket, key, data, options, cb) {
    this.send(encode_PutReq(bucket, key, data, options), cb);
}

Client.prototype.PutNokey = function(bucket, data, options, cb) {
    this.send(encode_PutNokeyReq(bucket, data, options), cb);
}

Client.prototype.Delete = function(bucket, key, options, cb) {
    this.send(encode_DeleteReq(bucket, key, options), cb);
}

Client.prototype.ListBuckets = function(cb) {
    this.send(encode_ListBucketsReq(), cb);
}

Client.prototype.ListKeys = function(bucket, cb) {
    this.send(encode_ListKeysReq(bucket), cb);
}

Client.prototype.GetBucket = function(bucket, cb) {
    this.send(encode_GetBucketReq(bucket), cb);
}

Client.prototype.SetBucket = function(bucket, props, cb) {
    this.send(encode_SetBucketReq(bucket, props), cb);
}

Client.prototype.MapReduce = function(request, content_type, cb) {
    this.send(encode_MapReduceReq(request, content_type), cb);
}

exports.Client = Client;
