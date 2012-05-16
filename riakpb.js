// ---------------------------
// Riakpb module
// Client for a Riak database using Protocol Buffers. This is a binary protocol
// with persistent connection, so it should be faster than independent HTTP requests.
//
// Copyright by Javier Arevalo in 2012.
// - http://www.iguanademos.com/Jare/
// - @TheJare on twitter
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
// ---------------------------

var net = require('net');

// -----------------------------
// Reading protocol buffers
// ----------------------

// Only varints in the Riak protocol buffers are bools, uint8s and uint32s.
// So we just use readVarInt to read all of them, no need to decode floats or signed ints.
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
// Riak API: http://wiki.basho.com/Client-Implementation-Guide.html
// ----------------------

// message RpbErrorResp {
//     required bytes errmsg = 1;
//     required uint32 errcode = 2;
// }
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

// message RpbGetClientIdResp {
//     required bytes client_id = 1; // Client id in use for this connection
// }
var decode_GetClientIdResp = function(stream, res) {
    res = res || {client_id:""};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.client_id = readString(stream);
        }
    });
    return res;
}

// message RpbGetServerInfoResp {
//     optional bytes node = 1;
//     optional bytes server_version = 2;
// }
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

// message RpbGetResp {
//     repeated RpbContent content = 1;
//     optional bytes vclock = 2;
//     optional bool unchanged = 3;
// }
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

// message RpbPutResp {
//     repeated RpbContent contents = 1;
//     optional bytes vclock = 2;        // the opaque vector clock for the object
//     optional bytes key = 3;           // the key generated, if any
// }
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

// message RpbListBucketsResp {
//     repeated bytes buckets = 1;
// }
var decode_ListBucketsResp = function(stream, res) {
    res = res || {buckets:[]};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.buckets.push(readString(stream));
        }
    });
    return res;
}

// message RpbListKeysResp {
//     repeated bytes keys = 1;
//     optional bool done = 2;
// }
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

// message RpbGetBucketResp {
//     required RpbBucketProps props = 1;
// }
// message RpbBucketProps {
//     optional uint32 n_val = 1;
//     optional bool allow_mult = 2;
// }
var decode_GetBucketResp = function(stream, res) {
    res = res || {props : {}};
    decodeLoop(stream, function(type, fieldnum) {
        if (fieldnum == 1 && type == 2) {
            res.props = decode_BucketProps(stream);
        }
    });
    return res;
}

// message RpbMapRedResp {
//     optional uint32 phase = 1;
//     optional bytes response = 2;
//     optional bool done = 3;
// }
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

// message RpbBucketProps {
//     optional uint32 n_val = 1;
//     optional bool allow_mult = 2;
// }
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

// message RpbPair {
//     required bytes key = 1;
//     optional bytes value = 2;
// }
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

// message RpbLink {
//     optional bytes bucket = 1;
//     optional bytes key = 2;
//     optional bytes tag = 3;
// }
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

// message RpbContent {
//     required bytes value = 1;
//     optional bytes content_type = 2;     // the media type/format
//     optional bytes charset = 3;
//     optional bytes content_encoding = 4;
//     optional bytes vtag = 5;
//     repeated RpbLink links = 6;          // links to other resources
//     optional uint32 last_mod = 7;
//     optional uint32 last_mod_usecs = 8;
//     repeated RpbPair usermeta = 9;       // user metadata stored with the object
//     repeated RpbPair indexes = 10;
// }
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
// ----------------------

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

            var tempv, l = 4096;
            // If tentative buffer size is too small, retry with required size, which we will know after addParameters()
            do {
                tempv = new Buffer(l);
                l = addParameters(tempv, 0, p);
            } while (l > tempv.length);
            idx = addVarInt(buf, idx+1, l);
            if (idx < buf.length) // Unlike the [] accessor, copy() will throw if out of bounds.
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
    var b0, idx, l = 8192;
    // If tentative buffer size is too small, retry with required size, which we will know after addParameters()
    do {
        b0 = new Buffer(l);
        idx = 5;
        if (params) idx = addParameters(b0, idx, params);
        l = idx;
    } while (idx > b0.length);
    addMessageHeader(b0, idx - 4, msg);
    return b0.slice(0, idx);
}

// ----------------------
// Encoding Riak requests
// ----------------------

var encode_PingReq = function() {
    return new Buffer([0,0,0,1,1]);
}

var encode_GetClientIdReq = function() {
    return new Buffer([0,0,0,1,3]);
}

// message RpbSetClientIdReq {
//     required bytes client_id = 1; // Client id to use for this connection
// }
var encode_SetClientIdReq = function(client_id) {
    return makeMessage(5, [client_id]);
}

var encode_GetServerInfoReq = function() {
    return new Buffer([0,0,0,1,7]);
}

// message RpbGetReq {
//     required bytes bucket = 1;
//     required bytes key = 2;
//     optional uint32 r = 3;
//     optional uint32 pr = 4;
//     optional bool basic_quorum = 5;
//     optional bool notfound_ok = 6;
//     optional bytes if_modified = 7;
//     optional bool head = 8;
//     optional bool deletedvclock = 9;
// }
var encode_GetReq = function(bucket, key, options) {
    options = options || {};
    return makeMessage(9, [bucket, key, options.r, options.pr, options.basic_quorum, options.notfound_ok, options.if_modified, options.head, options.deletedvclock]);
}

// message RpbPutReq {
//     required bytes bucket = 1;
//     optional bytes key = 2;
//     optional bytes vclock = 3;
//     required RpbContent content = 4;
//     optional uint32 w = 5;
//     optional uint32 dw = 6;
//     optional bool return_body = 7;
//     optional uint32 pw = 8;
//     optional bool if_not_modified = 9;
//     optional bool if_none_match = 10;
//     optional bool return_head = 11;%
// }
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

// message RpbDelReq {
//     required bytes bucket = 1;
//     required bytes key = 2;
//     optional uint32 rw = 3;
//     optional bytes vclock = 4;
//     optional uint32 r = 5;
//     optional uint32 w = 6;
//     optional uint32 pr = 7;
//     optional uint32 pw = 8;
//     optional uint32 dw = 9;
// }
var encode_DeleteReq = function(bucket, key, options) {
    options = options || {};
    return makeMessage(11, [bucket, key, options.rw, options.vclock, options.r, options.w, options.pr, options.pw, options.dw ]);
}

var encode_ListBucketsReq = function() {
    return new Buffer([0,0,0,1,15]);
}

// message RpbListKeysReq {
//     required bytes bucket = 1;
// }
var encode_ListKeysReq = function(bucket) {
    return makeMessage(17, [bucket]);
}

// message RpbGetBucketReq {
//     required bytes bucket = 1;
// }
var encode_GetBucketReq = function(bucket) {
    return makeMessage(19, [bucket]);
}

// message RpbSetBucketReq {
//     required bytes bucket = 1;
//     required RpbBucketProps props = 2;
// }
// message RpbBucketProps {
//     optional uint32 n_val = 1;
//     optional bool allow_mult = 2;
// }
var encode_SetBucketReq = function(bucket, props) {
    return makeMessage(21, [bucket, [props.n_val, props.allow_mult]]);
}

// message RpbMapRedReq {
//     required bytes request = 1;
//     required bytes content_type = 2;
// }
var encode_MapReduceReq = function(request, content_type) {
    return makeMessage(23, [request, content_type]);
}

// ----------------------
// The actual client object.
// ----------------------

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
    this.pendingBuffers = [];
    this.receivedTotalData = 0;
    this.needTotalData = 0;


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
    // Riak pbs only accept one request at a time, so we serialize them
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
    // TODO: handle external disconnects and re-connects
    this.state = 'disconnected';
    console.log('client disconnected');
}

Client.prototype.onData = function(data) {
    //console.log(data.toString('hex'));

    // Accumulate into our buffer array of received packets
    this.pendingBuffers.push(data);
    this.receivedTotalData += data.length;

    // If we have enough data (4 bytes needed) to read the length of next packet, do it.
    if (this.needTotalData == 0 && this.receivedTotalData >= 4) {
        // Consolidate all received data 
        if (this.pendingBuffers.length > 1) {
            this.pendingBuffers = [consolidateBuffers(this.pendingBuffers)];
        }
        this.needTotalData = this.pendingBuffers[0].readUInt32BE(0)+4;
    }

    // If we have at least a full packet, process it and keep the remainder (part of the next packet).
    if (this.needTotalData > 0 && this.needTotalData <= this.receivedTotalData) {
        if (this.pendingBuffers.length > 1) {
            this.pendingBuffers = [consolidateBuffers(this.pendingBuffers)];
        }
        var remainder = this.processPackets(this.pendingBuffers[0]);
        if (remainder) {
            this.pendingBuffers = [remainder];
            this.receivedTotalData = remainder.length;
        } else {
            this.pendingBuffers = [];
            this.receivedTotalData = 0;
        }
        this.needTotalData = 0;
    }

    //console.log(this.pendingBuffers, this.receivedTotalData, this.needTotalData);
}

// Process all the packets available in the buffer
// Return the remainder of the buffer
Client.prototype.processPackets = function(data) {
    var stream = {buf:data, idx:0};
    while (stream.idx+4 < data.length) {
        var len = data.readUInt32BE(stream.idx);
        if (stream.idx + 4 + len > data.length) {
            return data.slice(stream.idx);
        }
        var msg = data.readUInt8(stream.idx+4);
        stream.idx += 5;
        this.processPacket(stream, msg, len);
    }
    if (stream.idx == data.length)
        return null;
    return data.slice(stream.idx);
}

// Process one packet
Client.prototype.processPacket = function(stream, msg, len) {
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
    // If response was only one packet, or we have received all the packets for
    // a multi-packet response, deliver the callback and trigger next request
    if (this.res.done === undefined || this.res.done === true) {
        if (this.queue[0].cb) this.queue[0].cb(err, this.res);
        this.queue.shift();
        //console.log(err? err.errmsg : this.res);
        this.res = undefined;
        this.nextRequest();
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
