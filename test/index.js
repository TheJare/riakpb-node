var Riak = require('../');
var assert = require('assert');
var Client = Riak.Client;

var client;
beforeEach(function() {
    client = new Client();
});

describe('Requests', function() {
    describe('.ping', function() {
        it('should create a properly formed buffer');
    });
    describe('.queue', function() {
        it('should queue commands 3 for the server', function(done) {
            client.GetServerInfo();
            client.GetClientId();
            client.Ping();
            assert(client.queue.length == 3);
            done();
        });
    });
});

describe('Live server', function() {
    describe('.connect', function() {
        it('should connect to a local Riak server', function(done) {
            client.connect('localhost', 8087, done);
        });
    });
});
