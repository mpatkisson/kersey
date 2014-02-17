/*global describe,afterEach,it */

var assert = require('assert'),
    MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server,
    inserter = require('../lib/inserter'),
    config = require('./config');

describe('inserter', function () {
    'use strict';

    var tested = inserter.create({
        host: config.host,
        port: config.port,
        database: config.database,
        collection: 'inserter-test',
        threshold: 2
    });

    afterEach(function (done) {
        var server = new Server(tested.host, tested.port),
            client = new MongoClient(server);

        function removed(err, count) {
            if (err) {
                throw err;
            }
            assert.ok(count >= 0);
            done();
        }

        client.open(function (err, opened) {
            if (err) {
                throw err;
            }
            var db = opened.db(tested.database);
            db.collection(tested.collection).remove(null, {w: 1}, removed);
        });

    });

    describe('#open()', function () {
        it('should return an open MongoDB client', function (done) {
            tested.open(function opened(client) {
                assert.notEqual(client, null);
                done();
            });
        });
    });

    describe('#queue()', function () {
        it('should not insert records before batch threshold is met', function (done) {
            var record = {a: 1};

            function queued(queueSize) {
                assert.equal(queueSize, 1);
                done();
            }

            tested.queue(record, false, queued);
        });

        it('should insert records when the batch threshold is met', function (done) {
            var record = {a: 2};

            function queued(queueSize) {
                assert.equal(queueSize, 0);
                done();
            }

            tested.queue(record, false, queued);
        });

        it('should insert records when isLast is enabled', function (done) {
            var record = {a: 3};

            function queued(queueSize) {
                assert.equal(queueSize, 0);
                done();
            }

            tested.queue(record, true, queued);
        });
    });
});
