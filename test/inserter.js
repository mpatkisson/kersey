/*global describe,beforeEach,it */

var assert = require('assert'),
    MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server,
    inserter = require('../lib/inserter');

describe('inserter', function () {
    'use strict';

    var host = 'localhost',
        port = 27017,
        database = 'kersey',
        collection = 'inserter.test',
        tested = inserter.create({
            host: host,
            port: port,
            database: database,
            collection: collection,
            threshold: 2
        });

    afterEach(function (done) {
        var server = new Server(host, port),
            client = new MongoClient(server);
        client.open(function (err, opened) {
            if (err) {
                throw err;
            }
            var db = opened.db(database);
            db.collection(collection).remove(null, {w: 1}, function (err, removed) {
                if (err) {
                    throw err;
                }
                assert.ok(removed >= 0);
                done();
            });
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

            function queued(err, queueSize) {
                if (err) {
                    throw err;
                }
                assert.equal(queueSize, 1);
                done();
            }

            tested.queue(record, false, queued);
        });

        it('should insert records when the batch threshold is met', function (done) {
            var record = {a: 2};

            function queued(err, queueSize) {
                if (err) {
                    throw err;
                }
                assert.equal(queueSize, 0);
                done();
            }

            tested.queue(record, false, queued);
        });

        it('should insert records when isLast is enabled', function (done) {
            var record = {a: 3};

            function queued(err, queueSize) {
                if (err) {
                    throw err;
                }
                assert.equal(queueSize, 0);
                done();
            }

            tested.queue(record, true, queued);
        });
    });
});