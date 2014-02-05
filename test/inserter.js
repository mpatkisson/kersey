/*global describe,it */

var assert = require('assert'),
    inserter = require('../lib/inserter');

describe('inserter', function () {
    'use strict';

    var tested = inserter.create({
        database: 'kersey',
        collection: 'inserter.test',
        threshold: 2
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

            function queued(err, queueSize, insertedSize) {
                if (err) {
                    throw err;
                }
                assert.equal(queueSize, 1);
                assert.equal(insertedSize, 0);
                done();
            }

            tested.queue(record, false, queued);
        });
    });
});