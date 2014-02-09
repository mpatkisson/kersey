/*global describe,afterEach,it */

var assert = require('assert'),
    MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server,
    importer = require('../lib/textfile-importer'),
    config = require('./config');

describe('textfile-importer', function () {
    'use strict';

    function parse(options, callback) {
        var record = null,
            fields = [];
        if (options.line) {
            fields = options.line.split('|');
            record = {
                expediaHotelId: parseInt(fields[0], 10),
                caption: fields[1],
                url: fields[2],
                width: parseInt(fields[3], 10),
                height: parseInt(fields[4], 10),
                thumbnailUrl: fields[6],
                isDefault: fields[7] === '1' ? true : false
            };
        }
        callback(null, record);
    }

    var tested = importer.create({
        host: config.host,
        port: config.port,
        database: config.database,
        collection: 'textfile-importer-test',
        filename: './data/HotelImageList.sample',
        threshold: 5000,
        parse: parse
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

    describe('#start()', function () {
        it('should import all records', function (done) {
            tested.start(function (err, total) {
                if (err) {
                    throw err;
                }
                assert.ok(tested.saved = total);
                done();
            });
        });
    });

});