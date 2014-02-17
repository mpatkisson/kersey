/*jslint nomen: true */

var EventEmitter = require('events').EventEmitter,
    MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server;

/**
 * A general utility for batch MongoDB insertions.
 */
function inserter(options) {
    'use strict';

    var _that = new EventEmitter(),
        _options = options || {},
        _mongoClient = null,
        _mongoDb = null,
        _mongoCollection = null,
        _mongoClientOpened,
        _records = [];

    _that.host = _options.host || 'localhost';
    _that.port = _options.port || 27017;
    _that.database = _options.database;
    _that.collection = _options.collection;
    _that.threshold = _options.threshold || 100;
    _that.records = _options.records || -1;
    _that.saved = 0;

    function mongoClientOpened(err, client) {
        if (err) {
            _that.emit('complete', err);
            return;
        }

        _mongoClient = client;
        _mongoDb = client.db(_that.database);
        _mongoCollection = _mongoDb.collection(_that.collection);
        _mongoClientOpened(_mongoClient);
    }

    _that.queue = function (record, isLast, callback) {
        _records.push(record);
        if (_records.length === _that.threshold || isLast) {
            _mongoCollection.insert(_records, {w: 1}, function (err, result) {
                if (err) {
                    _that.emit('complete', err);
                    return;
                }

                _that.saved += result.length;
                _that.emit('inserted', _that.saved, isLast);
                if (_that.saved === _that.records) {
                    _that.emit('complete', null, _that.saved);
                }
            });
            _records = [];
        }
        if (callback) {
            callback(_records.length);
        }
    };

    _that.open = function (callback) {
        var mongoServer = new Server(_that.host, _that.port),
            mongoClient = new MongoClient(mongoServer);
        _mongoClientOpened = callback;
        mongoClient.open(mongoClientOpened);
    };

    return _that;
}

module.exports.create = inserter;
