/*jslint nomen: true */

var lineReader = require('line-reader'),
    inserter = require('./inserter');

/**
 * Imports data into MongoDb from a text file.
 * @constructor
 * @param {Object} options Optional values for the importer.
 * @param {String} options.host The hostname of the MongoDB server.
 * @param {Number} options.port The port used by the MongoDB server.
 * @param {String} options.database The database in which the data will be
 *                                  stored.
 * @param {String} options.collection The collection in which the data will be
 *                                   stored.
 * @param {String} options.filename The name of the text file to parse.
 * @param {Number} options.threshold The number of documents to insert as a
 *                                  batch.
 * @param {Function} options.parse A function which accepts a line of text and
 *                                 returns a document to be inserted.
 */
var textFileImporter = function (options) {
    'use strict';

    var _that = inserter.create(options),
        _mongoClient = null,
        _lines = 0,
        _linesInFile = 0,
        _recordsCount = 0;

    _that.filename = options.filename;
    _that.parse = options.parse;

    function processRecord(err, record) {
        if (err) {
            _that.emit('complete', err);
            return;
        }

        _recordsCount += 1;
        var isLastRecord = _recordsCount === _linesInFile;
        _that.queue(record, isLastRecord);
    }

    function processLine(line, isLastLine) {
        _lines = _lines + 1;
        if (isLastLine) {
            _linesInFile = _lines;
            _that.records = _lines;
        }
        var parseOptions = {
            line: line,
            lines: _lines,
            client: _mongoClient
        };
        _that.parse(parseOptions, processRecord);
    }

    function mongoClientOpened(client) {
        _mongoClient = client;
        lineReader.eachLine(_that.filename, processLine);
    }

    _that.start = function (callback) {
        _that.completed = callback;
        _that.open(mongoClientOpened);
        return _that;
    };

    return _that;
};

module.exports.create = textFileImporter;
