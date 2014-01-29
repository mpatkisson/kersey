/*jslint nomen: true */

var importer = require('../lib/textfile-importer');

var imageImporter = function (options) {
    'use strict';

    options.parse = function (options, callback) {
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
    };

    var options = {
        collection: 'images',
        filename: './data/HotelImageList.txt',
        threshold: 10000,
        completed: function (err) {
            if (err) {
                console.log(err);
                return;
            }
            imported('images');
            process.exit(0);
        }
    };

    var _that = importer.create(options);

    _that.import = function () {
        _that.start();
    };

    return _that;
};

module.exports.create = imageImporter;
