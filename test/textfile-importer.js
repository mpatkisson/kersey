
(function () {
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

    var importer = require('../lib/kersey'),
        options = {
            database: 'kersey',
            collection: 'images',
            filename: './data/HotelImageList.sample',
            threshold: 10000,
            parse: parse
        };

    importer.start(options, function (err) {
        if (err) {
            console.log(err);
            return;
        }
        console.log('done');
        process.exit(0);
    });

}());
