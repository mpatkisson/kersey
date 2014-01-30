
function start(options, callback) {
    'use strict';
    options = options || {};
    options.completed = callback;
    var type = options.type || 'textfile-importer',
        importer = require('./' + type).create(options);
    importer.start();
}

module.exports.start = start;
