(function () {
    'use strict';

    var textFileImporter = require('./textfile-importer');

    function importTextFile(options, callback) {
        options.completed = callback;
        var importer = textFileImporter.create(options);
        importer.start();
    }

    module.exports.importTextFile = importTextFile;
}());
