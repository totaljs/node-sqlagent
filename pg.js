var pg = require('pg');
require('./index');

function Agent(options) {
    this.options = options;
    this.command = [];
    this.db = null;
    this.done = null;
    this.autoclose = true;
}

Agent.prototype.push = function(name, query, params, prepare) {
    var self = this;
    self.command.push({ name: name, query: query, params: params, prepare: prepare });
    return self;
};

Agent.prototype.remove = function(name) {

    var self = this;

    for (var i = 0, length = self.command.length; i < length; i++) {

        var item = self.command[i];
        if (item.name !== name)
            continue;

        self.command.splice(i, 1);
        return true;

    }

    return false;
};

Agent.prototype.close = function() {
    var self = this;
    self.done();
    self.db = null;
    return self;
};

Agent.prototype.prepare = function(callback) {

    var results = {};
    var errors = {};
    var isError = false;
    var self = this;

    self.command.wait(function(item, next) {

        if (item.prepare) {
            if (item.prepare(item, results, isError ? errors : null) === false) {
                next();
                return;
            }
        }

        self.db.query({ text: item.query }, item.params, function(err, result) {

            if (err) {
                errors[item.name] = err;
                isError = true;
            } else
                results[item.name] = result.command === 'INSERT' ? result.rows[0] : result.rows;

            next();

        });

    }, function() {

        if (self.autoclose) {
            self.done();
            self.db = null;
        }

        if (!isError) {

            if (callback)
                callback(null, results);

            return;
        }

        var errs = {};

        for (var i = 0, length = results.length; i < length; i++) {
            if (results[i] instanceof Error)
                errs[i] = results[i];
        }

        if (callback)
            callback(isError ? errors : null, results);

    });

    return self;
};

Agent.prototype.exec = function(callback, autoclose) {

    var self = this;

    if (autoclose !== undefined)
        self.autoclose = autoclose;

    if (self.command.length === 0) {
        if (callback)
            callback(null, {});
        return self;
    }

    pg.connect(self.options, function(err, client, done) {

        if (err) {
            callback(err, null);
            return;
        }

        self.done = done;
        self.db = client;
        self.prepare(callback);

    });

    return self;
};

Agent.prototype.insert = function(name, table, obj, without, prepare) {

    var self = this;
    var conn = self.db;
    var keys = Object.keys(obj);
    var length = keys.length;
    var query = 'INSERT INTO ' + table + ' (';
    var values = '';
    var params = [];

    if (typeof(without) === 'function') {
        var tmp = without;
        prepare = without;
        without = tmp;
    }

    if (without === true && prepare === undefined)
        prepare = true;

    if (!(without instanceof Array) || without.length === 0)
        without = null;

    var is = false;

    for (var i = 0; i < length; i++) {

        var column = keys[i];

        if (without !== null && without.indexOf(column) !== -1)
            continue;

        if (!is) {
            is = true;
            query += column;
        } else
            query += ',' + column;

        var value = obj[column];
        var type = typeof(value);

        if (values !== '')
            values += ',';

        if (value === null || value === undefined) {
            values += 'null';
            continue;
        }

        values += '$' + (params.length + 1);

        switch (type) {
            case 'string':
            case 'number':
            case 'boolean':
                params.push(value);
                break;
            case 'object':
                if (utils.isDate(value))
                    params.push(value);
                else if (utils.isArray(value))
                    params.push(value.join(','));
                else
                    params.push(JSON.stringify(value));
                break;
        }
    }
    self.command.push({ name: name, query: query + ') VALUES(' + values + ') RETURNING ' + (obj['PRIMARYKEY'] ? obj['PRIMARYKEY'] : 'Id'), params: params, prepare: prepare });
    return self;
};

Agent.prototype.update = function(name, table, obj, condition, without, prepare) {

    var self = this;
    var conn = self.db;
    var keys = Object.keys(obj);
    var length = keys.length;
    var query = 'UPDATE ' + table + ' SET ';
    var values = '';
    var params = [];

    if (typeof(without) === 'function') {
        var tmp = without;
        prepare = without;
        without = tmp;
    }

    if (without === true && prepare === undefined)
        prepare = true;

    if (!(without instanceof Array) || without.length === 0)
        without = null;

    for (var i = 0; i < length; i++) {
        var column = keys[i];

        if (without !== null && without.indexOf(column) !== -1)
            continue;

        var value = obj[column];
        var type = typeof(value);

        if (values !== '')
            values += ',';

        if (value === null || value === undefined) {
            values += column + "=null";
            continue;
        }

        values += column + '=$' + (params.length + 1);

        switch (type) {
            case 'string':
            case 'number':
            case 'boolean':
                params.push(value);
                break;
            case 'object':
                if (utils.isDate(value))
                    params.push(value);
                else if (utils.isArray(value))
                    params.push(value.join(','));
                else
                    params.push(JSON.stringify(value));
                break;
        }
    }

    self.command.push({ name: name, query: query + values + ' WHERE ' + condition, params: params, prepare: prepare });
    return self;
};

Agent.prototype.compare = function(form, data, condition) {

    var formLength = form.length;
    var dataLength = data.length;

    var row_insert = [];
    var row_update = [];
    var row_remove = [];

    for (var i = 0; i < dataLength; i++) {
        for (var j = 0; j < formLength; j++) {
            if (condition(form[j], data[i]))
                row_update.push({ form: form[j], item: data[i] });
            else
                row_remove.push(data[i]);
        }
    }

    for (var j = 0; j < formLength; j++) {
        for (var i = 0; i < dataLength; i++) {
            if (!condition(form[j], data[i]))
                row_insert.push(data[i]);
        }
    }

    return { insert: row_insert, update: row_update, remove: row_remove };
};
module.exports = Agent;