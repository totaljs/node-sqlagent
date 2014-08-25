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

    if (typeof(query) !== 'string') {
        prepare = params;
        params = query;
        query = name;
        name = self.command.length + 1;
    }

    self.command.push({ name: name, query: query, params: params, prepare: prepare, first: query.substring(query.length - 7).toLowerCase() === 'limit 1' });
    return self;
};

Agent.prototype.insert = function(name, table, values, prepare, id) {

    var self = this;

    if (typeof(table) !== 'string') {
        id = prepare;
        prepare = values;
        values = table;
        table = name;
        name = self.command.length + 1;
    }

    if (typeof(prepare) === 'string') {
        var tmp = id;
        id = prepare;
        prepare = tmp;
    }

    var keys = Object.keys(values);

    var columns = [];
    var columns_values = [];
    var params = [];
    var index = 1;

    for (var i = 0, length = keys.length; i < length; i++) {
        var key = keys[i];
        var value = values[key];
        columns.push(key);

        if (value instanceof Array) {

            var helper = [];

            for (var j = 0, sublength = value.length; j < sublength; j++) {
                helper.push('$' + index++);
                params.push(value[j] === undefined ? null : value[j]);
            }

            columns_values.push('(' + helper.join(',') + ')');
        } else {
            columns_values.push('$' + index++);
            params.push(value === undefined ? null : value);
        }
    }

    self.command.push({ name: name, query: 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES(' + columns_values.join(',') + ') RETURNING ' + (id || 'Id'), params: params, prepare: prepare, first: true });

};

Agent.prototype.update = function(name, table, values, condition, prepare) {

    var self = this;

    if (typeof(table) !== 'string') {
        prepare = condition;
        condition = values;
        values = table;
        table = name;
        name = self.command.length + 1;
    }

    if (typeof(prepare) === 'string') {
        var tmp = id;
        id = prepare;
        prepare = tmp;
    }

    var keys = Object.keys(values);

    var columns = [];
    var params = [];
    var index = 1;

    for (var i = 0, length = keys.length; i < length; i++) {
        var key = keys[i];
        var value = values[key];

        if (value instanceof Array) {

            var helper = [];

            for (var j = 0, sublength = value.length; j < sublength; j++) {
                helper.push('$' + index++);
                params.push(value[j] === undefined ? null : value[j]);
            }

            columns.push(key + '=(' + helper.join(',') + ')');
        } else {
            columns.push(key + '=$' + index++);
            params.push(value === undefined ? null : value);
        }
    }

    self.command.push({ name: name, query: 'UPDATE ' + table + ' SET ' + columns.join(',') + ' WHERE ' + condition, params: params, prepare: prepare, first: true });

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
                results[item.name] = result.command === 'INSERT' || item.first ? result.rows[0] : result.rows;

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