var database = require('mysql');
var Url = require('url');
var Events = require('events');

require('./index');

function Agent(options) {

    if (typeof(options) === 'string') {
        var opt = Url.parse(options);
        var auth = opt.auth.split(':');
        options = {};
        options.host = opt.hostname;
        options.user = auth[0] || '';
        options.password = auth[1] || '';
        options.database = (opt.pathname || '').substring(1) || '';
    }

    this.options = options;
    this.command = [];
    this.db = null;
    this.done = null;
    this.autoclose = true;
}

Agent.prototype.__proto__ = new Events.EventEmitter();

Agent.prototype.query = function(name, query, params, before, after) {
    var self = this;
    return self.push(name, query, params, before, after);
};

Agent.prototype.push = function(name, query, params, before, after) {
    var self = this;

    if (typeof(query) !== 'string') {
        after = before;
        before = params;
        params = query;
        query = name;
        name = self.command.length;
    }

    self.command.push({ name: name, query: query, params: params, before: before, after: after, first: query.substring(query.length - 7).toLowerCase() === 'limit 1' });
    return self;
};

Agent.prototype._insert = function(item) {

    var self = this;
    var name = item.name;
    var values = item.values;
    var table = item.table;
    var keys = Object.keys(values);

    var columns = [];
    var columns_values = [];
    var params = [];
    var index = 1;

    for (var i = 0, length = keys.length; i < length; i++) {
        var key = keys[i];
        var value = values[key];

        if (item.without && item.without.indexOf(key) !== -1)
            continue;

        columns.push(key);
        columns_values.push('?');
        params.push(value === undefined ? null : value);
    }

    return { name: name, query: 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES(' + columns_values.join(',') + ')', params: params, first: true };
};

Agent.prototype._update = function(item) {

    var self = this;
    var name = item.name;
    var values = item.values;
    var condition = item.condition;
    var table = item.table;
    var keys = Object.keys(values);

    var columns = [];
    var params = [];
    var index = 1;

    for (var i = 0, length = keys.length; i < length; i++) {
        var key = keys[i];
        var value = values[key];

        if (item.without && item.without.indexOf(key) !== -1)
            continue;

        columns.push(key + '=?');
        params.push(value === undefined ? null : value);
    }

    return { name: name, query: 'UPDATE ' + table + ' SET ' + columns.join(',') + ' WHERE ' + condition, params: params, first: true };

};

Agent.prototype.insert = function(name, table, values, without, before, after) {

    var self = this;

    if (typeof(table) !== 'string') {
        after = before;
        before = without;
        without = values;
        values = table;
        table = name;
        name = self.command.length;
    }

    self.command.push({ type: 'insert', table: table, name: name, values: values, without: without, before: before, after: after });
    return self;
};

Agent.prototype.update = function(name, table, values, condition, without, before, after) {

    var self = this;

    if (typeof(table) !== 'string') {
        after = before;
        before = without;
        without = condition;
        condition = values;
        values = table;
        table = name;
        name = self.command.length;
    }

    self.command.push({ type: 'update', table: table, name: name, values: values, without: without, before: before, after: after, condition: condition });
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

    self.command.sqlagent(function(item, next) {

        if (item.before && item.before(item.type ? item.values : item, results, isError ? errors : null) === false) {
            next();
            return;
        }

        var current = item.type === 'update' ? self._update(item) : item.type === 'insert' ? self._insert(item) : item;

        self.db.query(current.query, current.params, function(err, rows) {

            if (err) {
                errors[current.name] = err;
                isError = true;
            } else {
                results[current.name] = current.first ? rows[0] : rows;
                self.emit('data', current.name, results);
            }

            if (item.after)
                item.after(current.type ? current.values : current, results, isError ? errors : null);

            next();

        });

    }, function() {

        if (self.autoclose) {
            self.done();
            self.db = null;
        }

        if (!isError) {

            self.emit('end', null, results);

            if (callback)
                callback(null, results);

            return;
        }

        var errs = {};

        for (var i = 0, length = results.length; i < length; i++) {
            if (results[i] instanceof Error)
                errs[i] = results[i];
        }

        self.emit('end', isError ? errors : null, results);

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

    var connection = database.createConnection(self.options);

    connection.connect(function(err) {

        if (err) {
            callback(err, null);
            return;
        }

        self.done = function() {
            connection.end();
        };

        self.db = connection;
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