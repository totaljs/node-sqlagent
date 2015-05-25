var database = require('pg');
var Events = require('events');
var queries = {};

require('./index');

function SqlBuilder(skip, take) {
    this.builder = [];
    this._order = null;
    this._skip = skip >= 0 ? skip : 0;
    this._take = take >= 0 ? take : 0;
    this._set = null;
    this.hasOperator = false;
}

SqlBuilder.prototype = {
    get data() {
        return this._set;
    }
};

SqlBuilder.prototype.set = function(name, value) {
    var self = this;
    if (!self._set)
        self._set = {};

    if (typeof(name) === 'string') {
        self._set[name] = value === '$' ? '$' : value;
        return self;
    }

    var keys = Object.keys(name);

    for (var i = 0, length = keys.length; i < length; i++) {
        var key = keys[i];
        var val = name[key];
        self._set[key] = val === '$' ? '$' : val;
    }

    return self;
};

SqlBuilder.prototype.sort = function(name, desc) {
    return this.order(name, desc);
};

SqlBuilder.prototype.order = function(name, desc) {

    var self = this;
    if (self._order === null)
        self._order = [];

    var lowered = name.toLowerCase();

    if (lowered.lastIndexOf('desc') !== -1 || lowered.lastIndexOf('asc') !== -1) {
        self._order.push(name);
        return self;
    } else if (typeof(desc) === 'boolean')
        desc = desc === true ? 'DESC' : 'ASC';
    else
        desc = 'ASC';

    self._order.push(SqlBuilder.column(name) + ' ' + desc);
    return self;
};

SqlBuilder.prototype.skip = function(value) {
    var self = this;
    self._skip = self.parseInt(value);
    return self;
};

SqlBuilder.prototype.limit = function(value) {
    return this.take(value);
};

SqlBuilder.prototype.page = function(value, max) {
    var self = this;
    value = self.parseInt(value) - 1;
    max = self.parseInt(max);
    if (value < 0)
        value = 0;
    self._skip = value * max;
    self._take = max;
    return self;
};

SqlBuilder.prototype.parseInt = function(num) {
    if (typeof(num) === 'number')
        return num;
    if (!num)
        return 0;
    num = parseInt(num);
    if (isNaN(num))
        num = 0;
    return num;
};

SqlBuilder.prototype.take = function(value) {
    var self = this;
    self._take = self.parseInt(value);
    return self;
};

SqlBuilder.prototype.first = function() {
    var self = this;
    self._skip = 0;
    self._take = 1;
    return self;
};

SqlBuilder.prototype.where = function(name, operator, value) {
    return this.push(name, operator, value);
};

SqlBuilder.prototype.push = function(name, operator, value) {
    var self = this;

    if (value === undefined) {
        value = operator;
        operator = '=';
    } else if (operator === '!=')
        operator = '<>';

    // I expect Agent.$$
    if (typeof(value) === 'function')
        value = '$';

    self.checkOperator();
    self.builder.push(SqlBuilder.column(name) + operator + (value === '$' ? '$' : SqlBuilder.escape(value)));
    return self;
};

SqlBuilder.prototype.checkOperator = function() {
    var self = this;
    if (!self.hasOperator)
        self.and();
    self.hasOperator = false;
    return self;
};

SqlBuilder.prototype.clear = function() {
    this._take = 0;
    this._skip = 0;
    this._order = null;
    this._set = null;
    this.builder = [];
    this.hasOperator = false;
    return this;
};

SqlBuilder.escape = function(value) {

    if (value === null || value === undefined)
        return 'null';

    var type = typeof(value);

    if (type === 'function') {
        value = value();

        if (value === null || value === undefined)
            return 'null';

        type = typeof(value);
    }

    if (type === 'boolean')
        return value === true ? 'true' : 'false';

    if (type === 'number')
        return value.toString();

    if (type === 'string')
        return pg_escape(value);

    if (value instanceof Array)
        return pg_escape(value.join(','));

    if (value instanceof Date)
        return pg_escape(dateToString(value));

    return pg_escape(value.toString());
};

SqlBuilder.column = function(name) {
    return name;
};

SqlBuilder.prototype.group = function(names) {
    var self = this;
    self.builder.push('GROUP BY ' + (names instanceof Array ? names.join(',') : names));
    return self;
};

SqlBuilder.prototype.having = function(condition) {
    var self = this;
    self.builder.push('HAVING ' + condition);
    return self;
};

SqlBuilder.prototype.and = function() {
    var self = this;
    if (self.builder.length === 0)
        return self;
    self.hasOperator = true;
    self.builder.push('AND');
    return self;
};

SqlBuilder.prototype.or = function() {
    var self = this;
    if (self.builder.length === 0)
        return self;
    self.builder.push('OR');
    return self;
};

SqlBuilder.prototype.scope = function(fn) {
    var self = this;
    self.checkOperator();
    self.builder.push('(');
    self.hasOperator = true;
    fn.call(self);
    self.builder.push(')');
    return self;
};

SqlBuilder.prototype.in = function(name, value) {
    var self = this;
    if (!(value instanceof Array))
        return self;
    self.checkOperator();
    var values = [];
    for (var i = 0, length = value.length; i < length; i++)
        values.push(SqlBuilder.escape(value[i]));
    self.builder.push(SqlBuilder.column(name) + ' IN (' + values.join(',') + ')');
    return self;
};

SqlBuilder.prototype.like = function(name, value, where) {
    var self = this;
    var search;

    self.checkOperator();

    switch (where) {
        case 'beg':
        case 'begin':
            search = SqlBuilder.escape('%' + value);
            break;
        case '*':
            search = SqlBuilder.escape('%' + value + '%');
            break;
        case 'end':
            search = SqlBuilder.escape(value + '%');
            break;
        default:
            search = SqlBuilder.escape(value);
            break;
    }

    self.builder.push(SqlBuilder.column(name) + ' LIKE ' + search);
    return self;
};

SqlBuilder.prototype.between = function(name, valueA, valueB) {
    var self = this;
    self.checkOperator();
    self.builder.push(SqlBuilder.column(name) + ' BETWEEN ' + valueA + ' AND ' + valueB);
    return self;
};

SqlBuilder.prototype.query = function(sql) {
    return this.sql(sql);
};

SqlBuilder.prototype.sql = function(sql) {
    var self = this;
    self.checkOperator();

    if (arguments.length > 1) {
        var indexer = 1;
        var argv = arguments;
        sql = sql.replace(/\?/g, function() {
            return SqlBuilder.escape(argv[indexer++]);
        });
    }

    self.builder.push(sql);
    return self;
};

SqlBuilder.prototype.toString = function(id) {

    var self = this;
    var plus = '';
    var order = '';

    if (self._order)
        order = ' ORDER BY ' + self._order.join(',');

    if (self._skip > 0 && self._take > 0)
        plus = ' LIMIT ' + self._take + ' OFFSET ' + self._skip;
    else if (self._take > 0)
        plus = ' LIMIT ' + self._take;
    else if (self._skip > 0)
        plus = ' OFFSET ' + self._skip;

    if (self.builder.length === 0)
        return order + plus;

    var where = self.builder.join(' ');

    if (id === undefined || id === null)
        id = 0;

    where = where.replace(/\$(?=\s|$)/g, SqlBuilder.escape(id));
    return ' WHERE ' + where + order + plus;
};

function Agent(options, error) {
    this.options = options;
    this.command = [];
    this.db = null;
    this.done = null;
    this.last = null;
    this.id = null;
    this.$id = null;
    this.isCanceled = false;
    this.index = 0;
    this.isPut = false;
    this.skipCount = 0;
    this.skips = {};
    this.isErrorBuilder = typeof(global.ErrorBuilder) !== 'undefined' ? true : false;
    this.errors = this.isErrorBuilder ? error : null;
    this.time;
    this.$primary = 'id';
}

Agent.prototype = {
    get $() {
        return new SqlBuilder();
    },
    get $$() {
        var self = this;
        return function() {
            return self.$id;
        };
    }
};

Agent.prototype.__proto__ = Object.create(Events.EventEmitter.prototype, {
    constructor: {
        value: Agent,
        enumberable: false
    }
});

// Debug mode (output to console)
Agent.debug = false;

Agent.query = function(name, query) {
    queries[name] = query;
    return Agent;
};

Agent.prototype.skip = function(name) {

    var self = this;

    if (!name) {
        self.skipCount++;
        return self;
    }

    self.skips[name] = true;
    return self;
};

Agent.prototype.primaryKey = Agent.prototype.primary = function(name) {
    var self = this;
    if (!name)
        name = 'id';
    self.command.push({ type: 'primary', name: name });
    return self;
};

Agent.prototype.prepare = function(fn) {
    var self = this;
    self.command.push({ type: 'prepare', fn: fn });
    return self;
};

Agent.prototype.bookmark = function(fn) {
    var self = this;
    self.command.push({ type: 'bookmark', fn: fn });
    return self;
};

Agent.prototype.put = function(value) {
    var self = this;
    self.command.push({ type: 'put', params: value, disable: value === undefined || value === null });
    return self;
};

Agent.prototype.lock = function() {
    return this.put(this.$$);
};

Agent.prototype.unlock = function() {
    this.command.push({ 'type': 'unput' });
    return this;
};

Agent.prototype.query = function(name, query, params) {
    return this.push(name, query, params);
};

Agent.prototype.push = function(name, query, params) {
    var self = this;

    if (typeof(query) !== 'string') {
        params = query;
        query = name;
        name = self.index++;
    }

    var is = false;

    if (!params) {
        is = true;
        params = new SqlBuilder();
    }

    if (queries[query])
        query = queries[query];

    self.command.push({ name: name, query: query, params: params, first: isFIRST(query) });
    return is ? params : self;
};

Agent.prototype.validate = function(fn, error) {
    var self = this;
    var type = typeof(fn);

    if (type === 'string' && error === undefined) {
        // checks the last result
        error = fn;
        fn = undefined;
    }

    if (type === 'function') {
        self.command.push({ type: 'validate', fn: fn, error: error });
        return self;
    }

    var exec = function(err, results, next) {
        var id = fn === undefined || fn === null ? self.last : fn;
        if (id === null || id === undefined)
            return next(false);
        var r = results[id];
        if (r instanceof Array)
            return next(r.length);
        if (r)
            return next(true);
        next(false);
    };

    self.command.push({ type: 'validate', fn: exec, error: error });
    return self;
};

Agent.prototype.cancel = function(fn) {
    return this.validate(fn);
};

Agent.prototype.begin = function() {
    var self = this;
    self.command.push({ type: 'begin' });
    return self;
};

Agent.prototype.end = function() {
    var self = this;
    self.command.push({ type: 'end' });
    return self;
};

Agent.prototype.commit = function() {
    return this.end();
};

function prepareValue(value) {

    if (value === undefined)
        return null;

    var type = typeof(value);

    if (type === 'function')
        value = value();

    if (type === 'string')
        value = value.trim();

    return value;
}

Agent.prototype._insert = function(item) {

    var self = this;
    var name = item.name;
    var values = item.values;
    var table = item.table;

    if (values instanceof SqlBuilder)
        values = values._set;

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

        if (key[0] === '$')
            continue;

        columns.push(key);

        if (value instanceof Array) {

            var helper = [];

            for (var j = 0, sublength = value.length; j < sublength; j++) {
                helper.push('$' + index++);
                params.push(prepareValue(value[j]));
            }

            columns_values.push('(' + helper.join(',') + ')');

        } else {
            columns_values.push('$' + index++);
            params.push(prepareValue(value));
        }
    }

    return { type: item.type, name: name, query: 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES(' + columns_values.join(',') + ') RETURNING ' + self.$primary + ' as identity', params: params, first: true };
};

Agent.prototype._update = function(item) {

    var name = item.name;
    var values = item.values;

    if (values instanceof SqlBuilder)
        values = values._set;

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

        if (key[0] === '$')
            continue;

        if (value instanceof Array) {

            var helper = [];

            for (var j = 0, sublength = value.length; j < sublength; j++) {
                helper.push('$' + (index++));
                params.push(prepareValue(value[j]));
            }

            columns.push(key + '=(' + helper.join(',') + ')');

        } else {
            columns.push(key + '=$' + (index++));
            params.push(prepareValue(value));
        }
    }

    return { type: item.type, name: name, query: 'UPDATE ' + table + ' SET ' + columns.join(',') + condition.toString(this.id), params: params, first: true };
};

Agent.prototype._select = function(item) {
    return { name: item.name, query: item.query + item.condition.toString(this.id), params: null, first: item.condition._take === 1 };
};

Agent.prototype._delete = function(item) {
    return { name: item.name, query: item.query + item.condition.toString(this.id), params: null, first: true };
};

Agent.prototype.insert = function(name, table, values, without) {

    var self = this;

    if (typeof(table) !== 'string') {
        without = values;
        values = table;
        table = name;
        name = self.index++;
    }

    var is = false;
    if (!values) {
        is = true;
        values = new SqlBuilder();
    }

    self.command.push({ type: 'insert', table: table, name: name, values: values, without: without });
    return is ? values : self;
};

Agent.prototype.select = function(name, table, schema, without, skip, take) {

    var self = this;
    if (typeof(table) !== 'string') {
        take = skip;
        skip = without;
        without = schema;
        schema = table;
        table = name;
        name = self.index++;
    }

    if (!schema)
        schema = '*';

    var condition = new SqlBuilder(skip, take);
    var columns;

    if (schema instanceof Array) {
        columns = schema;
    } else if (typeof(schema) === 'string') {
        columns = [schema];
    } else {
        var arr = Object.keys(schema);
        for (var i = 0, length = arr.length; i < length; i++) {
            if (without && without.indexOf(arr[i]) !== -1)
                continue;
            if (arr[i][0] === '$')
                continue;
            columns.push(SqlBuilder.column(arr[i]));
        }
    }

    self.command.push({ type: 'select', query: 'SELECT ' + columns.join(',') + ' FROM ' + table, name: name, without: without, condition: condition });
    return condition;
};

Agent.prototype.builder = function(name) {
    var self = this;
    for (var i = 0, length = self.command.length; i < length; i++) {
        var command = self.command[i];
        if (command.name === name)
            return command.condition;
    }
};

Agent.prototype.count = function(name, table, column) {
    var self = this;

    if (typeof(table) !== 'string') {
        table = name;
        name = self.index++;
    }

    if (!column)
        column = '*';

    var condition = new SqlBuilder();
    self.command.push({ type: 'query', query: 'SELECT COUNT(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn' });
    return condition;
};

Agent.prototype.max = function(name, table, column) {
    var self = this;
    if (typeof(table) !== 'string') {
        table = name;
        name = self.index++;
    }

    var condition = new SqlBuilder();
    self.command.push({ type: 'query', query: 'SELECT MAX(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn' });
    return condition;
};

Agent.prototype.min = function(name, table, column) {
    var self = this;
    if (typeof(table) !== 'string') {
        table = name;
        name = self.index++;
    }

    var condition = new SqlBuilder();
    self.command.push({ type: 'query', query: 'SELECT MAX(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn' });
    return condition;
};

Agent.prototype.avg = function(name, table, column) {
    var self = this;
    if (typeof(table) !== 'string') {
        table = name;
        name = self.index++;
    }

    var condition = new SqlBuilder();
    self.command.push({ type: 'query', query: 'SELECT AVG(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn' });
    return condition;
};

Agent.prototype.updateOnly = function(name, table, values, only) {

    var model = {};

    if (values instanceof SqlBuilder)
        values = values._set;

    for (var i = 0, length = only.length; i < length; i++) {
        var key = only[i];
        model[key] = values[i] === undefined ? null : values[i];
    }

    return this.update(name, table, model, null);
};

Agent.prototype.update = function(name, table, values, without) {

    var self = this;

    if (typeof(table) !== 'string') {
        without = values;
        values = table;
        table = name;
        name = self.index++;
    }

    var condition;

    if (values instanceof SqlBuilder)
        condition = values;
    else
        condition = new SqlBuilder();

    if (!values)
        values = condition;

    self.command.push({ type: 'update', table: table, name: name, values: values, without: without, condition: condition });
    return condition;
};

Agent.prototype.delete = function(name, table) {

    var self = this;

    if (typeof(table) !== 'string') {
        table = name;
        name = self.index++;
    }

    var condition = new SqlBuilder();
    self.command.push({ type: 'delete', query: 'DELETE FROM ' + table, name: name, condition: condition });
    return condition;
};

Agent.prototype.remove = function(name, table) {
    return this.delete(name, table);
};

Agent.prototype.destroy = function(name) {
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
    if (self.done)
        self.done();
    self.done = null;
    return self;
};

Agent.prototype.rollback = function(where, e, next) {
    var self = this;
    self.command.length = 0;
    if (!self.isTransaction)
        return next();
    self.isRollback = true;
    self.end();
    next();
};

Agent.prototype._prepare = function(callback) {

    var results = {};
    var self = this;

    self.isRollback = false;
    self.isTransaction = false;

    if (!self.errors)
        self.errors = self.isErrorBuilder ? new global.ErrorBuilder() : [];

    self.command.sqlagent(function(item, next) {

        if (item.type === 'validate') {
            try {
                item.fn(self.errors, results, function(output) {
                    if (output === true || output === undefined)
                        return next();
                    // reason
                    if (typeof(output) === 'string')
                        self.errors.push(output);
                    else if (item.error)
                        self.errors.push(item.error);

                    // we have error
                    if (self.isTransaction) {
                        self.command.length = 0;
                        self.isRollback = true;
                        self.end();
                        next();
                    } else
                        next(false);
                });
            } catch (e) {
                self.rollback('validate', e, next);
            }
            return;
        }

        if (item.type === 'bookmark') {
            try {
                item.fn(self.errors, results);
                return next();
            } catch (e) {
                self.rollback('bookmark', e, next);
            }
        }

        if (item.type === 'primary') {
            self.$primary = item.name;
            next();
            return;
        }

        if (item.type === 'prepare') {
            try {
                item.fn(self.errors, results, function() {
                    next();
                });
            } catch (e) {
                self.rollback('prepare', e, next);
            }
            return;
        }

        if (item.type === 'unput') {
            self.isPut = false;
            next();
            return;
        }

        if (item.type === 'put') {
            if (item.disable)
                self.$id = null;
            else
                self.$id = typeof(item.params) === 'function' ? item.params() : item.params;
            self.isPut = !self.disable;
            next();
            return;
        }

        if (self.skipCount > 0) {
            self.skipCount--;
            next();
            return;
        }

        if (typeof(item.name) === 'string') {
            if (self.skips[item.name] === true) {
                next();
                return;
            }
        }

        var current;

        switch (item.type) {
            case 'update':
                current = self._update(item);
                break;
            case 'insert':
                current = self._insert(item);
                break;
            case 'select':
                current = self._select(item);
                break;
            case 'delete':
                current = self._delete(item);
                break;
            default:
                current = item;
                break;
        }

        if (current.params instanceof SqlBuilder) {
            current.query = current.query + current.params.toString(self.id);
            current.params = undefined;
        } else
            current.params = prepare_params(current.params);

        var query = function(err, result) {
            if (err) {
                self.errors.push(err.message);
                if (self.isTransaction)
                    self.isRollback = true;
            } else {
                var rows = result.rows;

                if (current.type === 'insert') {
                    self.id = rows.length > 0 ? rows[0].identity : null;
                    if (self.isPut === false)
                        self.$id = self.id;
                }

                if (current.first && current.column) {
                    if (rows.length > 0)
                        results[current.name] = rows[0][current.column];
                } else if (current.first)
                    results[current.name] = rows instanceof Array ? rows[0] : rows;
                else
                    results[current.name] = rows;
                self.emit('data', current.name, results);
            }
            self.last = item.name;
            next();
        };

        if (item.type !== 'begin' && item.type !== 'end') {
            if (!current.first)
                current.first = isFIRST(current.query);

            if (Agent.debug)
                console.log(self.debugname, current.name, current.query);

            self.emit('query', current.name, current.query, current.params);
            self.db.query({ text: current.query }, current.params, query);
            return;
        }

        if (item.type === 'begin') {

            if (Agent.debug)
                console.log(self.debugname, 'begin transaction');

            self.db.query('BEGIN', function(err) {
                if (err) {
                    self.errors.push(err.message);
                    self.command.length = 0;
                    next(false);
                    return;
                }
                self.isTransaction = true;
                self.isRollback = false;
                next();
            });
            return;
        }

        if (item.type === 'end') {
            self.isTransaction = false;
            if (self.isRollback) {

                if (Agent.debug)
                    console.log(self.debugname, 'rollback transaction');

                self.db.query('ROLLBACK', function(err) {
                    if (!err)
                        return next();
                    self.command.length = 0;
                    self.push(err.message);
                    next(false);
                });
                return;
            }

            if (Agent.debug)
                console.log(self.debugname, 'commit transaction');

            self.db.query('COMMIT', function(err) {
                if (!err)
                    return next();
                self.errors.push(err.message);
                self.command.length = 0;
                self.db.query('ROLLBACK', function(err) {
                    if (!err)
                        return next();
                    self.errors.push(err.message);
                    next();
                });
            });
            return;
        }

    }, function() {
        self.time = Date.now() - self.debugtime;
        self.index = 0;
        if (self.done)
            self.done();
        self.done = null;
        var err = null;

        if (self.isErrorBuilder) {
            if (self.errors.hasError())
                err = self.errors;
        } else if (self.errors.length > 0)
            err = self.errors;

        if (Agent.debug)
            console.log(self.debugname, '----- done (' + self.time + ' ms)');

        self.emit('end', err, results, self.time);

        if (callback)
            callback(err, self.returnIndex !== undefined ? results[self.returnIndex] : results);
    });

    return self;
};

Agent.prototype.exec = function(callback, returnIndex) {

    var self = this;

    if (Agent.debug) {
        self.debugname = 'sqlagent/pg (' + Math.floor(Math.random() * 1000) + ')';
        self.debugtime = Date.now();
    }

    if (returnIndex !== undefined && typeof(returnIndex) !== 'boolean')
        self.returnIndex = returnIndex;
    else
        delete self.returnIndex;

    if (self.command.length === 0) {
        if (callback)
            callback.call(self, null, {});
        return self;
    }

    if (Agent.debug)
        console.log(self.debugname, '----- exec');

    database.connect(self.options, function(err, client, done) {

        if (err) {
            callback.call(self, err, null);
            return;
        }

        self.done = done;
        self.db = client;
        self._prepare(callback);
    });

    return self;
};

Agent.prototype.$$exec = function(returnIndex) {
    var self = this;
    return function(callback) {
        return self.exec(callback, returnIndex);
    };
};

Agent.destroy = function() {
    throw new Error('Not supported.');
};

Agent.prototype.writeStream = function(filestream, buffersize, callback) {
    var self = this;

    if (typeof(buffersize) === 'function') {
        callback = buffersize
        buffersize = callback;
    }

    database.connect(self.options, function(err, client, done) {

        if (err) {
            callback(err);
            return;
        }

        var LargeObjectManager = require('pg-large-object').LargeObjectManager;
        var man = new LargeObjectManager(client);
        client.query('BEGIN', function(err, result) {

            if (err) {
                done();
                callback(err);
                return;
            }

            man.createAndWritableStream(buffersize || 16384, function(err, oid, stream) {

                if (err) {
                    done();
                    callback(err);
                    return;
                }

                stream.on('finish', function() {
                    client.query('COMMIT');
                    done();
                    callback(null, oid);
                });

                filestream.pipe(stream);
            });
        });
    });
};

Agent.prototype.readStream = function(oid, buffersize, callback) {
    var self = this;

    if (typeof(buffersize) === 'function') {
        callback = buffersize
        buffersize = callback;
    }

    database.connect(self.options, function(err, client, done) {

        if (err) {
            callback(err);
            return;
        }

        var LargeObjectManager = require('pg-large-object').LargeObjectManager;
        var man = new LargeObjectManager(client);
        client.query('BEGIN', function(err, result) {

            if (err) {
                done();
                callback(err);
                return;
            }

            man.openAndReadableStream(oid, buffersize || 16384, function(err, size, stream) {

                if (err) {
                    done();
                    callback(err);
                    return;
                }

                stream.on('end', function() {
                    client.query('COMMIT');
                    done();
                });

                callback(null, stream, size);
            });
        });
    });
};

// Author: https://github.com/segmentio/pg-escape
// License: MIT
function pg_escape(val){
    if (val === null)
        return 'NULL';
    var backslash = ~val.indexOf('\\');
    var prefix = backslash ? 'E' : '';
    val = val.replace(/'/g, "''").replace(/\\/g, '\\\\');
    return prefix + "'" + val + "'";
};

function dateToString(dt) {
    var arr = [];
    arr.push(dt.getFullYear().toString());
    arr.push((dt.getMonth() + 1).toString());
    arr.push(dt.getDate().toString());
    arr.push(dt.getHours().toString());
    arr.push(dt.getMinutes().toString());
    arr.push(dt.getSeconds().toString());

    for (var i = 1, length = arr.length; i < length; i++) {
        if (arr[i].length === 1)
            arr[i] = '0' + arr[i];
    }

    return arr[0] + '-' + arr[1] + '-' + arr[2] + ' ' + arr[3] + ':' + arr[4] + ':' + arr[5];
}

function prepare_params(params) {
    if (!params)
        return params;
    for (var i = 0, length = params.length; i < length; i++) {
        var param = params[i];
        if (typeof(param) === 'function')
            params[i] = param(params);
    }
    return params;
}

function isFIRST(query) {
    if (!query)
        return false;
    return query.substring(query.length - 7).toLowerCase() === 'limit 1';
}

Agent.init = function(conn, debug) {
    Agent.debug = debug ? true : false;
    framework.database = function(errorBuilder) {
        return new Agent(conn, errorBuilder);
    };
};

module.exports = Agent;