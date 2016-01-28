var database = require('mongodb');
var Events = require('events');
var columns_cache = {};

require('./index');

function SqlBuilder(skip, take, agent) {
	this.agent = agent;
	this.builder = {};
	this._order = null;
	this._skip = skip >= 0 ? skip : 0;
	this._take = take >= 0 ? take : 0;
	this._set = null;
	this._inc = null;
	this._fn;
	this._fnpath;
	this._scope = 0;
	this._join;
	this._fields;
	this._schema;
	this._group;
	this._having;
	this._primary;
	this._is = false;
}

SqlBuilder.prototype = {
	get data() {
		var obj = {};
		if (this._set)
			obj.set = this._set;
		if (this._inc)
			obj.inc = this._inc;
		return obj;
	}
};

SqlBuilder.prototype.replace = function(builder) {
	var self = this;

	self.builder = builder.builder.slice(0);

	if (builder._order)
		self._order = copy(builder._order);

	self._skip = builder._skip;
	self._take = builder._take;

	if (builder._set)
		self._set = copy(builder._set);

	if (builder._inc)
		self._inc = copy(builder._inc);

	if (builder._fn)
		self._fn = copy(builder._fn);

	if (builder._fnpath)
		self._fnpath = copy(builder._fnpath);

	if (builder._fields)
		self._fields = copy(builder._fields);

	if (builder._schema)
		self._schema = builder._schema;

	self._is = builder._is;
	return self;
};

function copy(source) {

	var keys = Object.keys(source);
	var i = keys.length;
	var target = {};

	while (i--) {
		var key = keys[i];
		target[key] = source[key];
	}

	return target;
};

SqlBuilder.prototype.clone = function() {
	var builder = new SqlBuilder(0, 0, this.agent);
	return builder.replace(this);
};

SqlBuilder.prototype.join = function(name, on, type) {
	throw new Error('SqlBuilder.join(' + name + ') is not supported.');
};

SqlBuilder.prototype.set = function(name, value) {
	var self = this;
	if (!self._set)
		self._set = {};

	if (typeof(name) === 'string') {
		self._set[name] = value;
		return self;
	}

	var keys = Object.keys(name);

	for (var i = 0, length = keys.length; i < length; i++) {
		var key = keys[i];

		if (key === '_id' || key[0] === '$')
			continue;

		self._set[key] = name[key];
	}

	return self;
};

SqlBuilder.prototype.primary = SqlBuilder.prototype.primaryKey = function(name) {
	console.log('SqlBuilder.primary(' + name + ') is not supported.');
	// not implemented
	return this;
};

SqlBuilder.prototype.remove = SqlBuilder.prototype.rem = function(name) {
	if (this._set)
		delete this._set[name];
	if (this._inc)
		delete this._inc[name];
	return this;
};

SqlBuilder.prototype.schema = function(name) {
	this._schema = name;
	return this;
};

SqlBuilder.prototype.fields = function() {
	var self = this;

	if (arguments[0] instanceof Array) {
		var arr = arguments[0];
		for (var i = 0, length = arr.length; i < length; i++)
			self.field(arr[i]);
		return self;
	}

	for (var i = 0; i < arguments.length; i++)
			self.field(arguments[i]);
	return self;
};

SqlBuilder.prototype.field = function(name, visible) {
	var self = this;
	if (!self._fields)
		self._fields = {};
	self._fields[name] = visible === false ? 0 : 1;
	return self;
};

SqlBuilder.prototype.raw = function(name, value) {
	var self = this;
	if (!self._set)
		self._set = {};
	self._set[name] = value;
	return self;
};

SqlBuilder.prototype.inc = function(name, type, value) {

	var self = this;
	var can = false;

	if (!self._inc)
		self._inc = {};

	if (value === undefined) {
		value = type;
		type = '+';
		can = true;
	}

	if (value === undefined)
		value = 1;

	if (typeof(name) === 'string') {

		if (can && typeof(value) === 'string') {
			type = value[0];
			value = parseInt(value.substring(1));
			if (isNaN(value))
				return self;
		}

		if (!value)
			return self;

		if (type === '-')
			value = value * -1;

		if (value === '$')
			throw new Error('SqlBuilder.inc(' + name + ') can\'t contain "$" value.');

		self._inc[name] = value;
		return self;
	}

	var keys = Object.keys(name);

	for (var i = 0, length = keys.length; i < length; i++) {
		var key = keys[i];
		var val = name[key];

		if (key[0] === '$' || key === '_id')
			continue;

		if (can && typeof(val) === 'string') {
			type = val[0];
			val = parseInt(val.substring(1));
			if (isNaN(val))
				continue;
		}

		if (!val)
			continue;

		key = type + key;

		if (val === '$')
			throw new Error('SqlBuilder.inc(' + key + ') can\'t contain "$" value.');

		self._inc[key] = val;
	}

	return self;
};

SqlBuilder.prototype.sort = function(name, desc) {
	return this.order(name, desc);
};

SqlBuilder.prototype.order = function(name, desc) {

	var self = this;
	if (self._order === null)
		self._order = {};

	var key = '<' + name + '>';

	if (columns_cache[key]) {
		self._order[columns_cache[key].name] = columns_cache[key].value;
		return;
	}

	var lowered = name.toLowerCase();
	var index = lowered.lastIndexOf('desc');

	if (index !== -1 || lowered.lastIndexOf('asc') !== -1) {
		name = name.split(' ')[0];
		desc = indexOf !== -1;
	}

	columns_cache[key] = {};
	columns_cache[key].name = name;
	columns_cache[key].value = self._order[name] = desc ? -1 : 1;
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

	var is = false;

	// I expect Agent.$$
	if (typeof(value) === 'function') {

		if (!self._fn) {
			self._fn = {};
			self._fnpath = [];
		}

		var key = Math.floor(Math.random() * 1000000);
		self._fn[key] = value;
		self._fnpath.push(name);
		value = '#' + key + '#';
		is = true;
	}

	switch (operator) {
		case '=':
			self.$scope(name, value);
			break;
		case '<>':
			self.$scope(name, { $ne: value });
			break;
		case '>':
			self.$scope(name, { $gt: value });
			break;
		case '<':
			self.$scope(name, { $lt: value });
			break;
		case '>=':
			self.$scope(name, { $gte: value });
			break;
		case '<=':
			self.$scope(name, { $lte: value });
			break;
	}

	self.checkOperator();
	self._is = true;
	return self;
};

SqlBuilder.prototype.checkOperator = function() {
	// Not implemented
	return this;
};

SqlBuilder.prototype.clear = function() {
	this._take = 0;
	this._skip = 0;
	this._scope = 0;
	this._order = null;
	this._set = null;
	this._inc = null;
	this._fnpath = null;
	this._fn = null;
	this.builder = {};
	return this;
};

SqlBuilder.escape = SqlBuilder.prototype.escape = function(value) {
	return value;
};

SqlBuilder.column = function(name, schema) {
	return name;
};

SqlBuilder.prototype.group = function(names) {
	var self = this;

	if (names instanceof Array) {
		for (var i = 0, length = names.length; i < length; i++)
			names[i] = SqlBuilder.column(names[i], self._schema);
		self._group = 'GROUP BY ' + names.join(',');
	} else if (names) {
		var arr = new Array(arguments.length);
		for (var i = 0; i < arguments.length; i++)
			arr[i] = SqlBuilder.column(arguments[i.toString()], self._schema);
		self._group = 'GROUP BY ' + arr.join(',');
	} else
		delete self._group;

	return self;
};

SqlBuilder.prototype.having = function(condition) {
	var self = this;

	if (condition)
		self._having = 'HAVING ' + condition;
	else
		delete self._having;

	return self;
};

SqlBuilder.prototype.and = function() {
	var self = this;
	self._scope = 2;
	return self;
};

SqlBuilder.prototype.or = function() {
	var self = this;
	self._scope = 1;
	return self;
};

SqlBuilder.prototype.end = function() {
	var self = this;
	self._scope = 0;
	return self;
};

SqlBuilder.prototype.scope = function(fn) {
	var self = this;
	fn.call(self);
	self._scope = 0;
	return self;
};

SqlBuilder.prototype.$scope = function(name, obj) {
	var self = this;

	if (self._scope === 0) {
		self.builder[name] = obj;
		return self;
	}

	if (self._scope === 1) {
		if (!self.builder['$or'])
			self.builder['$or'] = [];
		var filter = {};
		filter[name] = obj;
		self.builder['$or'].push(filter);
	}

	if (self._scope === 2) {
		if (!self.builder['$and'])
			self.builder['$and'] = [];
		var filter = {};
		filter[name] = obj;
		self.builder['$and'].push(filter);
	}

	return self;
};

SqlBuilder.prototype.in = function(name, value) {
	var self = this;

	if (!(value instanceof Array)) {
		self.where(name, value);
		return self;
	}

	self.$scope(name, { '$in': value });
	return self;
};

SqlBuilder.prototype.like = function(name, value, where) {
	var self = this;

	switch (where) {
		case 'beg':
		case 'begin':
			self.$scope(name, { $regex: '^' + value.toString() });
			break;
		case 'end':
			self.$scope(name, { $regex: value.toString() + '$' });
			break;
		case '*':
		default:
			self.$scope(name, { $regex: value.toString() });
			break;
	}

	self._is = true;
	return self;
};

SqlBuilder.prototype.between = function(name, valueA, valueB) {
	var self = this;
	var obj = {};
	self.$scope(name, { $gte: valueA, $lt: valueB });
	self._is = true;
	return self;
};

SqlBuilder.prototype.query = function(obj) {
	return this.sql(obj);
};

SqlBuilder.prototype.sql = function(obj) {
	var self = this;
	// @TODO: extend
	self._is = true;
	return self;
};

SqlBuilder.prototype.toString = function() {
	console.log('SqlBuilder.toString() is not supported.');
	return this;
};

SqlBuilder.prototype.toQuery = function(query) {
	console.log('SqlBuilder.toQuery() is not supported.');
	return this;
};

SqlBuilder.prototype.make = function(fn) {
	var self = this;
	fn.call(self, self)
	return self.agent || self;
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
	this.results = {};

	// Hidden:
	// this.$when;
}

Agent.prototype = {
	get $() {
		return new SqlBuilder(0, 0, this);
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

Agent.prototype.when = function(name, fn) {

	if (!this.$when)
		this.$when = {};

	if (!this.$when[name])
		this.$when[name] = [fn];
	else
		this.$when[name].push(fn);

	return this;
};

Agent.prototype.priority = function() {
	var self = this;
	var length = self.command.length - 1;

	if (!length)
		return self;

	var last = self.command[length];

	for (var i = length; i > -1; i--)
		self.command[i] = self.command[i - 1];

	self.command[0] = last;
	return self;
};

Agent.prototype.default = function(fn) {
	fn.call(this.results, this.results);
	return this;
};

Agent.query = function() {
	console.log('Agent.query() is not supported.');
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
	console.log('Agent.primary() is not supported.');
	return self;
};

Agent.prototype.expected = function(name, index, property) {

	var self = this;

	if (typeof(index) === 'string') {
		property = index;
		index = undefined;
	}

	return function() {
		var output = self.results[name];
		if (!output)
			return null;
		if (index === undefined) {
			if (property === undefined)
				return output;
			return output[property];
		}
		output = output[index];
		if (output)
			return output[property];
		return null;
	};
};

Agent.prototype.prepare = function(fn) {
	var self = this;
	self.command.push({ type: 'prepare', fn: fn });
	return self;
};

Agent.prototype.modify = function(fn) {
	var self = this;
	self.command.push({ type: 'modify', fn: fn });
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
		params = new SqlBuilder(0, 0, self);
	}

	if (queries[query])
		query = queries[query];

	self.command.push({ name: name, query: query, params: params, first: isFIRST(query) });
	return is ? params : self;
};

Agent.prototype.validate = function(fn, error, reverse) {
	var self = this;
	var type = typeof(fn);

	if (typeof(error) === 'boolean') {
		reverse = error;
		error = undefined;
	}

	if (type === 'string' && error === undefined) {
		// checks the last result
		error = fn;
		fn = undefined;
	}

	if (type === 'function') {
		self.command.push({ type: 'validate', fn: fn, error: error });
		return self;
	}

	var exec;

	if (reverse) {
		exec = function(err, results, next) {
			var id = fn === undefined || fn === null ? self.last : fn;
			if (id === null || id === undefined)
				return next(true);
			var r = results[id];
			if (r instanceof Array)
				return next(r.length === 0);
			if (r)
				return next(false);
			next(true);
		};
	} else {
		exec = function(err, results, next) {
			var id = fn === undefined || fn === null ? self.last : fn;
			if (id === null || id === undefined)
				return next(false);
			var r = results[id];
			if (r instanceof Array)
				return next(r.length > 0);
			if (r)
				return next(true);
			next(false);
		};
	}

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

function prepareValue(value, type) {

	if (value === undefined || value === null)
		return null;

	if (!type)
		type = typeof(value);

	if (type === 'function')
		return value();

	if (type === 'string')
		return value.trim();

	return value;
}

Agent.prototype._insert = function(item) {

	var self = this;
	var name = item.name;
	var values = item.values;
	var table = item.table;
	var primary = self.$primary;

	if (values instanceof SqlBuilder) {
		if (values._primary)
			primary = values._primary;
		values = values._set;
	}

	var keys = Object.keys(values);

	var columns = [];
	var columns_values = [];
	var params = [];
	var index = 1;

	for (var i = 0, length = keys.length; i < length; i++) {
		var key = keys[i];
		var value = values[key];

		var isRAW = key[0] === '!';
		if (isRAW)
			key = key.substring(1);

		if (item.without && item.without.indexOf(key) !== -1)
			continue;

		if (key[0] === '$')
			continue;

		if (value instanceof Array) {

			columns.push('"' + key + '"');

			var helper = [];

			for (var j = 0, sublength = value.length; j < sublength; j++) {
				helper.push('$' + index++);
				params.push(prepareValue(value[j]));
			}

			columns_values.push('(' + helper.join(',') + ')');

		} else {

			switch (key[0]) {
				case '+':
				case '-':
				case '*':
				case '/':
					key = key.substring(1);
					if (!value)
						value = 1;
					break;
			}

			columns.push('"' + key + '"');

			if (isRAW) {
				columns_values.push(value);
				continue;
			}

			columns_values.push('$' + index++);
			params.push(prepareValue(value));
		}
	}

	return { type: item.type, name: name, query: 'INSERT INTO ' + table + ' (' + columns.join(',') + ') VALUES(' + columns_values.join(',') + ') RETURNING ' + primary + ' as identity', params: params, first: true };
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

		var isRAW = key[0] === '!';
		if (isRAW)
			key = key.substring(1);

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

			columns.push('"' + key + '"=(' + helper.join(',') + ')');

		} else {

			switch (key[0]) {
				case '+':

					if (!value)
						value = 1;

					key = key.substring(1);
					columns.push('"' + key + '"=COALESCE("' + key + '",0)+$' + (index++));
					break;
				case '-':

					if (!value)
						value = 1;

					key = key.substring(1);
					columns.push('"' + key + '"=COALESCE("' + key + '",0)-$' + (index++));
					break;
				case '*':

					if (!value)
						value = 1;

					key = key.substring(1);
					columns.push('"' + key + '"=COALESCE("' + key + '",0)*$' + (index++));
					break;
				case '/':

					if (!value)
						value = 1;

					key = key.substring(1);
					columns.push('"' + key + '"=COALESCE("' + key + '",0)/$' + (index++));
					break;
				default:
					if (isRAW)
						columns.push('"' + key + '"=' + value);
					else
						columns.push('"' + key + '"=$' + (index++));
					break;
			}

			if (!isRAW)
				params.push(prepareValue(value));
		}
	}

	return { type: item.type, name: name, query: 'WITH rows AS (UPDATE ' + table + ' SET ' + columns.join(',') + condition.toString(this.id) + ' RETURNING 1) SELECT count(*)::int as "count" FROM rows', params: params, first: true, column: 'count' };
};

Agent.prototype._select = function(item) {
	return { name: item.name, query: item.condition.toQuery(item.query) + item.condition.toString(this.id), params: null, first: item.condition._take === 1, datatype: item.datatype };
};

Agent.prototype._delete = function(item) {
	return { name: item.name, query: 'WITH rows AS (' + item.query + item.condition.toString(this.id) + ' RETURNING 1) SELECT count(*)::int as "count" FROM rows', params: null, first: true, column: 'count' };
};

Agent.prototype.save = function(name, table, insert, maker) {

	if (typeof(table) === 'boolean') {
		maker = insert;
		insert = table;
		table = name;
		name = undefined;
	}

	var self = this;
	if (insert)
		maker(self.insert(name, table), true);
	else
		maker(self.update(name, table), false);

	return self;
};

Agent.prototype.insert = function(name, table, values, without) {

	var self = this;

	if (typeof(table) !== 'string') {
		without = values;
		values = table;
		table = name;
		name = self.index++;
	}

	if (values instanceof Array) {
		var tmp = without;
		without = values;
		values = tmp;
	}

	var is = false;
	if (!values) {
		is = true;
		values = new SqlBuilder(0, 0, self);
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

	var condition = new SqlBuilder(skip, take, self);
	var columns;

	if (schema instanceof Array) {
		columns = schema;
	} else if (typeof(schema) === 'string') {
		columns = [schema];
	} else {
		columns = [];
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
			return command.values ? command.values : command.condition;
	}
};

Agent.prototype.exists = function(name, table) {
	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	condition.first();
	self.command.push({ type: 'query', query: 'SELECT 1 as sqlagentcolumn_e FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn_e' });
	return condition;
};

Agent.prototype.count = function(name, table, column) {
	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	if (!column)
		column = '*';

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'query', query: 'SELECT COUNT(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1 });
	return condition;
};

Agent.prototype.max = function(name, table, column) {
	var self = this;
	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'query', query: 'SELECT MAX(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1 });
	return condition;
};

Agent.prototype.min = function(name, table, column) {
	var self = this;
	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'query', query: 'SELECT MAX(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1 });
	return condition;
};

Agent.prototype.avg = function(name, table, column) {
	var self = this;
	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'query', query: 'SELECT AVG(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1 });
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

	if (values instanceof Array) {
		var tmp = without;
		without = values;
		values = tmp;
	}

	var condition;

	if (values instanceof SqlBuilder)
		condition = values;
	else
		condition = new SqlBuilder(0, 0, self);

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

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'delete', query: 'DELETE FROM ' + table, name: name, condition: condition });
	return condition;
};

Agent.prototype.remove = function(name, table) {
	return this.delete(name, table);
};

Agent.prototype.ifnot = function(name, fn) {
	var self = this;
	self.prepare(function(error, response, resume) {
		if (response[name])
			return resume();
		fn.call(self, error, response);
		resume();
	});
	return self;
};

Agent.prototype.ifexists = function(name, fn) {
	var self = this;
	self.prepare(function(error, response, resume) {
		if (!response[name])
			return resume();
		fn.call(self, error, response);
		resume();
	});
	return self;
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
	if (self.errors)
		self.errors.push(e);
	next();
};

Agent.prototype._prepare = function(callback) {

	var self = this;

	self.isRollback = false;
	self.isTransaction = false;

	if (!self.errors)
		self.errors = self.isErrorBuilder ? new global.ErrorBuilder() : [];

	self.command.sqlagent(function(item, next) {

		if (item.type === 'validate') {
			try {
				item.fn(self.errors, self.results, function(output) {
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
				item.fn(self.errors, self.results);
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

		if (item.type === 'modify') {
			try {
				item.fn(self.results);
				next();
			} catch (e) {
				self.rollback('modify', e, next);
			}
			return;
		}

		if (item.type === 'prepare') {
			try {
				item.fn(self.errors, self.results, function() {
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

		if (self.skipCount) {
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

		if (current.condition instanceof SqlBuilder)
			current.query = current.query + current.condition.toString(self.id);

		var query = function(err, result) {

			if (err) {
				self.errors.push(current.name + ': ' + err.message);
				if (self.isTransaction)
					self.isRollback = true;
			} else {
				var rows = result.rows;

				if (current.type === 'insert') {

					if (rows.length) {
						var tmp = parseInt(rows[0].identity);
						if (isNaN(tmp)) {
							self.id = rows[0].identity;
						} else {
							self.id = tmp;
							rows[0].identity = tmp;
						}
					} else
						self.id = null;

					if (self.isPut === false)
						self.$id = self.id;
				}

				if (current.first && current.column) {
					if (rows.length)
						self.results[current.name] = current.column === 'sqlagentcolumn_e' ? true : current.datatype === 1 ? parseFloat(rows[0][current.column] || 0) : rows[0][current.column];
				} else if (current.first)
					self.results[current.name] = rows instanceof Array ? rows[0] : rows;
				else
					self.results[current.name] = rows;

				self.emit('data', current.name, self.results);

				if (self.$when) {
					var tmp = self.$when[current.name];
					if (tmp) {
						for (var i = 0, length = tmp.length; i < length; i++)
							tmp[i](self.errors, self.results);
					}
				}
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
		} else if (self.errors.length)
			err = self.errors;

		if (Agent.debug)
			console.log(self.debugname, '----- done (' + self.time + ' ms)');

		self.emit('end', err, self.results, self.time);

		if (callback)
			callback(err, self.returnIndex !== undefined ? self.results[self.returnIndex] : self.results);
	});

	return self;
};

Agent.prototype.exec = function(callback, returnIndex) {

	var self = this;

	if (Agent.debug) {
		self.debugname = 'sqlagent/mongodb (' + Math.floor(Math.random() * 1000) + ')';
		self.debugtime = Date.now();
	}

	if (returnIndex !== undefined && typeof(returnIndex) !== 'boolean')
		self.returnIndex = returnIndex;
	else
		delete self.returnIndex;

	if (!self.command.length) {
		if (callback)
			callback.call(self, null, {});
		return self;
	}

	if (Agent.debug)
		console.log(self.debugname, '----- exec');

	database.connect(self.options, function(err, client, done) {

		if (err) {
			callback.call(self, err, {});
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
};

Agent.prototype.writeBuffer = function(buffer, callback) {
};

Agent.prototype.readStream = function(oid, buffersize, callback) {
};

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
global.SqlBuilder = SqlBuilder;