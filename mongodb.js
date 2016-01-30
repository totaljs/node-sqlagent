var database = require('mongodb');
var Events = require('events');
var columns_cache = {};
var NOOP = function(){};
var DB;

require('./index');

function SqlBuilder(skip, take, agent) {
	this.agent = agent;
	this.builder = {};
	this._order = null;
	this._skip = skip >= 0 ? skip : 0;
	this._take = take >= 0 ? take : 0;
	this._set = null;
	this._inc = null;
	this._scope = 0;
	this._fields;
	this._schema;
	this._group;
	this._having;
	this._primary;
	this._is = false;
	this._isfirst = false;
	this._prepare;
}

SqlBuilder.prototype = {
	get data() {
		var obj = {};
		if (this._set)
			obj.$set = this._set;
		if (this._inc)
			obj.$inc = this._inc;
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

	if (builder._prepare)
		self._prepare = copy(builder._prepare);

	if (builder._fields)
		self._fields = copy(builder._fields);

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
	console.log('SqlBuilder.schema(' + name + ') is not supported.');
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
	self._isfirst = true;
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
	var type = typeof(value);

	if (name[0] === '!' && type !== 'function') {
		name = name.substring(1);
		value = ObjectID.parse(value);
	}

	switch (operator) {
		case '=':
			self.$scope(name, value, type, 1);
			break;
		case '<>':
			self.$scope(name, { $ne: value }, type, 5);
			break;
		case '>':
			self.$scope(name, { $gt: value }, type, 6);
			break;
		case '<':
			self.$scope(name, { $lt: value }, type, 7);
			break;
		case '>=':
			self.$scope(name, { $gte: value }, type, 8);
			break;
		case '<=':
			self.$scope(name, { $lte: value }, type, 9);
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

SqlBuilder.prototype.$scope = function(name, obj, type, code, raw) {

	var self = this;
	var is = false;

	if (type === 'function') {
		if (!self._prepare)
			self._prepare = [];
		self._prepare.push({ context: self.builder, name: name, value: obj, scope: self._scope, type: code, raw: raw });
		is = true;
	}

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
		if (is)
			self._prepare[self._prepare.length - 1].index = self.builder['$or'].length - 1;
	}

	if (self._scope === 2) {
		if (!self.builder['$and'])
			self.builder['$and'] = [];
		var filter = {};
		filter[name] = obj;
		self.builder['$and'].push(filter);
		if (is)
			self._prepare[self._prepare.length - 1].index = self.builder['$and'].length - 1;
	}

	return self;
};

SqlBuilder.prototype.in = function(name, value) {
	var self = this;
	self.$scope(name, { '$in': value }, typeof(value), 4);
	return self;
};

SqlBuilder.prototype.like = function(name, value, where) {
	var self = this;
	var type = typeof(value);
	var val = type === 'function' ? '' : value.toString();

	switch (where) {
		case 'beg':
		case 'begin':
			self.$scope(name, { $regex: '^' + val }, type, 2, value);
			break;
		case 'end':
			self.$scope(name, { $regex: val + '$' }, type, 2, value);
			break;
		case '*':
		default:
			self.$scope(name, { $regex: val }, type, 2, value);
			break;
	}

	self._is = true;
	return self;
};

SqlBuilder.prototype.between = function(name, valueA, valueB) {
	var self = this;
	var obj = {};
	var typeA = typeof(valueA);
	var typeB = typeof(valueB);
	self.$scope(name, { $gte: valueA, $lte: valueB }, typeA === 'function' || typeB === 'function' ? 'function' : typeA, 3);
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

SqlBuilder.prototype.prepare = function() {

	if (!this._prepare)
		return this;

	for (var i = 0, length = this._prepare.length; i < length; i++) {

		var prepare = this._prepare[i];
		// prepare.type 1 - where "="
		// prepare.type 2 - like
		// prepare.type 3 - between
		// prepare.type 4 - in

		// or
		if (prepare.scope === 1) {
			prepare.context['$or'][prepare.index] = prepare.value();
			continue;
		}

		// and
		if (prepare.scope === 2) {
			prepare.context['$and'][prepare.index] = prepare.value();
			continue;
		}

		if (prepare.type === 1) {
			prepare.context[prepare.name] = prepare.value();
			continue;
		}

		if (prepare.type === 2) {
			prepare.value.$regex += prepare.raw();
			continue;
		}

		if (prepare.type === 3) {
			if (typeof(prepare.value.$gte) === 'function')
				prepare.value.$gte = prepare.value.$gte();
			if (typeof(prepare.value.$lte) === 'function')
				prepare.value.$lte = prepare.value.$lte();
			continue;
		}

		if (prepare.type === 4) {
			prepare.value.$in = prepare.value.$in();
			continue;
		}

		if (prepare.type === 5) {
			prepare.value.$ne = prepare.value.$ne();
			continue;
		}

		if (prepare.type === 6) {
			prepare.value.$gt = prepare.value.$gt();
			continue;
		}

		if (prepare.type === 7) {
			prepare.value.$lt = prepare.value.$lt();
			continue;
		}

		if (prepare.type === 8) {
			prepare.value.$gte = prepare.value.$gte();
			continue;
		}

		if (prepare.type === 9) {
			prepare.value.$lte = prepare.value.$lte();
			continue;
		}
	}

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

Agent.prototype.push = function(name, table, fn) {
	var self = this;

	if (typeof(table) !== 'string') {
		fn = table;
		table = name;
		name = self.index++;
	}

	self.command.push({ type: 'push', name: name, table: table, fn: fn });
	return self;
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
	console.log('Agent.begin() is not supported.');
	return self;
};

Agent.prototype.end = function() {
	var self = this;
	console.log('Agent.end() is not supported.');
	return self;
};

Agent.prototype.commit = function() {
	console.log('Agent.commit() is not supported.');
	return this;
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

Agent.prototype.insert = function(name, table) {

	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	var fn = function(db, builder, helper, callback) {
		builder.prepare();

		var data = builder.data;

		if (data.$inc) {

			if (data.$set)
				data.$set = {};

			Object.keys(data.$inc).forEach(function(key) {
				data.$set[key] = data.$inc[key];
			});

			delete data.$inc;
		}

		db.insert(data, function(err, response) {
			callback(err, response ? response.result.insertedCount > 0 : false);
		});
	};

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn });
	return condition;
};

Agent.prototype.select = function(name, table) {
	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);

	var fn = function(db, builder, helper, callback) {

		builder.prepare();
		if (builder._isfirst) {
			if (builder._fields)
				db.findOne(builder.builder, builder._fields, callback);
			else
				db.findOne(builder.builder, callback);
			return;
		}

		var cursor = db.find(builder.builder);
		if (builder._fields)
			cursor.project(builder._fields);
		if (builder._order)
			cursor.sort(builder._order);
		if (builder._take)
			cursor.limit(builder._take);
		if (builder._skip)
			cursor.skip(builder._skip);
		cursor.toArray(callback);
	};

	self.command.push({ type: 'query', name: name, table: table, condition: condition, fn: fn });
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
	condition.fields('_id');

	var fn = function(db, builder, helper, callback) {
		builder.prepare();
		db.findOne(builder.builder, function(err, doc) {
			callback(err, doc ? true : false);
		});
	};

	self.command.push({ type: 'query', name: name, table: table, condition: condition });
	return condition;
};

Agent.prototype.count = function(name, table, column) {
	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var fn = function(db, builder, helper, callback) {
		builder.prepare();
		db.find(builder.builder).count(callback);
	};

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn, helper: helper });
	return condition;
};

Agent.prototype.max = function(name, table, column) {
	throw new Error('Agent.max(' + name + ') is not supported now.');
};

Agent.prototype.min = function(name, table, column) {
	throw new Error('Agent.min(' + name + ') is not supported now.');
};

Agent.prototype.avg = function(name, table, column) {
	throw new Error('Agent.avg(' + name + ') is not supported now.');
};

Agent.prototype.updateOnly = function(name, table, values, only) {
	throw new Error('Agent.updateOnly(' + name + ') is not supported now.');
};

Agent.prototype.update = function(name, table) {

	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	var fn = function(db, builder, helper, callback) {
		builder.prepare();
		if (builder._isfirst) {
			db.updateOne(builder.builder, builder.data, function(err, response) {
				callback(err, response ? response.result.nModified > 0 : false);
			});
		} else {
			db.update(builder.builder, builder.data, { multi: true }, function(err, response) {
				callback(err, response ? response.result.nModified > 0 : false);
			});
		}
	};

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn });
	return condition;
};

Agent.prototype.delete = function(name, table) {

	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	var fn = function(db, builder, helper, callback) {
		builder.prepare();
		if (builder._isfirst) {
			db.remove(builder.builder, { single: true }, function(err, response) {
				callback(err, response ? response.result.nModified > 0 : false);
			});
		} else {
			db.remove(builder.builder, function(err, response) {
				callback(err, response ? response.result.nModified > 0 : false);
			});
		}
	};

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn });
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

		if (item.type === 'push') {
			item.fn(self.db.collection(item.table), function(err, response) {

				self.last = item.name;

				if (err) {
					self.errors.push(item.name + ': ' + err.message);
					next();
					return;
				}

				self.results[item.name] = response;
				self.emit('data', item.name, response);
				next();
			});
			return;
		}

		if (item.type !== 'query') {
			next();
			return;
		}

		item.fn(self.db.collection(item.table), item.condition, item.helper, function(err, response) {

			self.last = item.name;

			if (err) {
				self.errors.push(item.name + ': ' + err.message);
				next();
				return;
			}

			self.results[item.name] = response;
			self.emit('data', item.name, response);

			if (!self.$when) {
				next();
				return
			}

			var tmp = self.$when[item.name];
			if (tmp) {
				for (var i = 0, length = tmp.length; i < length; i++)
					tmp[i](self.errors, self.results);
			}
			next();
		});

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

	self.db = DB;
	self._prepare(callback);
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

Agent.prototype.readFile = function(id, callback) {
	var reader = new GridStore(DB, ObjectID.parse(id), 'r');
	reader.open(function(err, fs) {

		if (err) {
			reader.close();
			reader = null;
			return callback(err);
		}

		callback(null, fs, function() {
			reader.close();
			reader = null;
		});
	});
}

Agent.prototype.readToStream = function(id, stream, callback) {
	var reader = new GridStore(DB, ObjectID.parse(id), 'r');
	reader.open(function(err, fs) {

		if (err) {
			reader.close();
			reader = null;
			if (callback)
				return callback(err);
			return;
		}

		fs.stream(true).pipe(stream).on('close', function() {
			reader.close();
			reader = null;
			if (callback)
				callback(null);
		});

		callback(null, fs, function() {
			reader.close();
			reader = null;
		});
	});
}

Agent.prototype.writeFile = function(id, filename, name, meta, callback) {

	if (typeof(meta) === 'function') {
		var tmp = callback;
		callback = meta;
		meta = tmp;
	}

	var arg = [];
	var grid = new GridStore(DB, id, name, 'w', { metadata: meta });

	grid.open(function(err, fs) {

		if (err) {
			grid.close();
			grid = null;
			if (callback)
				callback(err);
			return;
		}

		grid.writeFile(filename, function(err) {
			grid.close();
			grid = null;
			if (!callback)
				return;
			callback(err);
		});
	});
}

Agent.prototype.writeBuffer = function(id, buffer, name, meta, callback) {

	if (!callback)
		callback = NOOP;

	if (typeof(meta) === 'function') {
		var tmp = callback;
		callback = meta;
		meta = tmp;
	}

	var arg = [];
	var grid = new GridStore(DB, id ? id : new ObjectID(), name, 'w', { metadata: meta });

	grid.open(function(err, fs) {

		if (err) {
			grid.close();
			grid = null;
			return callback(err);
		}

		grid.write(buffer, function(err) {
			if (err)
				return callback(err);
			callback(null);
			grid.close();
			grid = null;
		});
	});
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

	framework.wait('database');
	database.connect(conn, function(err, db) {
		if (err)
			throw err;
		DB = db;
		framework.wait('database');
		framework.emit('database');
	});

	framework.database = function(errorBuilder) {
		return new Agent(conn, errorBuilder);
	};
};

module.exports = Agent;
global.SqlBuilder = SqlBuilder;
global.ObjectID = database.ObjectID;
global.GridStore = database.GridStore;

ObjectID.parse = function(value, isArray) {
	if (value instanceof ObjectID)
		return value;
	if (isArray || value instanceof Array)
		return ObjectID.parseArray(value);
	try {
		return new ObjectID(value);
	} catch (e) {
		return null;
	}
};

ObjectID.parseArray = function(value) {

	if (typeof(value) === STRING)
		value = value.split(',');

	var arr = [];

	if (!(value instanceof Array))
		return arr;

	for (var i = 0, length = value.length; i < length; i++) {
		var id = ObjectID.parse(value[i]);
		if (id)
			arr.push(id);
	}

	return arr;
};
