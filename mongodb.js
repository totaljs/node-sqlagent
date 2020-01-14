const database = require('mongodb');
const Fs = require('fs');
const columns_cache = {};
const CONNECTIONS = {};
const NOOP = function(){};
const PROJECTION = { _id: 1 };
const FILEREADERFILTER = {};
const REG_APO = /'/;
const OPTIONS = { useNewUrlParser: true, useUnifiedTopology: true };

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

SqlBuilder.prototype.callback = function(fn) {
	this.$callback = fn;
	return this;
};

SqlBuilder.prototype.assign = function() {
	throw new Error('This method is not supported in MongoDB.');
};

SqlBuilder.prototype.replace = function(builder, reference) {
	var self = this;

	self.builder = reference ? builder.builder : copy(builder.builder);
	self.scope = builder.scope;

	if (builder._order)
		self._order = reference ? builder._order : copy(builder._order);

	self._skip = builder._skip;
	self._take = builder._take;

	if (builder._set)
		self._set = reference ? builder._set : copy(builder._set);

	if (builder._inc)
		self._inc = reference ? builder._inc : copy(builder._inc);

	if (builder._prepare)
		self._prepare = reference ? builder._prepare : copy(builder._prepare);

	if (builder._fields)
		self._fields = reference ? builder._fields : copy(builder._fields);

	self._is = builder._is;
	self._isfirst = builder._isfirst;

	return self;
};

SqlBuilder.prototype.debug = function(type) {
	var obj = {};
	obj.type = type;
	obj.condition = this.builder;
	this._fields && (obj.project = this._fields);
	this._order && (obj.sort = this._order);
	this._set && (obj.set = this._set);
	this._inc && (obj.inc = this._inc);
	obj.take = this._take;
	obj.skip = this._skip;
	return obj;
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
}

SqlBuilder.prototype.clone = function() {
	var builder = new SqlBuilder(0, 0, this.agent);
	return builder.replace(this);
};

SqlBuilder.prototype.join = function(name) {
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
		if (key !== '_id' && key[0] !== '$' && name[key] !== undefined)
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
			switch (type) {
				case '+':
				case '-':
				case '*':
				case '/':
					value = value.substring(1).parseFloat();
					break;
				default:
					type = '+';
					value = value.parseFloat();
					break;
			}
		} else {
			if(type !== '-')
				type = '+';
			if (value == null)
				value = 1;
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
		name[key] && self.inc(key, name[key]);
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

	var key = '<' + name + '.' + (desc || 'false') + '>';

	if (columns_cache[key]) {
		self._order[columns_cache[key].name] = columns_cache[key].value;
		return;
	}

	var lowered = name.toLowerCase();
	var index = lowered.lastIndexOf('desc');

	if (index !== -1 || lowered.lastIndexOf('asc') !== -1) {
		name = name.split(' ')[0];
		desc = index !== -1;
	}

	columns_cache[key] = {};
	columns_cache[key].name = name;
	columns_cache[key].value = self._order[name] = desc ? -1 : 1;
	return self;
};

SqlBuilder.prototype.random = function() {
	console.log('SqlBuilder.random() is not supported.');
	return this;
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
	console.log('SqlBuilder.escape() is not supported.');
	return value;
};

SqlBuilder.column = function(name) {
	console.log('SqlBuilder.column() is not supported.');
	return name;
};

SqlBuilder.prototype.group = function() {
	console.log('SqlBuilder.group() is not supported.');
	return this;
};

SqlBuilder.prototype.having = function() {
	console.log('SqlBuilder.having() is not supported.');
	return this;
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
	var typeA = typeof(valueA);
	var typeB = typeof(valueB);
	self.$scope(name, { $gte: valueA, $lte: valueB }, typeA === 'function' || typeB === 'function' ? 'function' : typeA, 3);
	self._is = true;
	return self;
};

SqlBuilder.prototype.query = function(name, value) {
	return this.$scope(name, value, undefined, 10);
};

SqlBuilder.prototype.sql = function() {
	console.log('SqlBuilder.sql() is not supported.');
	return this;
};

SqlBuilder.prototype.toString = function() {
	console.log('SqlBuilder.toString() is not supported.');
	return this;
};

SqlBuilder.prototype.toQuery = function() {
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
	fn.call(self, self);
	return self.agent || self;
};

function Agent(name, error) {
	this.connection = name;
	this.isErrorBuilder = typeof(global.ErrorBuilder) !== 'undefined' ? true : false;
	this.errors = this.isErrorBuilder ? error : null;
	this.clear();
	this.$events = {};

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

Agent.embedded = function() {
	require('mongodb-nosqlembedded').init(Agent);
};

// Debug mode (output to console)
Agent.debug = false;

Agent.connect = function(conn, callback) {
	database.connect(conn, OPTIONS, function(err, db) {
		if (err) {
			if (callback)
				return callback(err);
			throw err;
		}

		if (db.db) {
			// new mongodb
			if (conn[conn.length - 1] === '/')
				conn = conn.substring(0, conn.length - 1);
			var name = conn.substring(conn.lastIndexOf('/') + 1);
			var index = name.indexOf('?');
			if (index !== -1)
				name = name.substring(0, index);
			db = db.db(name);
		}

		CONNECTIONS[conn] = db;
		callback && callback();
	});
	return function(error) {
		return new Agent(conn, error);
	};
};

Agent.prototype.promise = function(index, fn) {
	var self = this;

	if (typeof(index) === 'function') {
		fn = index;
		index = undefined;
	}

	return new Promise(function(resolve, reject) {
		self.exec(function(err, result) {
			if (err)
				reject(err);
			else
				resolve(fn ? fn(result) : result);
		}, index);
	});
};

Agent.prototype.emit = function(name, a, b, c, d, e, f, g) {
	var evt = this.$events[name];
	if (evt) {
		var clean = false;
		for (var i = 0, length = evt.length; i < length; i++) {
			if (evt[i].$once)
				clean = true;
			evt[i].call(this, a, b, c, d, e, f, g);
		}
		if (clean) {
			evt = evt.remove(n => n.$once);
			if (evt.length)
				this.$events[name] = evt;
			else
				this.$events[name] = undefined;
		}
	}
	return this;
};

Agent.prototype.on = function(name, fn) {

	if (!fn.$once)
		this.$free = false;

	if (this.$events[name])
		this.$events[name].push(fn);
	else
		this.$events[name] = [fn];
	return this;
};

Agent.prototype.once = function(name, fn) {
	fn.$once = true;
	return this.on(name, fn);
};

Agent.prototype.removeListener = function(name, fn) {
	var evt = this.$events[name];
	if (evt) {
		evt = evt.remove(n => n === fn);
		if (evt.length)
			this.$events[name] = evt;
		else
			this.$events[name] = undefined;
	}
	return this;
};

Agent.prototype.removeAllListeners = function(name) {
	if (name === true)
		this.$events = EMPTYOBJECT;
	else if (name)
		this.$events[name] = undefined;
	else
		this.$events[name] = {};
	return this;
};

Agent.prototype.clear = function() {
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
	this.$primary = '_id';
	this.results = {};
	this.builders = {};

	if (this.$when)
		this.$when = undefined;

	if (this.errors && this.isErrorBuilder)
		this.errors.clear();
	else if (this.errors)
		this.errors = null;

	return this;
};

Agent.prototype.when = function(name, fn) {

	if (!this.$when)
		this.$when = {};

	if (this.$when[name])
		this.$when[name].push(fn);
	else
		this.$when[name] = [fn];

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
	if (name)
		self.skips[name] = true;
	else
		self.skipCount++;
	return self;
};

Agent.prototype.primaryKey = Agent.prototype.primary = function() {
	console.log('Agent.primary() is not supported.');
	return this;
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
		if (index === undefined)
			return property === undefined ? output : get(output, property);
		output = output[index];
		return output ? get(output, property) : null;
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
	self.command.push({ type: 'put', params: value, disable: value == null });
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

	if (type === 'string' && typeof(error) === 'function' && typeof(reverse) === 'string')
		return self.validate2(fn, error, reverse);

	var exec;

	if (reverse) {
		exec = function(err, results, next) {
			var id = fn == null ? self.last : fn;
			if (id == null)
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
			var id = fn == null ? self.last : fn;
			if (id == null)
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

// validate2('result', n => n.length > 0, 'error');
Agent.prototype.validate2 = function(name, fn, err) {
	var self = this;
	var type = typeof(fn);

	if (type === 'string') {
		type = err;
		err = fn;
		fn = type;
	}

	var validator = function(err, results, next) {
		if (fn(results[name]))
			return next(true);
		err.push(err || name);
		next(false);
	};

	self.command.push({ type: 'validate', fn: validator, error: err });
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

Agent.prototype.save = function(name, table, insert, maker) {

	if (typeof(table) === 'boolean') {
		maker = insert;
		insert = table;
		table = name;
		name = undefined;
	}

	var self = this;
	if (insert) {
		maker(self.insert(name, table), true);
		return self;
	}

	var builder = self.update(name, table);
	builder.first();
	maker(builder, false);

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

		if (!builder._set && !builder._inc) {
			var err = new Error('No data for inserting.');
			builder.$callback && builder.$callback(err);
			callback(err, null);
			return;
		}

		var data = builder.data;

		if (data.$inc) {

			if (!data.$set)
				data.$set = {};

			Object.keys(data.$inc).forEach(function(key) {
				data.$set[key] = data.$inc[key];
			});

			data.$inc = undefined;
		}

		self.$events.query && self.emit('query', name, builder.debug('insert'));

		var method = db.insertOne || db.insert;
		method.call(db, data.$set, function(err, response) {
			var id = response ? (response.insertedCount ? response.insertedId || (response.insertedIds && response.insertedIds.length > 1 ? response.insertedIds : response.insertedIds[0]) : null) : null;
			self.id = id;
			if (!self.isPut)
				self.$id = self.id;
			var data = id ? { identity: id } : null;
			builder.$callback && builder.$callback(err, data);
			callback(err, data);
		});
	};

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.listing = function(name, table) {
	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);

	var fn = function(db, builder, helper, callback) {

		builder.prepare();
		builder._isfirst && console.warn('You can\'t use "builder.first()" for ".listing()".');

		var cursor = db.find(builder.builder);
		cursor.project(PROJECTION);
		cursor.count(function(err, count) {

			if (err)
				return callback(err);

			self.$events.query && self.emit('query', name, builder.debug('listing'));
			var output = {};
			output.count = count;
			cursor = db.find(builder.builder);
			builder._fields && cursor.project(builder._fields);
			builder._order && cursor.sort(builder._order);
			builder._take && cursor.limit(builder._take);
			builder._skip && cursor.skip(builder._skip);
			cursor.toArray(function(err, docs) {
				if (err)
					return callback(err);
				output.items = docs;
				output.page = ((builder._skip || 0) / (builder._take || 0)) + 1;
				output.limit = builder._take || 0;
				output.pages = Math.ceil(output.count / output.limit);
				builder && builder.$callback && builder.$callback(null, output);
				callback(null, output);
			});
		});
	};

	self.command.push({ type: 'query', name: name, table: table, condition: condition, fn: fn });
	self.builders[name] = condition;
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

		var cb = function(err, data) {
			builder.$callback && builder.$callback(err, data);
			callback(err, data);
		};

		builder.prepare();
		self.$events.query && self.emit('query', name, builder.debug('select'));

		if (builder._isfirst && !builder._order) {
			if (builder._fields)
				db.findOne(builder.builder, { projection: builder._fields }, cb);
			else
				db.findOne(builder.builder, cb);
		} else {
			var cursor = db.find(builder.builder);
			builder._fields && cursor.project(builder._fields);
			builder._order && cursor.sort(builder._order);
			builder._take && cursor.limit(builder._take);
			builder._skip && cursor.skip(builder._skip);
			cursor.toArray(cb);
		}
	};

	self.command.push({ type: 'query', name: name, table: table, condition: condition, fn: fn });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.compare = function(name, table, obj, keys) {

	var self = this;

	if (typeof(table) !== 'string') {
		keys = obj;
		obj = table;
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	condition.first();

	var fn = function(db, builder, helper, callback) {

		var prop = keys ? keys : builder._fields ? Object.keys(builder._fields) : Object.keys(obj);

		!builder._fields && builder.fields.apply(builder, prop);
		builder.prepare();
		self.$events.query && self.emit('query', name, builder.debug('compare'));

		db.findOne(builder.builder, builder._fields ? { projection: builder._fields } : null, function(err, doc) {

			if (err)
				return callback(err);

			var val = doc;
			var diff;

			if (val) {
				diff = [];
				for (var i = 0, length = prop.length; i < length; i++) {
					var key = prop[i];
					var a = val[key];
					var b = obj[key];
					a !== b && diff.push(key);
				}
			} else
				diff = prop;

			callback(null, diff.length ? { diff: diff, record: val, value: obj } : false);
		});
	};

	self.command.push({ type: 'query', name: name, table: table, condition: condition, fn: fn });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.find = Agent.prototype.builder = function(name) {
	return this.builders[name];
};

const EMPTYEXISTS = { projection: { _id: 1 }};

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
		self.$events.query && self.emit('query', name, builder.debug('exists'));
		db.findOne(builder.builder, EMPTYEXISTS, function(err, doc) {
			builder.$callback && builder.$callback(err, !!doc);
			callback(err, !!doc);
		});
	};

	self.command.push({ type: 'query', name: name, table: table, condition: condition, fn: fn });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.count = function(name, table, column) {
	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	condition.fields(column || '_id');

	var fn = function(db, builder, helper, callback) {
		builder.prepare();
		self.$events.query && self.emit('query', name, builder.debug('count'));
		db.find(builder.builder).count(function(err, count) {
			builder.$callback && builder.$callback(err, count);
			callback(err, count);
		});
	};

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.max = function(name, table, column) {

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var self = this;
	var fn = function(db, builder, helper, callback) {

		builder.prepare();
		builder.first();
		self.$events.query && self.emit('query', name, builder.debug('max'));

		var cursor = db.find(builder.builder);
		cursor.sort(builder._order);
		cursor.project(builder._fields);
		cursor.limit(1);
		cursor.toArray(function(err, response) {
			var data = response && response.length ? response[0][helper] : 0;
			builder.$callback && builder.$callback(err, data);
			callback(err, data);
		});
	};

	var condition = new SqlBuilder(0, 0, self);
	condition.fields(column);
	condition.sort(column, true);

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn, helper: column });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.min = function(name, table, column) {

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var self = this;
	var fn = function(db, builder, helper, callback) {

		builder.prepare();
		builder.first();
		self.$events.query && self.emit('query', name, builder.debug('min'));

		var cursor = db.find(builder.builder);
		cursor.sort(builder._order);
		cursor.project(builder._fields);
		cursor.limit(1);
		cursor.toArray(function(err, response) {
			var data = response && response.length ? response[0][helper] : 0;
			builder.$callback && builder.$callback(err, data);
			callback(err, data);
		});
	};

	var condition = new SqlBuilder(0, 0, self);
	condition.fields(column);
	condition.sort(column, false);

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn, helper: column });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.avg = function(name) {
	throw new Error('Agent.avg(' + name + ') is not supported now.');
};

Agent.prototype.updateOnly = function(name, table, values) {
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

		if (!builder._set && !builder._inc) {
			var err = new Error('No data for update.');
			builder.$callback && builder.$callback(err);
			callback(err, null);
			return;
		}

		self.$events.query && self.emit('query', name, builder.debug('update'));

		if (builder._isfirst) {
			db.updateOne(builder.builder, builder.data, function(err, response) {
				var data = response ? (response.result.nModified || response.result.n) : 0;
				builder.$callback && builder.$callback(err, data);
				callback(err, data);
			});
		} else {
			var method = db.updateMany || db.update;
			method.call(db, builder.builder, builder.data, { multi: true }, function(err, response) {
				var data = response ? (response.result.nModified || response.result.n) : 0;
				builder.$callback && builder.$callback(err, data);
				callback(err, data);
			});
		}
	};

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn });
	self.builders[name] = condition;
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
		self.$events.query && self.emit('query', name, builder.debug('delete'));
		var method = db.removeOne || db.remove;
		if (builder._isfirst) {
			method.call(db, builder.builder, { single: true }, function(err, response) {
				var data = response ? (response.result.nRemoved || response.result.n) : 0;
				builder.$callback && builder.$callback(data);
				callback(err, data);
			});
		} else {
			method.call(db, builder.builder, function(err, response) {
				var data = response ? (response.result.nRemoved || response.result.n) : 0;
				builder.$callback && builder.$callback(data);
				callback(err, data);
			});
		}
	};

	self.command.push({ type: 'query', table: table, name: name, condition: condition, fn: fn });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.remove = function(name, table) {
	return this.delete(name, table);
};

Agent.prototype.ifnot = function(name, fn) {
	var self = this;
	self.prepare(function(error, response, resume) {
		var value = response[name];
		if (value instanceof Array) {
			if (value.length)
				return resume();
		} else if (value)
			return resume();
		fn.call(self, error, response, value);
		setImmediate(resume);
	});
	return self;
};

Agent.prototype.ifexists = function(name, fn) {
	var self = this;
	self.prepare(function(error, response, resume) {

		var value = response[name];
		if (value instanceof Array) {
			if (!value.length)
				return resume();
		} else if (!value)
			return resume();

		fn.call(self, error, response, value);
		setImmediate(resume);
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
		delete self.builders[name];
		return true;
	}
	return false;
};

Agent.prototype.close = function() {
	var self = this;
	self.done && self.done();
	self.done = null;
	return self;
};

Agent.prototype.rollback = function(where, e, next) {
	var self = this;
	self.errors && self.errors.push(e);
	next();
};

Agent.prototype._prepare = function(callback) {

	var self = this;

	if (!self.errors)
		self.errors = self.isErrorBuilder ? new global.ErrorBuilder() : [];

	self.command.sqlagent(function(item, next) {

		if (item.type === 'validate') {
			try {
				var tmp = item.fn(self.errors, self.results, function(output) {
					if (output === true || output === undefined)
						return next();
					// reason
					if (typeof(output) === 'string')
						self.errors.push(output);
					else if (item.error)
						self.errors.push(item.error);
					next(false);
				});

				var type = typeof(tmp);
				if (type !== 'boolean' && type !== 'string')
					return;
				if (tmp === true || tmp === undefined)
					return next();
				if (typeof(tmp) === 'string')
					self.errors.push(tmp);
				else if (item.error)
					self.errors.push(item.error);
				next(false);
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
				item.fn(self.errors, self.results, () => next());
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
				self.$events.data && self.emit('data', item.name, response);
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

			var val = item.condition._isfirst && item.condition._order ? (response instanceof Array ? response[0] : response) : response;
			self.results[item.name] = val;
			self.$events.data && self.emit('data', item.name, val);

			if (!self.$when) {
				next();
				return;
			}

			var tmp = self.$when[item.name];
			if (tmp) {
				for (var i = 0, length = tmp.length; i < length; i++)
					tmp[i](self.errors, self.results, self.results[item.name]);
			}
			next();
		});

	}, function() {

		if (Agent.debug || self.debug) {
			self.time = Date.now() - self.debugtime;
			console.log(self.debugname, '----- done (' + self.time + ' ms)');
		}

		self.index = 0;
		self.done && self.done();
		self.done = null;
		var err = null;

		if (self.isErrorBuilder) {
			if (self.errors.hasError())
				err = self.errors;
		} else if (self.errors.length)
			err = self.errors;

		self.$events.end && self.emit('end', err, self.results, self.time);
		callback && callback(err, self.returnIndex !== undefined ? self.results[self.returnIndex] : self.results);
	});

	return self;
};

Agent.prototype.exec = function(callback, returnIndex) {

	var self = this;

	if (Agent.debug || self.debug) {
		self.debugname = 'sqlagent/mongodb (' + Math.floor(Math.random() * 1000) + ')';
		self.debugtime = Date.now();
	}

	if (returnIndex !== undefined && typeof(returnIndex) !== 'boolean')
		self.returnIndex = returnIndex;
	else
		self.returnIndex = undefined;

	if (!self.command.length) {
		callback && callback.call(self, null, {});
		return self;
	}

	(Agent.debug || self.debug) && console.log(self.debugname, '----- exec');

	connect(self.connection, function(err, db) {

		if (err) {
			!self.errors && (self.errors = self.isErrorBuilder ? new global.ErrorBuilder() : []);
			self.errors.push(err);
			callback && callback.call(self, self.errors, {});
			return;
		}

		self.db = db;
		self._prepare(callback);
	});

	return self;
};

function connect(conn, callback, index) {
	var db = CONNECTIONS[conn];
	if (db)
		return callback(null, db);

	if (index > 60) {
		callback(new Error('SQLAgent: timeout to connect into the database.'));
		return;
	}

	if (index === undefined)
		index = 1;

	setTimeout(() => connect(conn, callback, index + 1), 100);
}

Agent.prototype.$$exec = function(returnIndex) {
	var self = this;
	return function(callback) {
		return self.exec(callback, returnIndex);
	};
};

Agent.destroy = function() {
	throw new Error('Not supported.');
};

Agent.prototype.readFile = function(id, options, callback) {

	if (typeof(options) === 'function') {
		callback = options;
		options = null;
	}

	connect(this.connection, function(err, db) {

		if (err)
			return callback(err);

		var bucket = new database.GridFSBucket(db, options);

		if (bucket.openUploadStream) {
			console.error('SQLAgent error: readFile() is not supported for MongoDB, use readStream().');
		} else {
			FILEREADERFILTER._id = id;
			bucket.find(FILEREADERFILTER).nextObject(function(err, doc) {
				if (!err && !doc)
					err = new Error('File not found.');
				if (err)
					return callback(err, null, NOOP);
				callback(null, new GridFSObject(doc._id, doc.metadata, doc.filename, doc.length, doc.contentType, bucket), NOOP, doc.metadata, doc.length, doc.filename);
			});
		}
	});
};

Agent.prototype.readStream = function(id, options, callback) {

	if (typeof(options) === 'function') {
		callback = options;
		options = null;
	}

	connect(this.connection, function(err, db) {

		if (err)
			return callback(err);

		var bucket = new database.GridFSBucket(db, options);

		if (bucket.openUploadStream) {
			FILEREADERFILTER._id = id;
			db.collection('fs.files').findOne(FILEREADERFILTER, function(err, doc) {
				if (!err && !doc)
					err = new Error('File not found.');
				if (err)
					return callback(err, null, NOOP);

				callback(null, bucket.openDownloadStream(id), doc.metadata, doc.length, doc.filename);
			});
		} else {
			FILEREADERFILTER._id = id;

			bucket.find(FILEREADERFILTER).nextObject(function(err, doc) {
				if (!err && !doc)
					err = new Error('File not found.');
				if (err)
					return callback(err);
				callback(null, new GridFSObject(doc._id, doc.metadata, doc.filename, doc.length, doc.contentType, bucket).stream(true), doc.metadata, doc.length, doc.filename);
			});
		}
	});

	return this;
};

Agent.prototype.writeFile = function(id, file, name, meta, options, callback) {
	var self = this;

	if (typeof(options) === 'function') {
		callback = options;
		options = null;
	}

	if (typeof(meta) === 'function') {
		var tmp = callback;
		callback = meta;
		meta = tmp;
	}

	connect(self.connection, function(err, db) {

		if (err) {
			self.errors && self.errors.push(err);
			return callback(err);
		}

		var bucket = new database.GridFSBucket(db, options);
		var upload = bucket.openUploadStreamWithId(id, name, meta ? { metadata: meta } : undefined);
		var stream = typeof(file.pipe) === 'function' ? file : Fs.createReadStream(file);

		stream.pipe(upload).once('finish', function() {
			callback && callback(null);
			callback = null;
		}).once('error', function(err) {
			self.errors && self.errors.push(err);
			callback && callback(err);
			callback = null;
		});
	});

	return self;
};

Agent.prototype.writeStream = function(id, stream, name, meta, options, callback) {
	var self = this;

	if (!callback)
		callback = NOOP;

	if (typeof(options) === 'function') {
		callback = options;
		options = null;
	}

	if (typeof(meta) === 'function') {
		var tmp = callback;
		callback = meta;
		meta = tmp;
	}

	connect(self.connection, function(err, db) {

		if (err) {
			self.errors && self.errors.push(err);
			callback && callback(err);
			return;
		}

		var bucket = new database.GridFSBucket(db, options);
		var upload = bucket.openUploadStreamWithId(id, name, meta ? { metadata: meta } : undefined);

		upload.once('finish', function(err) {
			self.errors && self.errors.push(err);
			callback && callback(err);
			callback = null;
		});

		stream.pipe(upload);
	});

	return self;
};

Agent.prototype.writeBuffer = function(id, buffer, name, meta, options, callback) {
	var self = this;

	if (!callback)
		callback = NOOP;

	if (typeof(options) === 'function') {
		callback = options;
		options = null;
	}

	if (typeof(meta) === 'function') {
		var tmp = callback;
		callback = meta;
		meta = tmp;
	}

	connect(self.connection, function(err, db) {

		if (err) {
			self.errors && self.errors.push(err);
			callback && callback(err);
			return;
		}

		var bucket = new database.GridFSBucket(db, options);
		var upload = bucket.openUploadStreamWithId(id, name, meta ? { metadata: meta } : undefined);

		upload.end(buffer, function(err) {
			self.errors && self.errors.push(err);
			callback && callback(err);
			callback = null;
		});
	});

	return self;
};

Agent.init = function(conn, debug) {
	Agent.debug = debug ? true : false;

	F.wait('database');

	database.connect(conn, OPTIONS, function(err, db) {
		if (err)
			throw err;

		if (db.db) {
			// new mongodb
			if (conn[conn.length - 1] === '/')
				conn = conn.substring(0, conn.length - 1);
			db = db.db(conn.substring(conn.lastIndexOf('/') + 1));
		}

		CONNECTIONS[conn] = db;
		F.wait('database');
		EMIT('database', conn);
	});

	F.database = function(errorBuilder) {
		return new Agent(conn, errorBuilder);
	};

	return Agent;
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

	if (typeof(value) === 'string')
		value = value.split(',');

	var arr = [];

	if (!(value instanceof Array))
		return arr;

	for (var i = 0, length = value.length; i < length; i++) {
		var id = ObjectID.parse(value[i]);
		id && arr.push(id);
	}

	return arr;
};

function get(obj, path) {

	var cachekey = '=' + path;

	if (columns_cache[cachekey])
		return columns_cache[cachekey](obj);

	var arr = path.split('.');
	var builder = [];
	var p = '';

	for (var i = 0, length = arr.length - 1; i < length; i++) {
		var tmp = arr[i];
		var index = tmp.lastIndexOf('[');
		if (index !== -1)
			builder.push('if(!w.' + (p ? p + '.' : '') + tmp.substring(0, index) + ')return');
		p += (p !== '' ? '.' : '') + arr[i];
		builder.push('if(!w.' + p + ')return');
	}

	var fn = (new Function('w', builder.join(';') + ';return w.' + path.replace(REG_APO, '\'')));
	columns_cache[cachekey] = fn;
	return fn(obj);
}

function GridFSObject(id, meta, filename, length, type, bucket) {
	this.id = id;
	this.meta = meta;
	this.filename = filename;
	this.length = length;
	this.type = type;
	this.bucket = bucket;
}

GridFSObject.prototype.stream = function() {
	return this.bucket.openDownloadStream(this.id);
};