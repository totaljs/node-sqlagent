const database = require('mssql');
const Parser = require('url');
const queries = {};
const columns_cache = {};
const pools_cache = {};
const REG_SELECT = /select/i;
const REG_CUSTOM = /#\d+#/g;
const REG_WILDCARD = /\*/i;
const REG_APO = /'/g;
const REG_COLUMN = /^(!{1,}|\s)*/;
const REG_COLUMN_CAST = /\[|\]/g;
const REG_ARGUMENT = /\?/g;

require('./index');

function SqlBuilder(skip, take, agent) {
	this.agent = agent;
	this.builder = [];
	this._order = null;
	this._skip = skip >= 0 ? skip : 0;
	this._take = take >= 0 ? take : 0;
	this._set = null;
	this._define;
	this._fn;
	this._join;
	this._fields;
	this._schema;
	this._primary;
	this._group;
	this._having;
	this._is = false;
	this.hasOperator = false;
}

SqlBuilder.prototype = {
	get data() {
		return this._set;
	}
};

SqlBuilder.prototype.callback = function(fn) {
	this.$callback = fn;
	return this;
};

SqlBuilder.prototype.assign = function(name, key) {
	this.$assignname = name;
	this.$assignkey = key;
	return this;
};

SqlBuilder.prototype.replace = function(builder, reference) {
	var self = this;

	self.builder = reference ? builder.builder : builder.builder.slice(0);

	if (builder._order)
		self._order = reference ? builder._order : builder._order.slice(0);

	self._skip = builder._skip;
	self._take = builder._take;

	if (builder._set)
		self._set = reference ? builder._set : copy(builder._set);

	if (builder._fn)
		self._fn = reference ? builder._fn : copy(builder._fn);

	if (builder._join)
		self._join = reference ? builder._join : builder._join.slice(0);

	if (builder._fields)
		self._fields = builder._fields;

	if (builder._schema)
		self._schema = builder._schema;

	if (builder._primary)
		self._primary = builder._primary;

	self._is = builder._is;
	self.hasOperator = builder.hasOperator;
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
}

SqlBuilder.prototype.clone = function() {
	var builder = new SqlBuilder(0, 0, this.agent);
	return builder.replace(this);
};

SqlBuilder.prototype.join = function(name, on, type) {
	var self = this;
	if (!self._join)
		self._join = [];

	if (!type)
		type = 'left';

	self._join.push(type + ' join ' + name + ' on ' + on);
	return self;
};

SqlBuilder.prototype.schema = function(name) {
	this._schema = name;
	return this;
};

SqlBuilder.prototype.remove = SqlBuilder.prototype.rem = function(name) {
	if (this._set)
		this._set[name] = undefined;
	return this;
};

SqlBuilder.prototype.prepare = function(query) {
	if (!this._skip && this._take)
		return query.replace(REG_SELECT, 'SELECT TOP ' + this._take);
	return query;
};

SqlBuilder.prototype.define = function(name, type) {
	var self = this;
	if (!self._define)
		self._define = {};
	self._define[name] = type;
	return self;
};

SqlBuilder.prototype.set = function(name, value) {
	var self = this;
	if (!self._set)
		self._set = {};

	if (typeof(name) === 'string') {
		self._set[name] = value === '$' ? '#00#' : value;
		return self;
	}

	var keys = Object.keys(name);

	for (var i = 0, length = keys.length; i < length; i++) {
		var key = keys[i];
		var val = name[key];
		if (val !== undefined)
			self._set[key] = val === '$' ? '#00#' : val;
	}

	return self;
};

SqlBuilder.prototype.inc = function(name, type, value) {

	var self = this;
	var can = false;

	if (!self._set)
		self._set = {};

	if (value === undefined) {
		value = type;
		type = '+';
		can = true;
	}

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
			type = '+';
			if (value == null)
				value = 1;
		}

		if (!value)
			return self;

		name = type + name;
		self._set[name] = value === '$' ? '#00#' : value;
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

SqlBuilder.prototype.random = function() {
	var self = this;
	if (!self._order)
		self._order = [];
	self._order.push('NEWID()');
	return self;
};

SqlBuilder.prototype.order = function(name, desc) {

	var self = this;
	if (!self._order)
		self._order = [];

	var key = '<' + name + '.' + self._schema + '.' + (desc || 'false') + '>';
	if (columns_cache[key]) {
		self._order.push(columns_cache[key]);
		return self;
	}

	var lowered = name.toLowerCase();

	if (lowered.lastIndexOf(' desc') !== -1 || lowered.lastIndexOf(' asc') !== -1) {
		columns_cache[key] = SqlBuilder.column(name, self._schema);
		self._order.push(columns_cache[key]);
		return self;
	} else if (typeof(desc) === 'boolean')
		desc = desc === true ? 'DESC' : 'ASC';
	else
		desc = 'ASC';

	columns_cache[key] = SqlBuilder.column(name, self._schema) + ' ' + desc;
	self._order.push(columns_cache[key]);
	return self;
};

SqlBuilder.prototype.random = function() {
	var self = this;
	if (!self._order)
		self._order = [];
	self._order.push('RAND()');
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
		if (!self._fn)
			self._fn = {};
		var key = Math.floor(Math.random() * 1000000);
		self._fn[key] = value;
		value = '#' + key + '#';
		is = true;
	}

	self.checkOperator();
	self.builder.push(SqlBuilder.column(name, self._schema) + operator + (is ? value : SqlBuilder.escape(value)));
	self._is = true;
	return self;
};

SqlBuilder.prototype.checkOperator = function() {
	var self = this;
	!self.hasOperator && self.and();
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

SqlBuilder.prototype.fields = function() {
	var self = this;
	if (!self._fields)
		self._fields = '';

	if (arguments[0] instanceof Array) {
		var arr = arguments[0];
		for (var i = 0, length = arr.length; i < length; i++)
			self._fields += (self._fields ? ',' : '') + SqlBuilder.column(arr[i], self._schema);
	} else {
		for (var i = 0; i < arguments.length; i++)
			self._fields += (self._fields ? ',' : '') + SqlBuilder.column(arguments[i], self._schema);
	}

	return self;
};

SqlBuilder.prototype.field = function(name) {
	var self = this;
	if (!self._fields)
		self._fields = '';
	self._fields += (self._fields ? ',' : '') + SqlBuilder.column(name, self._schema);
	return self;
};

SqlBuilder.escape = SqlBuilder.prototype.escape = function(value) {

	if (value == null)
		return 'null';

	var type = typeof(value);

	if (type === 'function') {
		value = value();
		if (value == null)
			return 'null';
		type = typeof(value);
	}

	if (type === 'boolean')
		return value ? '1' : '0';

	if (type === 'number')
		return value.toString();

	if (type === 'string')
		return SqlBuilder.escaper(value);

	if (value instanceof Array)
		return SqlBuilder.escaper(value.join(','));

	if (value instanceof Date)
		return dateToString(value);

	return SqlBuilder.escaper(value.toString());
};

SqlBuilder.escaper = function(value) {
	return "'" + value.replace(REG_APO, '\'\'') + "'";
};

SqlBuilder.prototype.raw = function(name, value) {
	var self = this;
	if (!self._set)
		self._set = {};
	self._set['!' + name] = value;
	return self;
};

SqlBuilder.column = function(name, schema) {

	var cachekey = (schema ? schema + '.' : '') + name;
	var val = columns_cache[cachekey];
	if (val)
		return val;

	var raw = false;

	if (name[0] === '!') {
		raw = true;
		name = name.replace(REG_COLUMN, '');
	}

	var index = name.lastIndexOf('-->');
	var cast = '';

	if (index !== -1) {
		cast = name.substring(index).replace('-->', '').trim();
		name = name.substring(0, index).trim();
	}

	var indexAS = name.toLowerCase().indexOf(' as');
	var plus = '';

	if (indexAS !== -1) {
		plus = name.substring(indexAS);
		name = name.substring(0, indexAS);
	} else if (cast)
		plus = ' as [' + name + ']';

	var casting = function(value) {
		return cast ? 'CAST(' + value + cast + ')' : value;
	};

	if (cast) {
		switch (cast) {
			case 'integer':
			case 'int':
			case 'byte':
			case 'smallint':
			case 'number':
				cast = 'INT';
				break;
			case 'float':
			case 'real':
			case 'double':
			case 'decimal':
			case 'currency':
				cast = 'REAL';
				break;
			case 'boolean':
			case 'bool':
				cast = 'BIT';
				break;
		}
		cast = ' AS ' + cast;
	}

	if (raw)
		return columns_cache[cachekey] = casting(name) + plus;

	name = name.replace(REG_COLUMN_CAST, '');
	index = name.indexOf('.');

	if (index === -1)
		return columns_cache[cachekey] = casting((schema ? schema + '.' : '') + '[' + name + ']') + plus;
	return columns_cache[cachekey] = casting(name.substring(0, index) + '.[' + name.substring(index + 1) + ']') + plus;
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
		self._group = undefined;

	return self;
};

SqlBuilder.prototype.having = function(condition) {
	var self = this;

	if (condition)
		self._having = 'HAVING ' + condition;
	else
		self._having = undefined;

	return self;
};

SqlBuilder.prototype.and = function() {
	var self = this;
	if (!self.builder.length)
		return self;
	self.hasOperator = true;
	self.builder.push('AND');
	return self;
};

SqlBuilder.prototype.or = function() {
	var self = this;
	if (!self.builder.length)
		return self;
	self.hasOperator = true;
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
	if (!(value instanceof Array)) {
		self.where(name, value);
		return self;
	}
	self.checkOperator();
	var values = [];
	for (var i = 0, length = value.length; i < length; i++)
		values.push(SqlBuilder.escape(value[i]));
	self.builder.push(SqlBuilder.column(name, self._schema) + ' IN (' + values.join(',') + ')');
	self._is = true;
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

	self.builder.push(SqlBuilder.column(name, self._schema) + ' LIKE ' + search);
	self._is = true;
	return self;
};

SqlBuilder.prototype.between = function(name, valueA, valueB) {
	var self = this;
	self.checkOperator();
	self.builder.push(SqlBuilder.column(name, self._schema) + ' BETWEEN ' + SqlBuilder.escape(valueA) + ' AND ' + SqlBuilder.escape(valueB));
	self._is = true;
	return self;
};

SqlBuilder.prototype.query = SqlBuilder.prototype.sql = function(sql) {
	var self = this;
	self.checkOperator();

	if (arguments.length > 1) {
		var indexer = 1;
		var argv = arguments;
		sql = sql.replace(REG_ARGUMENT, () => SqlBuilder.escape(argv[indexer++]));
	}

	self.builder.push(sql);
	self._is = true;
	return self;
};

SqlBuilder.prototype.toString = function(id, isCounter) {

	var self = this;
	var plus = '';
	var order = '';
	var join = '';

	if (self._join)
		join = self._join.join(' ') + ' ';

	if (!isCounter) {
		if (self._order)
			order = ' ORDER BY ' + self._order.join(',');
		if (self._skip && self._take)
			plus = ' OFFSET ' + self._skip + ' ROWS FETCH NEXT ' + self._take + ' ROWS ONLY';
		else if (self._take)
			plus = ' OFFSET 0 ROWS FETCH NEXT ' + self._take + ' ROWS ONLY';
		else if (self._skip)
			plus = ' OFFSET ' + self._skip + ' ROWS';
		if (!self._order && plus.length)
			order = ' ORDER BY 1';
	}

	if (!self.builder.length)
		return (join ? ' ' + join : '') + (self._group ? ' ' + self._group : '') + (self._having ? ' ' + self._having : '') + order + plus;

	var where = self.builder.join(' ');

	if (id === undefined)
		id = null;

	if (self._fn)
		where = where.replace(REG_CUSTOM, text => text === '#00#' ? SqlBuilder.escape(id) : SqlBuilder.escape(self._fn[parseInt(text.substring(1, text.length - 1))]));

	return (join ? ' ' + join : '') + (self._is ? ' WHERE ' : ' ') + where + (self._group ? ' ' + self._group : '') + (self._having ? ' ' + self._having : '') + order + plus;
};

SqlBuilder.prototype.make = function(fn) {
	var self = this;
	fn.call(self, self);
	return self.agent || self;
};

SqlBuilder.prototype.toQuery = function(query) {
	var self = this;
	return self._fields ? query.replace(REG_WILDCARD, self._fields) : query;
};

function Agent(options, error, id) {
	this.$conn = id === undefined ? JSON.stringify(options) : id;
	this.isErrorBuilder = global.ErrorBuilder ? true : false;
	this.errors = this.isErrorBuilder ? error : null;
	this.options = options;
	this.db = null;
	this.clear();
	this.$events = {};

	// Hidden:
	// this.time;
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

Agent.connect = function(conn, callback) {
	callback && callback(null);
	var id = (Math.random() * 1000000) >> 0;
	return function(error) {
		return new Agent(conn, error, id);
	};
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

// Debug mode (output to console)
Agent.debug = false;

Agent.prototype.clear = function() {

	this.command = [];
	this.done = null;
	this.last = null;
	this.id = null;
	this.$id = null;
	this.isCanceled = false;
	this.index = 0;
	this.isPut = false;
	this.skipCount = 0;
	this.skips = {};
	this.$transaction;
	this.$fast = false;
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

Agent.query = function(name, query) {
	queries[name] = query;
	return Agent;
};

Agent.prototype.nolock = function(enable) {
	if (enable === undefined)
		this.$fast = true;
	else
		this.$fast = false;
	return this;
};

Agent.prototype.primaryKey = Agent.prototype.primary = function() {
	// compatibility with PG
	return this;
};

Agent.prototype.skip = function(name) {
	var self = this;
	if (name)
		self.skips[name] = true;
	else
		self.skipCount++;
	return self;
};

Agent.prototype.prepare = function(fn) {
	var self = this;
	self.command.push({ type: 'prepare', fn: fn });
	return self;
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
	self.command.push({ type: 'put', value: value, disable: value == null });
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
	self.builders[name] = params;
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

Agent.prototype._insert = function(item) {

	var values = item.condition._set;
	var isPrepare = item.condition._define;

	var keys = Object.keys(values);
	var columns = [];
	var columns_values = [];
	var params = [];

	for (var i = 0, length = keys.length; i < length; i++) {
		var key = keys[i];
		var value = values[key];

		var isRAW = key[0] === '!';
		if (isRAW)
			key = key.substring(1);

		if (key[0] === '$' || value === undefined)
			continue;

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

		columns.push('[' + key + ']');

		if (isRAW) {
			columns_values.push(value);
			continue;
		}

		columns_values.push('@' + key);

		var type = typeof(value);
		var isFN = false;

		if (type === 'function')
			value = value();

		if (type === 'string')
			value = value.trim();

		if (type === 'function')
			isFN = true;

		if (type === 'object') {
			if (Buffer.isBuffer(value))
				type = 'varbinary';
			else if (typeof(value.getTime) === 'function')
				type = 'datetime';
		}

		if (isPrepare && item.condition._define[key])
			type = item.condition._define[key];

		params.push({ name: key, type: type, value: value === undefined ? null : value, isFN: isFN });
	}

	item.$query = 'INSERT INTO ' + item.table + ' (' + columns.join(',') + ') VALUES(' + columns_values.join(',') + '); SELECT @@identity AS [identity]';
	item.$params = params;
	item.first = true;
	return item;
};

Agent.prototype._update = function(item) {

	var values = item.condition._set;
	var keys = Object.keys(values);

	var columns = [];
	var params = [];

	for (var i = 0, length = keys.length; i < length; i++) {
		var key = keys[i];
		var value = values[key];

		var isRAW = key[0] === '!';
		if (isRAW)
			key = key.substring(1);

		if (key[0] === '$' || value === undefined)
			continue;

		var type = typeof(value);
		if (type === 'function')
			value = value();

		if (type === 'string')
			value = value.trim();

		switch (key[0]) {
			case '+':
				key = key.substring(1);
				columns.push('[' + key + ']=ISNULL([' + key + '],0)+@' + key);
				if (!value)
					value = 1;
				break;
			case '-':
				key = key.substring(1);
				columns.push('[' + key + ']=ISNULL([' + key + '],0)-@' + key);
				if (!value)
					value = 1;
				break;
			case '*':
				key = key.substring(1);
				columns.push('[' + key + ']=ISNULL([' + key + '],0)*@' + key);
				if (!value)
					value = 1;
				break;
			case '/':
				key = key.substring(1);
				columns.push('[' + key + ']=ISNULL([' + key + '],0)/@' + key);
				if (!value)
					value = 1;
				break;
			default:
				if (isRAW)
					columns.push('[' + key + ']=' + value);
				else
					columns.push('[' + key + ']=@' + key);
				break;
		}

		!isRAW && params.push({ name: key, type: type, value: value === undefined ? null : value });
	}

	item.$query = 'UPDATE ' + item.table + ' SET ' + columns.join(',') + item.condition.toString(this.id) + '; SELECT @@rowcount As affectedRows';
	item.$params = params;
	item.column = 'affectedRows';
	item.first = true;
	return item;
};

Agent.prototype._query = function(item) {
	if (item.condition instanceof SqlBuilder) {
		item.$query = (item.scalar ? item.query : item.condition.toQuery(item.query)) + item.condition.toString(this.id, item.scalar);
		return item;
	}
	item.$query = item.query;
	item.$params = item.condition;
	return item;
};

Agent.prototype._select = function(item) {
	item.query = 'SELECT * FROM ' + item.table;
	item.$query = item.condition.toQuery(item.query) + item.condition.toString(this.id);
	item.first = item.condition._take === 1;
	return item;
};

Agent.prototype._compare = function(item) {
	var keys = item.keys ? item.keys : item.condition._fields ? item.condition._fields.split(',') : Object.keys(item.value);
	!item.condition._fields && item.condition.fields.apply(item.condition, keys);
	item.query = 'SELECT * FROM ' + item.table;
	item.$query = item.condition.toQuery(item.query) + item.condition.toString(this.id);
	item.first = item.condition._take === 1;
	return item;
};

Agent.prototype._delete = function(item) {
	item.$query = item.query + item.condition.toString(this.id) + '; SELECT @@rowcount As affectedRows';
	item.column = 'affectedRows';
	item.first = true;
	return item;
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

Agent.prototype.insert = function(name, table) {

	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'insert', table: table, name: name, condition: condition });
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
	self.command.push({ type: 'select', name: name, table: table, condition: condition });
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
	self.command.push({ type: 'compare', name: name, table: table, condition: condition, value: obj, keys: keys });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.listing = function(name, table, column) {

	var self = this;
	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var key ='$listing_' + name;
	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'query', query: 'SELECT COUNT(' + (column || '*') + ') as sqlagentcolumn FROM ' + table, name: key + '_count', condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1, scalar: true, nocallback: true });
	self.command.push({ type: 'select', name: key + '_items', table: table, condition: condition, listing: key, target: name });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.find = Agent.prototype.builder = function(name) {
	return this.builders[name];
};

Agent.prototype.exists = function(name, table) {
	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	condition.first();
	self.command.push({ type: 'query', query: 'SELECT 1 as sqlagentcolumn_e FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn_e', scalar: true });
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
	self.command.push({ type: 'query', query: 'SELECT COUNT(' + (column || '*') + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1, scalar: true });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.max = function(name, table, column) {
	var self = this;
	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'query', query: 'SELECT MAX(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1, scalar: true });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.min = function(name, table, column) {
	var self = this;
	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'query', query: 'SELECT MIN(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1, scalar: true });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.avg = function(name, table, column) {
	var self = this;
	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'query', query: 'SELECT AVG(' + column + ') as sqlagentcolumn FROM ' + table, name: name, condition: condition, first: true, column: 'sqlagentcolumn', datatype: 1, scalar: true });
	self.builders[name] = condition;
	return condition;
};

Agent.prototype.update = function(name, table) {

	var self = this;

	if (typeof(table) !== 'string') {
		table = name;
		name = self.index++;
	}

	var condition = new SqlBuilder(0, 0, self);
	self.command.push({ type: 'update', table: table, name: name, condition: condition });
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
	self.command.push({ type: 'delete', query: 'DELETE FROM ' + table, name: name, condition: condition });
	self.builders[name] = condition;
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
		delete self.builders[name];
		return true;
	}
	return false;
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

Agent.prototype.close = function() {
	var self = this;
	self.done && self.done();
	self.done = null;
	return self;
};

Agent.prototype.rollback = function(where, e, next) {
	var self = this;

	self.errors && self.errors.push(e);
	self.command.length = 0;

	if (!self.isTransaction)
		return next();

	self.isRollback = true;
	self.end();
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
				var tmp = item.fn(self.errors, self.results, function(output) {
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

				var type = typeof(tmp);
				if (type !== 'boolean' && type !== 'string')
					return;
				if (tmp === true || tmp === undefined)
					return next();
				// reason
				if (typeof(tmp) === 'string')
					self.errors.push(tmp);
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
				return;
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
				self.$id = typeof(item.value) === 'function' ? item.value() : item.value;
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

		switch (item.type) {
			case 'select':
				self._select(item);
				break;
			case 'update':
				self._update(item);
				break;
			case 'insert':
				self._insert(item);
				break;
			case 'delete':
				self._delete(item);
				break;
			case 'compare':
				self._compare(item);
				break;
			default:
				self._query(item);
				break;
		}

		if (item.type !== 'begin' && item.type !== 'end') {

			if (!item.first)
				item.first = isFIRST(item.$query);

			(Agent.debug || self.debug) && console.log(self.debugname, item.name, item.$query);
			self.$events.query && self.emit('query', item.name, item.$query, item.$params);

			var request = new database.Request(self.$transaction ? self.$transaction : self.db);
			item.$params && prepare_params_request(request, item.$params);

			request.query(item.$query, function(err, rows) {
				self.$bind(item, err, rows ? rows.recordset : []);
				next();
			});
			return;
		}

		if (item.type === 'begin') {
			(Agent.debug || self.debug) && console.log(self.debugname, 'begin transaction');
			self.$transaction = new database.Transaction(self.db);
			self.$transaction.begin(function(err) {
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
				(Agent.debug || self.debug) && console.log(self.debugname, 'rollback transaction');
				self.$transaction.rollback(function(err) {
					self.$transaction = null;
					if (!err)
						return next();
					self.command.length = 0;
					self.push(err.message);
					self.$transaction = null;
					next(false);
				});
				return;
			}

			(Agent.debug || self.debug) && console.log(self.debugname, 'commit transaction');
			self.$transaction.commit(function(err) {

				if (!err) {
					self.$transaction = null;
					return next();
				}

				self.errors.push(err.message);
				self.command.length = 0;
				self.$transaction.rollback(function(err) {
					self.$transaction = null;
					if (!err)
						return next();
					self.errors.push(err.message);
					next();
				});
			});
			return;
		}

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

Agent.prototype.$bindwhen = function(name) {
	var self = this;
	if (!self.$when)
		return self;
	var tmp = self.$when[name];
	if (!tmp)
		return self;
	for (var i = 0, length = tmp.length; i < length; i++)
		tmp[i](self.errors, self.results, self.results[name]);
	return self;
};

Agent.prototype.$bind = function(item, err, rows) {

	var self = this;
	var obj;

	if (err) {
		item.condition && item.condition.$callback && item.condition.$callback(err);
		self.errors.push(item.name + ': ' + err.message);
		if (self.isTransaction)
			self.isRollback = true;
		self.last = item.name;
		return;
	}

	if (!rows || !rows.length) {
		if (item.type === 'insert') {
			self.id = null;
			if (!self.isPut)
				self.$id = self.id;
		} else if (!item.first)
			self.results[item.name] = [];

		if (item.listing) {
			obj = {};
			obj.count = self.results[item.listing + '_count'];
			obj.items = self.results[item.listing + '_items'];
			obj.page = 1;
			obj.pages = 0;
			obj.limit = item.condition._take;
			self.results[item.target] = obj;
			self.results[item.listing + '_count'] = null;
			self.results[item.listing + '_items'] = null;
			item.condition && item.condition.$callback && item.condition.$callback(null, obj);
			item.condition.$assignname && self.results[item.condition.$assignname] && (self.results[item.condition.$assignname][item.condition.$assignkey] = obj);
		} else
			item.condition && !item.nocallback && item.condition.$callback && item.condition.$callback(null, self.results[item.name]);
		self.$events.data && self.emit('data', item.target || item.name, self.results);
		self.last = item.name;
		self.$bindwhen(item.name);
		return;
	}

	if (item.type === 'insert') {
		self.id = rows.length ? rows[0].identity : null;
		if (!self.isPut)
			self.$id = self.id;
	}

	if (item.first && item.column) {
		if (rows.length)
			self.results[item.name] = item.column === 'sqlagentcolumn_e' ? true : item.datatype === 1 ? item.condition && item.condition._group ? rows.length : parseFloat(rows[0][item.column] || 0) : rows[0][item.column];
	} else if (item.first)
		self.results[item.name] = rows instanceof Array ? rows[0] : rows;
	else
		self.results[item.name] = rows;

	if (item.listing) {
		obj = {};
		obj.count = self.results[item.listing + '_count'];
		obj.items = self.results[item.listing + '_items'];
		obj.page = ((item.condition._skip || 0) / (item.condition._take || 0)) + 1;
		obj.limit = item.condition._take || 0;
		obj.pages = Math.ceil(obj.count / obj.limit);
		self.results[item.target] = obj;
		self.results[item.listing + '_count'] = null;
		self.results[item.listing + '_items'] = null;
		item.condition && item.condition.$callback && item.condition.$callback(null, obj);
	} else if (item.type === 'compare') {

		var keys = item.keys;
		var val = self.results[item.name];
		var diff;

		if (val) {
			diff = [];
			for (var i = 0, length = keys.length; i < length; i++) {
				var key = keys[i];
				var a = val[key];
				var b = item.value[key];
				if (a != b)
					diff.push(key);
			}
		} else
			diff = keys;

		self.results[item.name] = diff.length ? { diff: diff, record: val, value: item.value } : false;
	}

	!item.listing && item.condition && !item.nocallback && item.condition.$callback && item.condition.$callback(null, self.results[item.name]);
	item.condition && item.condition.$assignname && self.results[item.condition.$assignname] && (self.results[item.condition.$assignname][item.condition.$assignkey] = obj);
	self.$events.data && self.emit('data', item.target || item.name, self.results);
	self.last = item.name;
	self.$bindwhen(item.name);
};

Agent.prototype.exec = function(callback, returnIndex) {

	var self = this;

	if (Agent.debug || self.debug) {
		self.debugname = 'sqlagent/sqlserver (' + Math.floor(Math.random() * 1000) + ')';
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

	if (!pools_cache[self.$conn]) {
		if (typeof(self.options) === 'string') {
			var options = Parser.parse(self.options);
			self.options = {};
			self.options.server = options.host.replace(/_/g, '\\').split(':')[0];
			if (options.pathname && options.pathname.length > 1)
				self.options.database = options.pathname.substring(1);
			if (options.port)
				self.options.port = +options.port;
			var auth = options.auth;
			if (auth) {
				auth = auth.split(':');
				self.options.user = auth[0];
				self.options.password = auth[1];
			}
			pools_cache[self.$conn] = self.options;
		} else
			pools_cache[self.$conn] = self.options;
	} else
		self.options = pools_cache[self.$conn];

	//self.db = new database.connect(self.options, function(err) {
	self.db = new database.ConnectionPool(self.options, function(err) {
		if (err) {
			if (!self.errors)
				self.errors = self.isErrorBuilder ? new global.ErrorBuilder() : [];
			self.errors.push(err);
			callback && callback.call(self, self.errors, {});
			return;
		}
		self._prepare(callback);
	});

	return self;
};

Agent.destroy = function() {
	var keys = Object.keys(pools_cache);
	for (var i = 0, length = keys.length; i < length; i++)
		pools_cache[keys[i]].end(function(){});
};

Agent.prototype.done = function() {
	this.db && this.db.close();
	return this;
};

Agent.prototype.$$exec = function(returnIndex) {
	var self = this;
	return function(callback) {
		return self.exec(callback, returnIndex);
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

function prepare_params_request(request, params) {

	if (!params)
		return;

	for (var i = 0, length = params.length; i < length; i++) {
		var param = params[i];
		var type = param.type.toLowerCase();
		var value = param.value;

		if (param.isFN) {
			value = value(params);
			type = typeof(value);
		}

		switch (type) {
			case 'number':
				request.input(param.name, value % 1 === 0 ? database.Int : database.Float, value);
				break;
			case 'decimal':
				request.input(param.name, database.Decimal, value);
				break;
			case 'uniqueidentifier':
			case 'guid':
				request.input(param.name, database.UniqueIdentifier, value);
				break;
			case 'money':
				request.input(param.name, database.Money, value);
				break;
			case 'float':
				request.input(param.name, database.Float, value);
				break;
			case 'bigint':
				request.input(param.name, database.BigInt, value);
				break;
			case 'smallint':
			case 'byte':
				request.input(param.name, database.SmallInt, value);
				break;
			case 'string':
			case 'nvarchar':
				request.input(param.name, database.NVarChar, value);
				break;
			case 'boolean':
			case 'bit':
				request.input(param.name, database.Bit, value);
				break;
			case 'datetime':
				request.input(param.name, database.DateTime, value);
				break;
			case 'smalldatetime':
				request.input(param.name, database.SmallDateTime, value);
				break;
			case 'binary':
				request.input(param.name, database.Binary, value);
				break;
			case 'image':
				request.input(param.name, database.Image, value);
				break;
			case 'varbinary':
				request.input(param.name, database.VarBinary, value);
				break;
			case 'varchar':
				request.input(param.name, database.VarChar, value);
				break;
			case 'text':
				request.input(param.name, database.Text, value);
				break;
			case 'ntext':
				request.input(param.name, database.NText, value);
				break;
		}
	}
}

function isFIRST(query) {
	return query ? query.substring(0, 13).toLowerCase() === 'select top 1' : false;
}

Agent.init = function(conn, debug) {
	Agent.debug = debug ? true : false;
	var id = (Math.random() * 100000) >> 0;
	framework.database = function(errorBuilder) {
		return new Agent(conn, errorBuilder, id);
	};
	EMIT('database');
};

Agent.escape = Agent.prototype.escape = SqlBuilder.escape = SqlBuilder.prototype.escape = function(value) {

	if (value == null)
		return 'null';

	var type = typeof(value);

	if (type === 'function') {
		value = value();

		if (value == null)
			return 'null';

		type = typeof(value);
	}

	if (type === 'boolean')
		return value === true ? '1' : '0';

	if (type === 'number')
		return value.toString();

	if (type === 'string')
		return SqlBuilder.escaper(value);

	if (value instanceof Array)
		return SqlBuilder.escaper(value.join(','));

	if (value instanceof Date)
		return dateToString(value);

	return SqlBuilder.escaper(value.toString());
};

module.exports = Agent;
global.SqlBuilder = SqlBuilder;
