/**
 * A custom implementation of pg-large-object
 * @author Joris van der Wel <joris@jorisvanderwel.com>
 */

const NOOP = function(){};
const Stream = require('stream');
const BUFFERSIZE = 16384;
const WRITESTREAM = { highWaterMark: BUFFERSIZE, decodeStrings: true, objectMode: false };
const READSTREAM = { highWaterMark: BUFFERSIZE, encoding: null, objectMode: false };

var LargeObject = function(client, oid, fd) {
	this._client = client;
	this.oid = oid;
	this._fd = fd;
};

LargeObject.SEEK_SET = 0;
LargeObject.SEEK_CUR = 1;
LargeObject.SEEK_END = 2;

LargeObject.prototype.close = function(callback) {
	var self = this;
	self._client.query({ name: 'npg_lo_close', text: 'SELECT lo_close($1) as ok', values: [self._fd] }, callback);
	return self;
};

LargeObject.prototype.read = function(length, callback) {
	var self = this;
	self._client.query({ name: 'npg_loread', text:'SELECT loread($1, $2) as data', values: [self._fd, length] }, function(err, response) {
		if (err)
			return callback(err);
		var data = response.rows[0].data;
		callback(null, data);
	});
	return self;
};

LargeObject.prototype.write = function(buffer, callback) {
	var self = this;
	self._client.query({ name: 'npg_lowrite', text:'SELECT lowrite($1, $2)', values: [self._fd, buffer] }, callback);
	return self;
};

LargeObject.prototype.seek = function(position, ref, callback) {
	var self = this;
	self._client.query({ name: 'npg_lo_lseek64', text: 'SELECT lo_lseek' + self.plusql + '($1, $2, $3) as location', values: [self._fd, position, ref] }, function(err, response) {
		if (err)
			return callback(err);
		var location = response.rows[0].location;
		callback(null, location);
	});
	return self;
};

LargeObject.prototype.tell = function(callback) {
	var self = this;
	self._client.query({ name: 'npg_lo_tell64', text: 'SELECT lo_tell' + self.plusql + '($1) as location', values: [self._fd] }, function(err, response) {
		if (err)
			return callback(err);
		var location = response.rows[0].location;
		callback(null, location);
	});
	return self;
};

LargeObject.prototype.size = function(callback) {
	var self = this;
	self._client.query({ name: 'npg_size', text: 'SELECT lo_lseek' + self.plusql + '($1, location, 0), seek.size FROM (SELECT lo_lseek' + self.plusql + '($1, 0, 2) AS SIZE, tell.location FROM (SELECT lo_tell' + self.plusql + '($1) AS location) tell) seek', values: [self._fd] }, function(err, response) {
		if (err)
			return callback(err);
		var size = response.rows[0].size;
		callback(null, size);
	});
	return self;
};

LargeObject.prototype.truncate = function(length, callback) {
	var self = this;
	self._client.query({ name: 'npg_lo_truncate' + self.plusql, text:'SELECT lo_truncate' + self.plusql + '($1, $2)', values: [self._fd, length]}, callback);
	return self;
};

LargeObject.prototype.getReadableStream = function(bufferSize) {
	return new ReadStream(this, bufferSize);
};

LargeObject.prototype.getWritableStream = function(bufferSize) {
	return new WriteStream(this, bufferSize);
};

var LargeObjectManager = function(client) {
	this._client = client;
};

LargeObjectManager.WRITE = 0x00020000;
LargeObjectManager.READ = 0x00040000;
LargeObjectManager.READWRITE = 0x00020000 | 0x00040000;

LargeObjectManager.prototype.open = function(oid, mode, callback) {

	if (!oid)
		throw 'Illegal Argument';

	var self = this;
	self._client.query({ name: 'npg_lo_open', text:'SELECT lo_open($1, $2) AS fd, current_setting(\'server_version_num\') as version', values: [oid, mode]}, function(err, response) {
		if (err)
			return callback(err);
		var lo = new LargeObject(self._client, oid, response.rows[0].fd);
		lo.oldversion = response.rows[0].version < 90300;
		lo.plusql = lo.oldversion ? '' : '64';
		callback(null, lo);
	});

	return self;
};

LargeObjectManager.prototype.create = function(callback) {
	this._client.query({ name: 'npg_lo_creat', text:'SELECT lo_creat($1) AS oid', values: [LargeObjectManager.READWRITE]}, function(err, response) {
		if (err)
			return callback(err);
		var oid = response.rows[0].oid;
		callback(null, oid);
	});
	return this;
};

LargeObjectManager.prototype.unlink = function(oid, callback) {
	if (!oid)
		throw 'Illegal Argument';
	this._client.query({ name: 'npg_lo_unlink', text:'SELECT lo_unlink($1) as ok', values: [oid]}, callback);
	return this;
};


LargeObjectManager.prototype.readStream = function(oid, bufferSize, callback) {

	if (typeof(bufferSize) === 'function') {
		callback = bufferSize;
		bufferSize = undefined;
	}

	var self = this;

	self.open(oid, LargeObjectManager.READ, function(err, obj) {

		if (err)
			return callback(err);

		obj.size(function(err, size) {

			if (err)
				return callback(err);

			if (size === '0')
				return callback(new Error('Stream is empty.'), size, null);

			var stream = obj.getReadableStream(bufferSize);

			stream.on('error', function() {
				obj.close(NOOP);
			});

			stream.on('end', function() {
				obj.close(NOOP);
			});

			callback(null, size, stream);
		});
	});

	return self;
};

LargeObjectManager.prototype.writeStream = function(bufferSize, callback) {

	if (typeof(bufferSize) === 'function') {
		callback = bufferSize;
		bufferSize = undefined;
	}

	var self = this;
	self.create(function(err, oid) {
		if (err)
			return callback(err);

		self.open(oid, LargeObjectManager.WRITE, function(err, obj) {
			if (err)
				return callback(err);

			var stream = obj.getWritableStream(bufferSize);

			stream.on('error', function() {
				obj.close(NOOP);
			});

			stream.on('finish', function() {
				obj.close(NOOP);
			});

			callback(null, oid, stream);
		});

	});
	return self;
};


var WriteStream = function(largeObject, bufferSize) {

	if (bufferSize && bufferSize !== BUFFERSIZE)
		WRITESTREAM.bufferSize = bufferSize;
	else if (WRITESTREAM.bufferSize !== BUFFERSIZE)
		WRITESTREAM.bufferSize = BUFFERSIZE;

	Stream.Writable.call(this, WRITESTREAM);
	this._largeObject = largeObject;
};

WriteStream.prototype = Object.create(Stream.Writable.prototype);
WriteStream.prototype._write = function(chunk, encoding, callback) {
	if (!Buffer.isBuffer(chunk))
		throw 'Illegal Argument';
	this._largeObject.write(chunk, callback);
};

var ReadStream = function(largeObject, bufferSize) {
	if (bufferSize && bufferSize !== BUFFERSIZE)
		READSTREAM.bufferSize = bufferSize;
	else if (READSTREAM.bufferSize !== BUFFERSIZE)
		READSTREAM.bufferSize = BUFFERSIZE;
	Stream.Readable.call(this, READSTREAM);
	this._largeObject = largeObject;
};

ReadStream.prototype = Object.create(Stream.Readable.prototype);
ReadStream.prototype._read = function(length) {

	if (length <= 0)
		throw 'Illegal Argument';

	var self = this;
	self._largeObject.read(length, function(error, data) {

		if (error)
			return self.emit('error', error);

		self.push(data);

		if (data.length < length)
			self.push(null); // the large object has no more data left
	});
};

exports.create = function(client) {
	return new LargeObjectManager(client);
};