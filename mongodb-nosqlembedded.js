const ERR = ' is not supported with MongoDB';

function extend(Database, DatabaseBuilder, DatabaseBuilder2) {

	// ====================================
	// DatabaseBuilder
	// ====================================
	DatabaseBuilder.prototype.join = function() {
		throw Error('DatabaseBuilder.join()' + ERR);
	};

	// ====================================
	// DatabaseBuilder2
	// ====================================

	DatabaseBuilder2.prototype.callback = function() {
	};

	// ====================================
	// Database
	// ====================================

	Database.prototype.get = function(name) {
	};

	Database.prototype.set = function(name, value) {
	};

	Database.prototype.meta = function(name, value) {
	};

	Database.prototype.backups = function(filter, callback) {
	};

	Database.prototype.insert = function(doc, unique) {
	};

	Database.prototype.update = function(doc, insert) {
	};

	Database.prototype.modify = function(doc, insert) {
	};

	Database.prototype.restore = function(filename, callback) {
		throw Error('Database.restore()' + ERR);
	};

	Database.prototype.backup = function(filename, callback) {
		throw Error('Database.backup()' + ERR);
	};

	Database.prototype.drop = function() {
		return this;
	};

	Database.prototype.free = function() {
		return this;
	};

	Database.prototype.release = function() {
		return this;
	};

	Database.prototype.clear = Database.prototype.remove = function() {
	};

	Database.prototype.find = function(view) {
	};

	Database.prototype.count = function(view) {
	};

	Database.prototype.one = function(view) {
	};

	Database.prototype.top = function(max, view) {
	};

	Database.prototype.view = function(name) {
	};

	Database.prototype.lock = function() {
	};

	Database.prototype.unlock = function() {
	};

	Database.prototype.next = function() {
	};

	Database.prototype.refresh = function() {
	};

	Database.prototype.$meta = function(write) {
	};

	Database.prototype.$reader = NOOP;
	Database.prototype.$reader2 = NOOP;
	Database.prototype.$readerview = NOOP;
	Database.prototype.$reader2_inmemory = NOOP;
	Database.prototype.$views = NOOP;
	Database.prototype.$views_inmemory = NOOP;
	Database.prototype.$remove = NOOP;
	Database.prototype.$remove_inmemory = NOOP;
	Database.prototype.$drop = NOOP;
}

F.prototypes(function(proto) {
	extend(proto.Database, proto.DatabaseBuilder, proto.DatabaseBuilder2);
});