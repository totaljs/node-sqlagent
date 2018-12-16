Array.prototype.sqlagent = function(onItem, callback) {

	var self = this;
	var item = self.shift();

	if (!item) {
		callback();
		return self;
	}

	onItem.call(self, item, function(val) {
		if (val === false) {
			self.length = 0;
			callback();
		} else
			setImmediate(() => self.sqlagent(onItem, callback));
	});

	return self;
};