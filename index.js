if (!Array.prototype.wait) {
    Array.prototype.wait = function(onItem, callback) {

        var self = this;
        var item = self.shift();

        if (item === undefined) {
            if (callback)
                callback();
            return self;
        }

        onItem.call(self, item, function() {
            setImmediate(function() {
                self.wait(onItem, callback);
            });
        });

        return self;
    };
}