var emiter = require('events');
/*
 *   @class Realtime.Event Provides event capabilities to any object.
 *   @see Event\example.js
 */

/*
 *   @function {public void} extend Provides the specified object with event capabilities.
 *   @param {Object} target The object to provide event capabilities.
 */
this.extend = function(obj) {

    var provider = new emiter.EventEmitter();

    provider.setMaxListeners(0);

    obj.bind = function(events) {
        for (var type in events) {
            if (events[type]) {
                provider.on(type, events[type]);
            }
        }
    };

    obj.once = function(events) {
        for (var type in events) {
            provider.once(type, events[type].bind(this));
        }
    };

    obj.unbind = function(events) {
        for (var type in events) {
            provider.removeListener(type, events[type]);
        }
    };

    obj.unbindAll = function(type) {
        // must be tested because they count arguments name
        if(type) {
            provider.removeAllListeners(type);
        } else {
            provider.removeAllListeners();
        }
    };

    obj.fire = function(events) {

        for (var type in events) {

            process.nextTick(function(eventType, args) {

                var eventArgs = {};

                for (var key in args) {
                    eventArgs[key] = args[key];
                }

                provider.emit(type, eventArgs);

            }.bind(obj, type, events[type]));

        }
    };

    obj.listeners = function(eventType) {
        return provider.listeners(eventType);
    };
};

module.exports = {
    extend: this.extend
};