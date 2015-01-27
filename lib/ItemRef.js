var Event = require("./Event.js");
var ItemSnapshot = require("./ItemSnapshot.js");

/**
 *   @class Realtime.Storage.ItemRef Class with the definition of an item reference.
 */
var ItemRef = function(args) {	
    var self = this;
    var table = args.table;
    var connection = args.connection;
    var operationManager = args.operationManager;
    var key = args.key;
    
    var channel = "rtcs_" + table.name() + ":" + key.primary + (typeof key.secondary != "undefined" ? ( "_" + key.secondary) : "");
    var events = {
    	put: channel + ":put",
    	update: channel + ":update",
    	"delete": channel + ":delete"
    };
    
    /* Notification engine */
    
    var _eventEmitter = {};
    Event.extend(_eventEmitter);

    var subscribe = args.privateKey ? function(type, once, handler) {
        var evt = {};
        evt[events[type]] = handler;
        _eventEmitter[once ? "once" : "bind"](evt);
    	
    } : function (type, once, handler) {
        if (!connection.channels[channel]) {
            connection.isConnected() ? connection.subscribe({ name: channel }) : connection.createChannel({ name: channel });
            // always a bind because type of notification is not part of the channel
            connection.channels[channel].bind({
                message: function(e) {
                    var message = JSON.parse(e.message);
                    // check if there are listeners for this type of notification
                    if(_eventEmitter.listeners(events[message.type]).length != 0) {
                        table.meta(function(myMeta) {
                            var evt = {};
                            evt[events[message.type]] = new ItemSnapshot({
                                key: {
                                	primary: message[myMeta.key.primary.name],
                                	secondary: myMeta.key.secondary ? message[myMeta.key.secondary.name] : undefined
                                },
                                table: table,
                                connection: connection,
                                value: message.data,
                                operationManager: operationManager
                            });
                            _eventEmitter.fire(evt);
                        });
                    }
                }
            });
        }

        var evt = {};
        evt[events[type]] = handler;
        _eventEmitter[once ? "once" : "bind"](evt);
    };
    
    var unsubscribe = args.privateKey ? function(type, handler) {
    	// remove all listeners regardless of notification type (put, delete, update)
        if (!type) {
            _eventEmitter.unbindAll();
        } 
        // remove all listeners of specified notification type
        else {                   
            if (typeof handler != "function") {
                _eventEmitter.unbindAll(events[type]);
            } 
            else {
                var evt = {};
                evt[events[type]] = handler;
                _eventEmitter.unbind(evt);
            }
        }   	
    } : function (type, handler) {  
    	// remove all listeners regardless of notification type (put, delete, update)
        if (!type) {
            _eventEmitter.unbindAll();
            connection.unsubscribe(channel);
        } 
        // remove all listeners of specified notification type
        else {                   
            if (typeof handler != "function") {
                _eventEmitter.unbindAll(events[type]);
            } 
            else {
                var evt = {};
                evt[events[type]] = handler;
                _eventEmitter.unbind(evt);
            }
            
        	if(connection.channels[channel] && 
        			_eventEmitter.listeners(events.put).length == 0 && 
        			_eventEmitter.listeners(events.update).length == 0 && 
        			_eventEmitter.listeners(events["delete"]).length == 0) {
        		connection.unsubscribe(channel);
        	}
        }
    };           
               
    /* end of Notification engine */

    /* Validations */
    
    function validateKey(metaKey) {
        if (typeof key.primary != metaKey.primary.dataType) {
        	return "The primary key specified does not match the table schema.";
        }

        if (metaKey.secondary) {
            if (typeof key.secondary == "undefined") {
            	return "Missing secondary key. Must be specified when creating an ItemRef.";
            }

            if (typeof key.secondary != metaKey.secondary.dataType) {
            	return "The secondary key specified does not match the table schema.";
            }
        }
    };
    
    function validateAttach(eventType, callback) {
        if(operationManager.privateKey && !operationManager.authenticationToken) {
        	throw new Error("Notifications are disabled when private key is being used.");
        }
    	
        if (!eventType) {
        	return "Must specify an event type";
        }

        if (eventType != "put" && eventType != "delete" && eventType != "update") {
        	return "Invalid event type. Possible values: put, delete and update";
        }

        if (typeof callback != "function") {
        	return "Must specify an event handler";
        }
    };
    
    /* end of Validations */
    
    function snapshot(data) {
    	return new ItemSnapshot({
            key: key,
            table: table,
            connection: connection,
            value: data,
            operationManager: operationManager
        });
    }; 
    
    /**
     *   @function {ItemRef} get Get the value of this item reference.
     *   @param {optional function(ItemSnapshot itemSnapshot)} success Response from the server when the request was completed successfully.
     *   @...{ItemSnapshot} itemSnapshot An immutable copy of the retrieved item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see ItemRef\get.js
     */
    this.get = function(success, error) {
        table.meta(function(meta) {
            var err = validateKey(meta.key);
            if(err) { 
            	if(error) error(err);
                return self; 
        	}

            operationManager.getItem({
                table: table.name(),
                key: key
            }, function(data) {
                if (success) {
                    success(Object.keys(data) != 0 ? snapshot(data) : null);
                }

            }, error);
        });

        return this;
    };
    
    /**
     *   @function {ItemRef} set Updates the value of this item reference.
     *   @param {Object} items The object with the properties to set or update.
     *   @param {optional function(ItemSnapshot itemSnapshot)} success Response from the server when the request was completed successfully.
     *   @...{ItemSnapshot} itemSnapshot An immutable copy of the inserted item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see ItemRef\set.js
     */
    this.set = function(item, success, error) {
        table.meta(function(meta) {
            var err = validateKey(meta.key);
            if(err) { 
            	if(error) error(err);
                return self;
        	}

            if (Object.keys(item).length == 0) {
            	if(error) error("No properties to update");
                return self;
            }

            operationManager.updateItem({
                table: table.name(),
                key: key,
                item: item
            }, function(data) {
                if (success) {
                    success(snapshot(data));
                }
            }, error);

        });
        
        return this;
    };
    
    /**
     *   @function {ItemRef} del Delete the value of this item reference.
     *   @param {optional String[]} properties The name of the properties to delete.
     *   @param {optional function(ItemSnapshot itemSnapshot)} success Response from the server when the request was completed successfully.
     *   @...{ItemSnapshot} itemSnapshot An immutable copy of the deleted item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see ItemRef\del.js
     */
    this.del = function() {
        var properties;
        var success;
        var error;

        // process and validate arguments
        switch(arguments.length) {
            case 3:
                properties = arguments[0];
                success = arguments[1];
                error = arguments[2];
        	break;
            case 2:
                if (typeof arguments[0] == "function" && typeof arguments[1] == "function") {
                    success = arguments[0];
                    error = arguments[1];
                } 
                else { 
                	if (typeof arguments[1] == "function") {
                        properties = arguments[0];
                        success = arguments[1];
                    }	
                }
        	break;
            case 1:                    
            	if (typeof arguments[0] != "function") {
            		properties = arguments[0];
                } 
            	else {
                    success = arguments[0];
                }
        	break;
        }

        table.meta(function(meta) {
            var err = validateKey(meta.key);
            if(err) { 
            	if(error) error(err);
                return this;
        	}

            if (Array.isArray(properties) && properties.length > 0) {
                for (var i = 0; i < properties.length; i++) {
                    if (typeof properties[i] != "string") {
                        if (error) {
                            error("Invalid property names. Must be strings");
                        }
                        return self;
                    }
                }
            }

            operationManager.deleteItem({
                table: table.name(),
                key: key,
                properties : properties
            }, function(data) {
                if (success) {
                    success(snapshot(data));
                }
            }, error);
        });

        return this;
    };
    
    /**
     *   @function {ItemRef} on Attach a listener to run every time the eventType occurs.
     *   @param {String} eventType The type of the event to listen. Possible values: put, update, delete.
     *   @param {function(ItemSnapshot itemSnapshot)} handler The function to run whenever the notification is received. If the event type is "put", it will immediately trigger a "getItems" to get the initial data and run the callback with each item snapshot as argument.
     *   @... {ItemSnapshot} itemSnapshot An immutable copy of the modified item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see ItemRef\on.js
     */
    this.on = function(eventType, callback, error) {
    	var err = validateAttach(eventType, callback);
        if(err) { 
        	if(error) error(err);
            return this;
    	}
        
        subscribe(eventType, false, callback);

        if (eventType == "put") {
            this.get(callback, error);
        }
        
        return this;
    };
    
    /**
     *   @function {ItemRef} off Remove an event handler.
     *   @param {optional String} eventType The type of the event to remove. Possible values: put, update, delete. If not specified, it will remove all listeners of this reference.
     *   @param {optional function(ItemSnapshot itemSnapshot)} handler A previously attached listener. If not specified, it will remove all listeners of the specified type or all listeners of this reference.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see ItemRef\off.js
     */
    this.off = function(eventType, callback, error) {
        if (eventType) {
            if (eventType != "put" && eventType != "delete" && eventType != "update") {
                if (error) {
                    error("Invalid event type. Possible values: put, delete and update");
                }
                return this;
            }
        }
        
        unsubscribe(eventType, callback);
        return this;
    };
    
    /**
     *   @function {ItemRef} once Attach a listener to run only once the event type occurs.
     *   @param {String} eventType The type of the event to listen. Possible values: put, update, delete.
     *   @param {function(ItemSnapshot itemSnapshot)} handler The function invoked, only once, when the notification is received. If the event type is "put", it will immediately trigger a "getItems" to get the initial data and run the callback with each item snapshot as argument.
     *   @... {ItemSnapshot} itemSnapshot An immutable copy of the modified item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see ItemRef\once.js
     */
    this.once = function(eventType, callback, error) {
    	var err = validateAttach(eventType, callback);
        if(err) { 
        	if(error) error(err);
            return this;
    	}
        
        subscribe(eventType, true, callback);
        
        if (eventType == "put") {
            this.get(callback, error);
        }   
        
        return this;
    };
    
    function counter() {
    	var op = arguments[0];
    	var property = arguments[1];
    	if(!property) {
        	if(error) error("The parameter 'property' is mandatory.");
            return;            		
    	}
    	
    	var value = typeof arguments[2] == "number" ? arguments[2] : 1;
    	
    	var argLength = arguments.length;
    	var success, error; 
    	if(typeof arguments[argLength - 1] == "function" && typeof arguments[argLength - 2] == "function") {
    		success = arguments[argLength - 2];
    		error = arguments[argLength - 1];
    	}
    	else {
    		if(typeof arguments[argLength - 1] == "function") {
    			error = arguments[argLength - 1];
    		}
    	}
    	
    	
        table.meta(function(meta) {
            var err = validateKey(meta.key);
            if(err) { 
            	if(error) error(err);
                return;
        	}
        	
            var data = {
                table: table.name(),
                key: key,
                property: property,
                value: value
            };
            
            operationManager[op](data, function(data) {
                if (success) {
                    success(snapshot(data));
                }
            }, error);
        });           	
    };
    
    /**
     *   @function {ItemRef} incr Increments a given attribute of an item. If the attribute doesn't exist, it is set to zero before the operation.
     *   @param {String} property The name of the item's attribute.
     *   @param {optional Number} value The number to add. Defaults to 1 if not specified.
     *   @param {function(ItemSnapshot itemSnapshot)} success The function invoked once the attribute has been incremented successfully.
     *   @... {ItemSnapshot} itemSnapshot An immutable copy of the incremented item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see ItemRef\incr.js
     */           
    this.incr = function(attribute, value, success, error) {
    	var args = [].slice.apply(arguments);
    	args.unshift("incr");
    	counter.apply(this, args);
    	return this;
    };
    
    /**
     *   @function {ItemRef} decr Decrements the value of an items attribute. If the attribute doesn't exist, it is set to zero before the operation.
     *   @param {String} property The name of the item's attribute.
     *   @param {optional Number} value The number to subtract. Defaults to 1 if not specified.
     *   @param {function(ItemSnapshot itemSnapshot)} success The function invoked once the attribute has been decremented successfully.
     *   @... {ItemSnapshot} itemSnapshot An immutable copy of the decremented item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see ItemRef\decr.js
     */          
    this.decr = function(attribute, value, success, error) {
    	var args = [].slice.apply(arguments);
    	args.unshift("decr");
    	counter.apply(this, args);
    	return this;
    };

    /**
     *   @function {ItemRef} setPresence Enables/Disables the presence feature. This operation requires the private key which should be assigned while declaring a storage reference.
     *   @param {Boolean} enabled Flag that enables or disables the presence feature.
     *   @param {optional Boolean} metadata Indicates if the connection metadata of a subscription is included in a presence request. Defaults to false.
     *   @param {function()} success Response from the server when the request was completed successfully.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     */
    this.setPresence = function(enabled) {
        var metadata, callback, error;

        if(enabled) {
            metadata = typeof arguments[1] != "boolean" ? false : arguments[1];            
        }

        var idx = typeof arguments[1] != "boolean" ? 1 : 2;

        if(typeof arguments[idx] == "function") {
            callback = arguments[idx];
            error = arguments[idx + 1] == "function" ? arguments[idx + 1] : function(err) { if(console) console.error(err); };
        }

        connection.setPresence(enabled, channel, metadata, function(err, result) { !err ? callback(result) : error(err); });

        return this;
    };

    /**
     *   @function {ItemRef} presence Retrieves the number of the item subscriptions and their respective connection metadata (limited to the first 100 subscriptions). Each subscriber is notified of changes made to the item.
     *   @param {function(PresenceInfo presenceInfo)} success Response from the server when the request was completed successfully.
     *   @... {Realtime.Storage.StorageRef.PresenceInfo} presenceInfo Information regarding the item subscriptions.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     */
    this.presence = function(callback, error) {
        connection.presence(channel,
            function(err, result) { 
                result ? callback(result) : (error ? error(err) : (function(err) { if(console) { console.error(err); } })(err));
            }
        );

        return this;
    };
};

module.exports = ItemRef;