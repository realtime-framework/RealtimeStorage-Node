var ItemSnapshot = require("./ItemSnapshot.js");
var ItemRef = require("./ItemRef.js");
var Event = require("./Event.js");

/**
 *  @class Realtime.Storage.TableRef Class with the definition of a table reference.
 */
var TableRef = function(args) {
	
	var Storage = require("./Storage.js");
	
    var self = this;

    Event.extend(this);

    // "inherited" from a storage ref
    var connection = args.connection;
    var operationManager = args.operationManager;
    var meta = args.meta;
    
    // "inherited" from an operation
    var limit = args.limit || 0;
    var searchForward = typeof args.searchForward != "undefined" ? args.searchForward : true;
    var filters = args.filters || [];
    
    var name = args.name;
    
    /* Notification engine */
    
    var _eventEmitter = {};
    Event.extend(_eventEmitter);

    function getChannel(key) {
        var channel = "rtcs_" + name;
        if (key) {
            channel += ":" + key;
        }                
        return channel;
    };

    var subscribe = args.privateKey ? function (type, key, once, callback) {
        var evt = {};
        evt[channel + ":" + type] = callback;
        _eventEmitter[op](evt);
    } : function (type, key, once, callback) {
        var channel = "rtcs_" + name; //+ "_" + type;

        if (key) {
        	channel += ":" + key.primary;
            if (typeof key.secondary != "undefined") {
            	channel += "_" + key.secondary;
            }
        } 
        
        var op = once ? "once" : "bind";

        if (!connection.channels[channel]) {
            connection.isConnected() ? connection.subscribe({ name: channel }) : connection.createChannel({ name: channel });
            connection.channels[channel][op]({
                message: (function(ch) {
                	var channel = ch;
                	return function(e) {
                    var message = JSON.parse(e.message);
                   
                    meta(function(myMeta) {
                        var itemKey = {
                            primary: message[myMeta.key.primary.name]
                        };

                        if (myMeta.key.secondary) {
                            itemKey.secondary = message[myMeta.key.secondary.name];
                        }

                        var evt = {};
                        evt[channel + ":" + message.type] = new ItemSnapshot({
                            key: itemKey,
                            table: self,
                            connection: connection,
                            value: message.data
                        });
                        _eventEmitter.fire(evt);
                    });
                }
            	})(channel)
            });
        }

        var evt = {};
        evt[channel + ":" + type] = callback;
        _eventEmitter[op](evt);
    };
    
    var unsubscribe = args.privateKey ? function (type, key, callback) {
        if (!type) {
            _eventEmitter.unbindAll();
        } 
        else {
            var channel = "rtcs_" + name;

            if (key) {
            	channel += ":" + key.primary;
                if (typeof key.secondary != "undefined") {
                	channel += "_" + key.secondary;
                }
            } 
            var eventName = channel + ":" + type;
            
            if (typeof callback != "function") {
                _eventEmitter.unbindAll(eventName);
            } 
            else {
                var evt = {};
                evt[eventName] = callback;
                _eventEmitter.unbind(evt);
            }
        }   	
    } : function (type, key, callback) {
        if (!type) {
            for(var name in connection.channels) {
            	connection.unsubscribe(name);
            }
            _eventEmitter.unbindAll();
        } 
        else {
            var channel = "rtcs_" + name;

            if (key) {
            	channel += ":" + key.primary;
                if (typeof key.secondary != "undefined") {
                	channel += "_" + key.secondary;
                }
            }                	

        	if(connection.channels[channel]) {
        		connection.unsubscribe(channel);
        	}
        	
            var eventName = channel + ":" + type;
            
            if (typeof callback != "function") {
                _eventEmitter.unbindAll(eventName);
            } 
            else {
                var evt = {};
                evt[eventName] = callback;
                _eventEmitter.unbind(evt);
            }
        }
    };            
               
    /* end of Notification engine */
    
    // called by 'once' and 'on' and always in the object's context
    function attach() {
        var error = function(err) {
        	if(Storage.debug && console && console.error) console.error(err);
        };
        
        if(operationManager.privateKey && !operationManager.authenticationToken) {
        	throw new Error("Notifications are disabled when private key is being used.");
        }
        
    	if(arguments.length == 1 || arguments.length == 2 || arguments.length > 5) {
    		error("Invalid arguments. You must specify an event type and a success callback. The primary key is optional.");
    		return this;
    	}

        var once = arguments[0];
        var eventType = arguments[1];
        var primaryKey;
        var handler;
        
        // process and validate arguments
        if (eventType != "put" && eventType != "delete" && eventType != "update") {
            error("Invalid event type. Possible values: put, delete and update");
            return this;
        }
        
        // primary key
        if(typeof arguments[2] != "function") {
        	primaryKey = arguments[2];     
        	
            if (typeof primaryKey != "string" && typeof primaryKey != "number") {
                error("Invalid primary key. It must be a string or a number.");
                return this;
            }
                               
            if (typeof arguments[3] != "function") {
                error("Must specify a success function.");
                return this;
            }

            handler = arguments[3]; 
        	error = typeof arguments[4] == "function" ? arguments[4] : error;
        	
            subscribe(eventType, { primary: primaryKey }, once, handler);

            if (eventType == "put") {
                this.meta(function(meta) {
                	var key = meta.key;
                	
                    if (key.secondary) {                            	
                    	copy().equals({
                            item: key.primary.name,
                            value: primaryKey
                        }).getItems(handler,
                            function(e) {
                                error(e);
                                return self;
                            }
                        );                            	
                    } 
                    else {
                        self.item({ primary: primaryKey }).get(handler, error);
                    }
                });
            }
        }
        else {                    
        	handler = arguments[2];
        	error = typeof arguments[3] == "function" ? arguments[3] : error;  
        	
            if (eventType == "put") {
                self.getItems(handler, error);
            }
            
            if(filters.length != 0) {
                this.meta(function(meta) {
                	var key = meta.key;
                    var hasFilter = false;

                    for (var i = 0; i < filters.length && !hasFilter; i++) {
                        if (filters[i].item == key.primary.name && filters[i].operator == "equals") {
                            subscribe(eventType, { primary: filters[i].value }, once, handler);
                            hasFilter = true;
                        }
                    };

                    if (!hasFilter) {
                        subscribe(eventType, undefined, once, handler);
                    }

                });
            }
            else {
                subscribe(eventType, undefined, once, handler);                    	
            }
        }

        return this;
    }
    
    function copy(query) {
    	query = query || {};
    	return new TableRef({
            connection: connection,
            operationManager: operationManager,
            meta: meta,
            name: name,
    		limit: query.limit || limit,
    		searchForward: typeof query.searchForward != "undefined" ? query.searchForward : searchForward,
    		filters: query.filter ? (function() { var f = filters.concat(); f.push(query.filter); return f; })() : filters.concat()
        });
    };
    
    /**
     *   @function {String} name Returns the name of the table.
     *   @return The name of the referred table.
     *   @see TableRef\name.js
     */
    this.name = function() {
        return name;
    };
    
    /**
     *   @function {public Object[]} filters Returns a copy of the filters of the referred table.
     *   @return The filters applied to the table.
     *   @see TableRef\filters.js
     */         
    this.filters = function() {
    	return filters.concat();
    };
    
    /**
     *   @function {TableRef} limit Applies a limit over the number of items a search will return.
     *   @param {Number} value The limit to apply.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\limit.js
     */
    this.limit = function(value, error) {
        if (typeof value != "number" || value <= 0) {
            if (error) {
                error("The limit must be a number greater than zero.")
            }
            return this;
        }

        return copy({ limit: value });
    };
    
    /**
     *   @function {TableRef} asc Defines if the items will be retrieved in ascending order.
     *   @return This table reference.
     *   @see TableRef\asc.js
     */
    this.asc = function() {            	
    	return copy({ searchForward: true });
    };
    
    /**
     *   @function {TableRef} desc Defines if the items will be retrieved in descending order.
     *   @return This table reference.
     *   @see TableRef\desc.js
     */
    this.desc = function() {            	
    	return copy({ searchForward: false });
    };
    
    /**
     *   @function {TableRef} notNull Applies a filter that, upon retrieval, will have the items that have the selected property with a value other than null.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the item's property.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\notNull.js
     */
    this.notNull = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;                	
        }
        
        return copy({ 
        	filter: {
                operator: "notNull",
                item: filter.item
        	}
        });
    };
    
    /**
     *   @function {TableRef} isNull Applies a filter that, upon retrieval, will have the items that have the selected property with a null value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the item's property.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\null.js
     */
    this.isNull = function(filter, error) {
        if (typeof filter.item != "string") {

            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }
        
        return copy({
        	filter: {
                operator: "null",
                item: filter.item
            }
        });
    };
    
    
    /**
     *   @function {TableRef} equals Applies a filter to the table. When fetched, it will return the items that match the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {Object} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\equals.js
     */
    this.equals = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "equals",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} notEquals Applies a filter to the table. When fetched, it will return the items that does not match the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {Object} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\notEquals.js
     */
    this.notEquals = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "notEquals",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} greaterEqual Applies a filter to the table. When fetched, it will return the items greater or equal to filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {Object} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\greaterEqual.js
     */
    this.greaterEqual = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "greaterEqual",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} greaterThan Applies a filter to the table. When fetched, it will return the items greater than the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {Object} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\greaterThan.js
     */
    this.greaterThan = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "greaterThan",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} lesserEqual Applies a filter to the table. When fetched, it will return the items lesser or equals to the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {Object} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\lesserEqual.js
     */
    this.lesserEqual = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }
        
        return copy({ 
        	filter: {
                operator: "lessEqual",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} lesserThan Applies a filter to the table. When fetched, it will return the items lesser than the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {Object} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\lesserThan.js
     */
    this.lesserThan = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "lessThan",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} contains Applies a filter to the table. When fetched, it will return the items that contains the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {String} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\contains.js
     */
    this.contains = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "contains",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} notContains Applies a filter to the table. When fetched, it will return the items that does not contains the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {String} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\notContains.js
     */
    this.notContains = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "notContains",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} beginsWith Applies a filter to the table. When fetched, it will return the items that begins with the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {String} value The value of the property to filter.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\beginsWith.js
     */
    this.beginsWith = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (typeof filter.value == "undefined") {
            if (error) {
                error("Invalid filter. The property value must be defined.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "beginsWith",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} between Applies a filter to the table. When fetched, it will return the items in range of the filter property value.
     *   @param {Object} filter The structure with the filter arguments.
     *   @... {String} item The name of the property to filter.
     *   @... {Array} value The definition of the interval. Array of numbers (ex: [1, 5]).
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\between.js
     */
    this.between = function(filter, error) {
        if (typeof filter.item != "string") {
            if (error) {
                error("Invalid filter. The property item must be defined and be a string.");
            }
            return this;
        }

        if (!Array.isArray(filter.value) || (filter.value.length != 2)) {
            if (error) {
                error("Invalid filter. The property value must be defined and must be an array (length 2) of numbers.");
            }
            return this;
        }

        return copy({ 
        	filter: {
                operator: "between",
                item: filter.item,
                value: filter.value
            }
        });
    };
    
    /**
     *   @function {TableRef} meta Contains information about the table, including the current status of the table, the primary key schema and when the table was created.
     *   @param {function(Metadata metadata)} success Response from the server when the request was completed successfully.
	 *	 @... {Metadata} metadata Information regarding the table structure.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\meta.js
     */
    this.meta = function(success, error) {
		meta(success, error);
        return this;
    };
    
    /**
     *   @function {TableRef} del Delete this table.
     *   @param {function(Boolean data)} success Response from the server when the request was completed successfully.
     *   @... {Boolean} data Indicates if the table was deleted. If false, no table was found to be deleted.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return The reference to the deleted table.
     *   @see TableRef\del.js
     */
    this.del = function(success, error) {
        operationManager.deleteTable({
            table: this.name()
        }, function(res) {
            if (success) {
                success(res.data);
            }
        }, error);

        return this;
    };
    
    /**
     *   @function {TableRef} push Adds a new item to the table.
     *   @param {Object} item The item to add.
     *   @param {function(ItemSnapshot itemSnapshot)} success Response from the server when the request was completed successfully.
     *   @param {ItemSnapshot} itemSnapshot An immutable copy of the inserted item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\push.js
     */
    this.push = function(item, success, error) {
        if (typeof item != "object" || Object.keys(item).length == 0) {
            if (error) {
                error("Invalid item to push.");
            }
            return this;
        }

        var parameters = {
            table:name,
            item: item
        };

        meta(function(meta) {
        	var key = meta.key;
        	
        	if (!(key.primary.name in item)) {
                if (error) {
                    error("The item specified does not contain the primary key property.");
                }
                return self;
            }

            if (typeof item[key.primary.name] != key.primary.dataType) {
                if (error) {
                    error("The data type of the primary key does not match the schema of the table.");
                }
                return self;
            }

            if (key.secondary) {
            	if (!(key.secondary.name in item)) {
                    if (error) {
                        error("The item specified does not contain the secondary key property.");
                    }
                    return self;
                }

                if (typeof item[key.secondary.name] != key.secondary.dataType) {
                    if (error) {
                        error("The data type of the secondary key does not match the schema of the table.");
                    }
                    return self;
                }
                if (Object.keys(item).length <= 2) {
                    if (error) {
                        error("Cannot put an item that only contains key properties.");
                    }
                    return self;
                }
            } 
            else {
                if (Object.keys(item).length <= 1) {
                    if (error) {
                        error("Cannot put an item that only contains the key property.");
                    }
                    return self;
                }
            }

            operationManager.putItem(parameters, function(data) {
                if (success) {
                    success(new ItemSnapshot({
                        table: self,
                        key: {
                        	primary: item[key.primary.name],
                        	secondary: key.secondary ? item[key.secondary.name] : undefined
                        },
                        connection: connection,
                        value: data,
                        operationManager: operationManager
                    }));
                }

            }, error);

        }, error);

        return this;
    };
    
    /**
     *   @function {TableRef} create Creates a new table.
     *   @param {Object} args Structure with the table properties.
     *   @... {Number} provisionType The provision type id. Use the ProvisionType object.
     *   @... {Number} provisionLoad The provision load id. Use the ProvisionLoad object. Not mandatory when it is a table with custom provision type.
     *   @... {Key} key The definition of the key for this table. Must contain a primary property. The primary property is also an object that must contain a name and datatype. The table can have a secondary key ( { primary: { name: "id", dataType: "string" }, secondary: { name: "timestamp", dataType: "number" } }).
     *   @... {optional Throughput} throughput The custom provision to apply (ex: throughput: { read: 1, write: 1 }). Required when the provision type is Custom.
     *   @param {function(Metadata metadata)} success Response from the server when the request was completed successfully.
     *   @... {Metadata} metadata Information regarding the table structure.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\create.js
     */
    this.create = function(args, success, error) {
        if (!args.provisionType) {
            if (error) {
                error("Missing provision type property. Must specify the provision type properties. The provision values must be between 1 and 5. Use the constants in the Storage.ProvisionType object");
            }
            return this;
        }

        if (typeof args.provisionType != "number") {
            if (error) {
                error("Invalid provision. Must specify the provision type properties. The provision values must be between 1 and 5. Use the constants in the Storage.ProvisionType object");
            }
            return this;
        }

        if (!args.key) {
            if (error) {
                error("Missig argument key. The key schema must be specified");
            }
            return this;
        }

        if (typeof args.key.primary != "object") {
            if (error) {
                error("Missing primary key. The primary key property must be an object.");
            }
            return this;
        }

        if (typeof args.key.primary.name == "undefined" || typeof args.key.primary.dataType == "undefined") {
            if (error) {
                error("The primary key must contain a name and a data type (dataType) properties.");
            }
            return this;
        }

        if (args.key.primary.dataType != "string" && args.key.primary.dataType != "number") {
            if (error) {
                error('Invalid primary key dataType. Must be "number" or "string".');
            }
            return this;
        }

        var parameters = {
            table: name,
            provisionType: args.provisionType,
            key: {
                primary: args.key.primary
            }
        };

        if (args.key.secondary) {

            if (typeof args.key.secondary != "object") {
                if (error) {
                    error("The secondary key property must be an object.");
                }
                return this;
            }

            if (typeof args.key.secondary.name == "undefined" || typeof args.key.secondary.dataType == "undefined") {
                if (error) {
                    error("The secondary key must contain a name and a data type (dataType) properties.");
                }
                return this;
            }

            if (args.key.secondary.dataType != "string" && args.key.secondary.dataType != "number") {
                if (error) {
                    error('Invalid secondary key dataType. Must be "number" or "string".');
                }
                return this;
            }

            parameters.key.secondary = args.key.secondary;
        }

        // custom
        if (parameters.provisionType == 5) {
            // read and write throughput
            if (typeof args.throughput != "object") {
                if (error) {
                    error("Invalid throughput. For custom provision you must specify the throughput object with the read and write values. The values mean operations per second (ex: 2 reads per second and 3 writes per second).");
                }
                return this;
            }

            if (typeof args.throughput.read != "number") {
                if (error) {
                    error("Invalid read throughput. Must be a number");
                }
                return this;
            }

            if (typeof args.throughput.write != "number") {
                if (error) {
                    error("Invalid write throughput. Must be a number");
                }
                return this;
            }

            parameters.throughput = args.throughput;

        } else {

            if (!args.provisionLoad) {
                if (error) {
                    error("Must specify the provision load property. The provision values must be between 1 and 3. Use the constants in the Storage.ProvisionLoads object");
                }
                return this;
            }

            if (typeof args.provisionLoad != "number") {
                if (error) {
                    error("Invalid provision. Must specify the provision properties. The provision load value must be between 1 and 3. Use the constants in the Storage.ProvisionLoads object");
                }
                return this;
            }

            parameters.provisionLoad = args.provisionLoad;
        }

        operationManager.createTable(parameters, success, error);

        return this;
    };
    
    /**
     *   @function {TableRef} update Updates the provision type of the referenced table.
     *   @param {Object} args Structure with the table properties.
     *   @... {Number} provisionType The new provision type id. Use the ProvisionType object.
     *   @... {Number} provisionLoad The new provision load id. Use the ProvisionLoad object.
     *   @... {optional Throughput} throughput The custom provision to apply (ex: throughput: { read: 1, write: 1 }). Required when the provision type is Custom.
     *   @param {function(Metadata metadata)} success Response from the server when the request was completed successfully.
	 *	 @... {Metadata} metadata Information regarding the table structure.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\update.js
     */
    this.update = function(args, success, error) {
        var parameters = {
            table: name
        };

        if (!args.provisionType && !args.provisionLoad) {
            if (error) {
                error("Nothing to update. Must specify a provision load or a provision type. Use the constants in the Storage.ProvisionType and Storage.ProvisionLoad objects");
            }
            return this;
        }

        if (args.provisionType) {
            if (typeof args.provisionType != "number") {
                if (error) {
                    error("Invalid provisionType property. The provision values must be between 1 and 5. Use the constants in the Storage.ProvisionType object");
                }
                return this;
            }


            parameters.provisionType = args.provisionType;
        }

        if (args.provisionLoad) {
            if (typeof args.provisionLoad != "number") {
                if (error) {
                    error("Invalid provisionLoad property. The provision values must be between 1 and 3. Use the constants in the Storage.ProvisionLoad object");
                }
                return this;
            }

            parameters.provisionLoad = args.provisionLoad
        }

        if (args.provisionType == 5) {
            if (typeof args.throughput != "object") {
                if (error) {
                    error("Invalid throughput. For custom provision you must specify the throughput object with the read and write values. The values mean operations per second (ex: 2 reads per second and 3 writes per second).");
                }
                return this;
            }

            if (typeof args.throughput.read != "number") {
                if (error) {
                    error("Invalid read throughput. Must be a number");
                }
                return this;
            }

            if (typeof args.throughput.write != "number") {
                if (error) {
                    error("Invalid write throughput. Must be a number");
                }
                return this;
            }

            parameters.throughput = args.throughput;

        }

        operationManager.updateTable(parameters, function(data) {
            if (success) {
                success(data)
            }
        }, error);

        return this;
    };
    
    /**
     *   @function {ItemRef} item Creates a new item reference.
     *   @param {Object} key Structure with the key properties.
     *   @... {Object} primary The primary key. Must match the table schema.
     *   @... {Object} secondary The secondary key. Must match the table schema.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return The new item reference
     *   @see TableRef\item.js
     */
    this.item = function(key, error) {
        if (typeof key == "undefined"  || key === null) {
            if (error) {
                error("Invalid key argument. Must be an object containing the primary key and, if exists, the secondary key.");
            }
            return null;
        }
        if (typeof key.primary != "string" && typeof key.primary != "number") {
            if (error) {
                error("Invalid key object. Primary key missing");
            }
            return null;
        }
        return new ItemRef({
            key: key,
            table: this,
            connection: connection,
            operationManager: operationManager
        });
    };

    function callNextItem(item, success) {
        process.nextTick(function() {
            self.meta(function(meta) {
                if (success) {
                    success(item ? new ItemSnapshot({
                    	key: {
                        	primary: item[meta.key.primary.name],
                        	secondary: typeof meta.key.secondary != "undefined" ? item[meta.key.secondary.name] : undefined
                        },
                        table: self,
                        connection: connection,
                        value: item,
                        operationManager: operationManager
                    }) : null);
                }
            });
        });
    };
    
    /**
     *   @function {TableRef} getItems Get the items of this tableRef.
     *   @param {function(ItemSnapshot itemSnapshot)} success Response from the server when the request was completed successfully. This function will be called for each item retrieved and once more with 'null' value when all items have been processed.
     *   @... {ItemSnapshot} itemSnapshot An immutable copy of the retrieved item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This table reference.
     *   @see TableRef\getItems.js
     */
    this.getItems = function(success, error) {
        meta(function(meta) {
            var operation;
            var key = meta.key;

            var parameters = {
                table: name
            };

            if (key.secondary && filters.length == 1) {
                var filter = filters[0];

                if (filter.item == key.primary.name && filter.operator == "equals") {
                    parameters.key = {
                        primary: filter.value
                    };
                    operation = "queryItems";
                } else {
                    operation = "listItems";
                    parameters.filter = filters.concat();
                }
            } 
            else {
            	if (key.secondary && filters.length == 2) {
                    var props = {};

                    for (var i = 0; i < filters.length; i++) {
                        var filter = filters[i];

                        if (filter.item == key.primary.name && filter.operator == "equals") {
                            props.key = filter.value;
                        }

                        if (filter.item == key.secondary.name) {
                            props.filter = filter;
                        }
                    }

                    if (props.key && props.filter) {
                        operation = "queryItems";
                        parameters.key = {
                            primary: props.key
                        };
                        parameters.filter = props.filter;
                    } else {
                        operation = "listItems";
                        parameters.filter = filters.concat();
                    }
                } 
            	else {
                    operation = "listItems";

                    if (filters.length > 0) {
                        parameters.filter = filters.concat();
                    }
            	}
            }

            if (limit > 0) {
                parameters.limit = limit;
            }
            
            if (operation == "queryItems") {
                parameters.searchForward = searchForward;
            }

            var isCompleted = false;
            var itemsRetrieved = 0;
            var itemsDispatched = 0;

            var cb = function(response) {

                var resultArr = response.items;
                itemsRetrieved += resultArr.length;

                if (limit > 0) {

                    if (limit <= itemsRetrieved) {
                        isCompleted = true;
                    } else {
                        if (response.stopKey) {
                            parameters.startKey = response.stopKey;
                            operationManager[operation](parameters, cb, error);
                        } else {
                            isCompleted = true;
                        }
                    }

                } else {
                    if (response.stopKey) {
                        parameters.startKey = response.stopKey;
                        operationManager[operation](parameters, cb, error);
                    } else {
                        isCompleted = true;
                    }
                }

                if (operation != "queryItems") {

                    var KeyToOrder = key.secondary ? "secondary" : "primary";

                    if (key[KeyToOrder].dataType == "number") {
                        if (searchForward) {
                            resultArr.sort(function(a, b) {
                                return a[key[KeyToOrder].name] - b[key[KeyToOrder].name];
                            });
                        } else {
                            resultArr.sort(function(a, b) {
                                return -1 * (a[key[KeyToOrder].name] - b[key[KeyToOrder].name]);
                            });
                        }
                    } else {
                        if (searchForward) {
                            resultArr.sort(function(a, b) {
                                return a[key[KeyToOrder].name].localeCompare(b[key[KeyToOrder].name]);
                            });
                        } else {
                            resultArr.sort(function(a, b) {
                                return -1 * (a[key[KeyToOrder].name].localeCompare(b[key[KeyToOrder].name]));
                            });
                        }
                    }
                }

				if (limit > 0) {
				
				    for (var i = 0; i < resultArr.length && itemsDispatched < limit; i++) {
	                    callNextItem(resultArr[i], success);
				        itemsDispatched++;
				    }
				} else {
				    for (var i = 0; i < resultArr.length; i++) {
	                    callNextItem(resultArr[i], success);
				    }
				}
	
	            if (isCompleted && success) {
	            	callNextItem(null, success);
	            }
            };

            operationManager[operation](parameters, cb, error);

        }, error);
        
        return this;
    };
    
    /**
     *   @function {TableRef} on Attach a listener to run every time the eventType occurs.
     *   @param {String} eventType The type of the event to listen. Possible values: put, update, delete.
     *   @param {optional String} primaryKey The primary key of the item(s). Everytime a change occurs to the item(s), the handler is called.
     *   @param {function(ItemSnapshot itemSnapshot)} handler The function to run whenever the notification is received. If the event type is "put", it will immediately trigger a "getItems" to get the initial data and run the callback with each item snapshot as argument.
     *   @... {ItemSnapshot} itemSnapshot An immutable copy of the modified item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see TableRef\on.js
     */
    this.on = function() {
    	var args = [].slice.apply(arguments);
    	args.unshift(false);
    	attach.apply(this, args);
    };
    
    /**
     *   @function {TableRef} off Remove an event handler.
     *   @param {optional String} eventType The type of the event to remove. Possible values: put, update, delete. If not specified, it will remove all listeners of this reference.
     *   @param {optional String} primaryKey The primary key of the items to stop listen.
     *   @param {optional function(ItemSnapshot itemSnapshot)} handler A previously attached listener. If not specified, it will remove all listeners of the specified type or all listeners of this reference.
     *   @... {ItemSnapshot} itemSnapshot An immutable copy of the modified item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see TableRef\off.js
     */
    this.off = function() {
        var eventType = arguments.length != 0 ? arguments[0] : undefined;
        var primaryKey;
        var handler;
        var error = function(err) {
        	if(Storage.debug && console && console.error) console.error(err);
        };

        // process and validate arguments
        switch(arguments.length) {
        	case 4:
                primaryKey = arguments[1];
                handler = arguments[2];
            	error = typeof arguments[3] == "function" ? arguments[3] : error;
        	break;
        	case 3:
                if (typeof arguments[1] == "function") {
                	handler = arguments[1];
                	error = typeof arguments[2] == "function" ? arguments[2] : error;
                } 
                else {
                    primaryKey = arguments[1];
                    handler = arguments[2];
                }
        	break;
        	case 2:
        		handler = arguments[1];
        	break;
        }
        
        if (eventType) {
            if (eventType != "put" && eventType != "delete" && eventType != "update") {
                error("Invalid event type. Possible values: put, delete and update");
                return this;
            } 
            else {
                if (typeof primaryKey != "undefined") {
                    unsubscribe(eventType, { primary: primaryKey }, handler);
                } 
                else {
                	if(filters.length != 0) {
                        this.meta(function(meta) {
                        	var key = meta.key;
                            var hasFilter = false;

                            for (var i = 0; i < filters.length && !hasFilter; i++) {
                                if (filters[i].item == key.primary.name && filters[i].operator == "equals") {
                                    unsubscribe(eventType, { primary: filters[i].value }, handler);
                                    hasFilter = true;
                                }
                            };

                            if (!hasFilter) {
                                unsubscribe(eventType, undefined, handler);
                            }
                        });
                	}
                    else {
                    	unsubscribe(eventType, undefined, handler);                    	
                    }
                }
            }
        } 
        else {
            unsubscribe();
        }

        return this;
    };
    
    /**
     *   @function {TableRef} once Attach a listener to run only once when event type occurs.
     *   @param {String} eventType The type of the event to listen. Possible values: put, update, delete.
     *   @param {optional String} primaryKey The primary key of the item(s). When a change occurs to the item(s), the handler is called once.
     *   @param {function(ItemSnapshot itemSnapshot)} handler The function invoked, only once, when the notification is received. If the event type is "put", it will immediately trigger a "getItems" to get the initial data and run the callback with each item snapshot as argument.
     *   @... {ItemSnapshot} itemSnapshot An immutable copy of the modified item.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This item reference.
     *   @see TableRef\once.js
     */
    this.once = function() {
    	var args = [].slice.apply(arguments);
    	args.unshift(true);
    	attach.apply(this, args);
    };

    /**
     *   @function {TableRef} setPresence Enables/Disables the Messaging's presence feature. This operation requires the private key which should be assigned while declaring a storage reference.
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

        // enable the presence for the table's channel
        connection.setPresence(enabled, getChannel(), metadata, function(err, result) { 
            if(!err) {
                // enable the presence for the item's channels
                connection.setPresence(enabled, getChannel('*'), metadata, function(err, result) {
                    !err ? callback(result) : error(err);
                });
            } 
            else {
                error(err);
            }
        });

        return this;
    };


    /**
     *   @function {TableRef} presence Retrieves the number of the table subscriptions and their respective connection metadata (limited to the first 100 subscriptions). Each subscriber is notified of changes made to the table.
     *   @param {function(PresenceInfo presenceInfo)} success Response from the server when the request was completed successfully.
     *   @... {Realtime.Storage.StorageRef.PresenceInfo} presenceInfo Information regarding the table subscriptions.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     */
    this.presence = function(callback, error) {
        connection.presence(getChannel(), function(err, result) {
            result ? callback(result) : error ? error(err) : (function(err) { if(console) console.error(err); })(err);
        });

        return this;
    };
    
    /**
     * @object Realtime.Storage.TableRef.Key Specification of the key structure for table maintenance operations.
     */
    /**
     * @property {read write String} name The name of the key.
     */
    /**
     * @property {read write String} dataType Type of the key. The allowed types are String and Number.
     */
    
    /**
     * @object Realtime.Storage.TableRef.Throughput Specification of a table read and write operation capacity.
     */
    /**
     * @property {read write Number} read Read operations per second of a table.
     */
    /**
     * @property {read write Number} write Write operations per second of a table.
     */

    /**
    *   @object Realtime.Storage.TableRef.Metadata Specification of the metadata structure regarding the table schema.
    **/
    /**
    *   @property {String} applicationKey Public key of the application's license.
    **/
    /**
    *   @property {String} name The name of the table.
    **/
    /**
    *   @property {ProvisionType} provisionType The provision type set for the table.
    **/
    /**
    *   @property {ProvisionLoad} provisionLoad The provision load set for the table.
    **/
    /**
    *   @property {Throughput} throughput The read and write capacity units set for the table.
    **/
    /**
    *   @property {Number} creationDate The date the table was created.
    **/
    /**
    *   @property {Number} updateDate The date of the last change made to the table schema.
    **/
    /**
    *   @property {Boolean} isActive Control flag.
    **/
    /**
    *   @property {Key} key The key schema. Can be a single (primary) or composite (primary and secondary) key.
    **/
    /**
    *   @property {String} status The current status of the table. Possible values include: "Creating", "Updating", "Deleting", "Active".
    **/
    /**
    *   @property {Number} size The total size of the specified table, in bytes. Recent changes might not be reflected in this value.
    **/
    /**
    *   @property {Number} itemCount The number of items in the specified table. Recent changes might not be reflected in this value.
    **/
    
    /**
     * @end
     */
};


module.exports = TableRef;