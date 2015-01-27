var Messaging = require("./Messaging.js");
var Operation = require("./Operation.js");
var TableRef = require("./TableRef.js");
var TableSnapshot = require("./TableSnapshot.js");;
var Event = require("./Event.js");

/**
*   @class Realtime.Storage.StorageRef Class with the definition of a storage reference.
*/
var StorageRef = function (args, success, error) {	
	var Storage = require("./Storage.js");
	
    error = error ? error : function(err) {
    	if(Storage.debug && console && console.error) console.error(err);
    };
    
    var self = this;

    var operationManager = new Operation(args);

    var connection = this.connection = Messaging.ConnectionManager.create({
		url: args.isSecure ? "https://ortc-storage.realtime.co/server/ssl/2.1/" : "http://ortc-storage.realtime.co/server/2.1/",
        appKey: args.applicationKey,
        privateKey: args.privateKey,
        authToken: args.authenticationToken,
        connectAttempts: 100,
        autoConnect: false,
        metadata: args.metadata,
        onConnect: typeof args.onConnect == "function" ? function() {
        	args.onConnect.call(self, self);
        	// remove the error handler so it isn't called on an unrelated operation.                	
        	if(success) success(self);
        } : function() { if(success) success(self); },
        onDisconnect: typeof args.onDisconnect == "function" ? function() {
        	args.onDisconnect.call(self, self);
        } : undefined,
        onReconnect: typeof args.onReconnect == "function" ? function() {
        	args.onReconnect.call(self, self);
        } : undefined,
        onReconnecting: typeof args.onReconnecting == "function" ? function() {
        	args.onReconnecting.call(self, self);
        } : undefined,
        onException: typeof args.onException == "function" ? function(err) {
        	args.onException.call(self, self);
        	error(err);
        } : error
    });
    
    this.applicationKey = args.applicationKey;
    this.authenticationToken = args.authenticationToken;
    this.privateKey = args.privateKey;
    
    var meta = {
    		tables: {},
    		add: function(table) {    			
    			if(!meta.tables[table]) {
    				meta.tables[table] = { pending: true };
    				
                    operationManager.describeTable({
                    	applicationKey: this.applicationKey,
                		authenticationToken: this.authenticationToken,
                		privateKey: this.privateKey,
                		table: table
                    }, function(metadata) {                    	
                    	meta.tables[metadata.name].pending = false;
                    	meta.tables[metadata.name].metadata = metadata;

        				var event = {};
        				event["meta_" + table] = metadata;		            				
                    	meta.fire(event);                        	
                    }, function(error) {
        				var event = {};
        				event["meta_" + table] = { error: error };
                    	meta.fire(event);
                    });
    			}
    		},
    		
    		remove: function(table) {
    			if(meta.tables[table]) delete meta.tables[table];
    		},
    		
    		get: function(table, success, error) {
    			if(meta.tables[table] && !meta.tables[table].pending) success(meta.tables[table].metadata);
    			else {
    				var event = {};
    				event["meta_" + table] = function(data) {  				
    					if(data.error) {
    						if(error) error(data.error);
    						return;
    					}
    					if(success) success(data);        						
    				};
    				
    				meta.once(event);
    			}
				return undefined;
    		}
    };
	Event.extend(meta);
    
	// queues requests while checking if the token is authenticated
	var auth = (function() {
		var isAuthenticated;
		// queued 
		var ops = [];

		if(!args.privateKey) {	
			// authentication check
	        operationManager.isAuthenticated({
	            authenticationToken: args.authenticationToken
	        }, function(response) {
	        	isAuthenticated = response;
	        	if(isAuthenticated) {
                    authenticationDone();
	        	}
	        	else {
	        		ops = [];
	        		error("Token " + self.authenticationToken + " is not authenticated.");
	        	}
	        }, error);
		}
        // existence of a private key overrides authentication verification procedure
		else {
			isAuthenticated = true;
            if(args.authenticationToken) {
                authenticationDone();
            }
		}
		
        // establishes the ORTC connection and executes the waiting requests
        function authenticationDone() {
            connection.connect();
            for(var i = 0; i < ops.length; ++i) {
                check.apply(this, ops[i]);
            }
        };

        // receives the function and its arguments
        function check() {
			// check is done
			if(typeof isAuthenticated != "undefined") {
				if(isAuthenticated) {
					  var args = Array.prototype.slice.call(arguments);
					  var fn = args.shift();
					  fn.apply(fn, args);
				}
            	else {
            		error("Token " + self.authenticationToken + " is not authenticated.");
            	}
			}
			// waiting check to complete
			else {
				ops.push(Array.prototype.slice.call(arguments));
			}
        };
        
		return check;    			
	})();
	
    /**
     *   @function {TableRef} table Creates a new table reference.
     *   @param {String} name The table name.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return The new item reference
     *   @see StorageRef\table.js
     */
    this.table = function(name, error) {
        if (!name) {
        	if(error) error("Name of the table is mandatory");
            return null;
        }        
        
        auth(meta.add, name);
        
        return new TableRef({
            name: name,
            connection: connection,
            operationManager: operationManager,
            meta: function(success, error) {
            	auth(meta.get, name, success, error);
            }
        });
    };
    
    /**
     *   @function {StorageRef} authenticate Authenticate a token with the given permissions.
     *   @param {String} authenticationToken The token to authenticate.
     *   @param {Number} timeout The time (in seconds) that the token is valid.
     *   @param {optional Role[]} roles The list of roles assigned.
     *   @param {optional Policy[]} policies Additional policies particular to this token.
     *   @param {function(Boolean data)} success Function called when the operation completes successfully.
     *   @... {Boolean} data Indicates if the token was successfully authenticated.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This storage reference.
     *   @see StorageRef\authenticate.js
     */
     this.authenticate = function (args, success, error) {
         if (!args.authenticationToken) {
             if (error) {
                 error("The authenticationToken is missing.");
             }
             return this;
         }

         if (args.roles && !Array.isArray(args.roles)) {
             if (error) {
                 error("The roles property must be an Array with the name of the roles.");
             }
             return this;
         }

         if (!args.timeout || typeof args.timeout != "number") {
             if (error) {
                 error("Timeout must be a number.");
             }
             return this;
         }

         operationManager.authenticate({
             authenticationToken: args.authenticationToken,
             roles: args.roles,
             timeout: args.timeout,
             policies: typeof args.policies == "object" ? args.policies : undefined
         }, success, error);

         return this;
     };
    
    /**
     *   @function {StorageRef} isAuthenticated Verifies if the specified token is authenticated.
     *   @param {String} authenticationToken The token to verify.
     *   @param {function(Boolean data)} callback Function called when the operation completes successfully.
     *   @... {Boolean} data Indicates if the token is authenticated.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This storage reference.
     *   @see StorageRef\isAuthenticated.js
     */
    this.isAuthenticated = function(authenticationToken, success, error) {
        if (typeof authenticationToken != "string") {
            if (error) {
                error("Must specify an authentication token.");
            }
            return this;
        }

        operationManager.isAuthenticated({
            authenticationToken: authenticationToken
        }, success, error);

        return this;
    };
    
    /**
     *   @function {StorageRef} getTables Retrieves a list of the names of the tables created by the user’s application.
     *   @param {function(String[] tableNames)} success Function called when the operation completes successfully and for each existing table.
     *   @... {String[]} tableNames The names of the tables.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This storage reference.
     *   @see StorageRef\getTables.js
     */
    this.getTables = function(success, error, stopTable) {
        var that = this;
        operationManager.listTables({ startTable: stopTable }, function(res) {
            if (success) {
                var table;
                for (var i = 0; i < res.tables.length; i++) {
                    table = res.tables[i];
                    success(new TableSnapshot({
                        name: table,
                        connection: connection,
                        operationManager: operationManager,
                        meta: (function(name) { 
                            return function(success, error) {
                                auth(meta.get, name, success, error);
                            };
                        })(table)
                    }));
                }

                typeof res.stopTable != 'undefined' ?
                    that.getTables(success, error, res.stopTable) :
                    success(null);
            }
        }, error);
        return this;
    };

    /**
    *   @function {StorageRef} listRoles Retrieves a list of the names of the roles created by the user’s application.
    *   @param {function(String[] roleNames)} success Function called, for each existing role name, when the operation completes successfully.
    *   @... {String[]} roleNames The names of the roles.
    *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
    *   @... {String} message The message detailing the error.
    *   @return This storage reference.
    *   @see StorageRef\listRoles.js
    */
    this.listRoles = function (success, error) {
    	operationManager.listRoles({}, success, error);
    };
    
    /**
    *   @function {StorageRef} getRoles Retrieves the specified roles policies associated with the subscription.
    *   @param {String[]} roles The names of the roles to retrieve.
    *   @param {function(Role[] roles)} success Function called when the operation completes successfully and for each existing role.
    *   @... {Role[]} roles The retrieved roles.
    *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
    *   @... {String} message The message detailing the error.
    *   @return This storage reference.
    *   @see StorageRef\getRoles.js
    */
    this.getRoles = function (roles, success, error) {
        if (!roles) {
            if (error) {
                error("Must specify a list of roles.");
            }
            return this;
        }

        if (!Array.isArray(roles)) {
            if (error) {
                error("Roles must be an array.");
            }
            return this;
        }

        operationManager.getRoles({ roles: roles }, success, error);
    };
    
    /**
    *   @function {StorageRef} getRole Retrieves the policies that compose the role.
    *   @param {String} role The name of the role to retrieve.
    *   @param {function(Role role)} success Function called when the operation completes successfully.
    *   @... {Role} role The retrieved role.
	*   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
	*   @... {String} message The message detailing the error.
    *   @return This storage reference.
    *  @see StorageRef\getRole.js
    */
    this.getRole = function (role, success, error) {
        if (!role) {
            if (error) {
                error("Must specify the name of the role.");
            }
            return this;
        }
        
        operationManager.getRole({ role: role }, success, error);
    };
    
    /**
    *   @function {StorageRef} deleteRole Removes a role associated with the subscription.
    *   @param {String} role The name of the role to delete.
    *   @param {function(Boolean data)} success Function called when the operation completes successfully.
    *   @... {Boolean} data Indicates if the role was successfully deleted. If false, no role was found to delete.
    *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
    *   @... {String} message The message detailing the error.
    *   @return This storage reference.
    *  @see StorageRef\deleteRole.js
    */
    this.deleteRole = function (role, success, error) {
        if (!role) {
            if (error) {
                error("Must specify the name of the role.");
            }
            return this;
        }
        
        operationManager.deleteRole({ role: role }, success, error);
    };
    
    /**
    *   @function {StorageRef} setRole Stores a set of rules that control access to the Storage database.
    *   @param {String} role The name of the role to set.
    *   @param {Policy} policies An object with access rules.
    *   @param {function(Boolean data)} success Function called when the operation completes successfully.
    *   @... {Boolean} data Indicates if role was successfully inserted.
    *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
    *   @... {String} message The message detailing the error.
    *   @return This storage reference.
    *  @see StorageRef\setRole.js
    */
    this.setRole = function (role, policies, success, error) {
        if (!role) {
            if (error) {
                error("Must specify the name of the role.");
            }
            return this;
        }

        if (!policies) {
            if (error) {
                error("Must specify the policies of the role.");
            }
            return this;
        }

        if (typeof policies != "object") {
            if (error) {
                error("Policies must be an object.");
            }
            return this;
        }

        operationManager.setRole({
            role: role,
            policies: policies
        }, success, error);
    };
    
    /**
     *   @function {StorageRef} updateRole Modifies a set of existing rules that control access to the Storage database.
     *   @param {String} role The name of the role to set.
     *   @param {Policy} policies An object with access rules.
     *   @param {function(Boolean data)} success Function called when the operation completes successfully.
     *   @... {Boolean} data Indicates if role was successfully inserted.
     *   @param {optional function(String message)} error Response if client side validation failed or if an error was returned from the server.
     *   @... {String} message The message detailing the error.
     *   @return This storage reference.
     *  @see StorageRef\setRole.js
     */
     this.updateRole = function (role, policies, success, error) {
         if (!role) {
             if (error) {
                 error("Must specify the name of the role.");
             }
             return this;
         }

         if (!policies) {
             if (error) {
                 error("Must specify the policies of the role.");
             }
             return this;
         }

         if (typeof policies != "object") {
             if (error) {
                 error("Policies must be an object.");
             }
             return this;
         }

         operationManager.updateRole({
             role: role,
             policies: policies
         }, success, error);
     };

    // quick-fix...
	// no connection and is authenticated.
	if(!!args.authenticationToken && success) success(self);
	
	/**
	 * @object Realtime.Storage.StorageRef.Role Defines a role which is a set of rules that control access to the Storage database.
	 */
	/**
	 * @property {String} name The name of the role.
	 */
	/**
	 * @property {Policy} policies Contains the storage access rules.
	 */
	
	/**
	 * @object Realtime.Storage.StorageRef.Policy Defines a set of rules that control access to the Storage database.
	 */
	/**
	 * @property {Object} database Rules regarding the use of operations
	 */
	/**
	 * @property {Object} tables Rules regarding the access to tables and their keys.
	 */
    
    /**
     * @object Realtime.Storage.StorageRef.PresenceInfo Specification of the presence data structure.
     */
    /**
     * @property {read write Number}  subscriptions The number of subscriptions.
     */
    /**
     * @property {read write Object[]} metadata Information regarding the connection of each subscriber (limited to 100). Each object's key is the connection metadata and the value the number of subscribers with that same metadata.
     */
};
module.exports = StorageRef;