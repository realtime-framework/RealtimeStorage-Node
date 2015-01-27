/**
 * @namespace Realtime.Storage Provides access to the Realtime Storage API.
 */
var Storage = {		
	/**
	 *   @property {Boolean} Realtime.Storage.debug If set, displays internal error messages.
	 */
	debug: false,

	/**
	 *   @function {public StorageRef} create Creates a new Storage reference.
	 *   @param {Object} args Structure with the necessary arguments to create a storage reference.
	 *   @... {String} applicationKey Public key of the application's license.
     *   @... {String} authenticationToken An authenticated token.
	 *   @... {optional Boolean} isSecure Defines if the requests and notifications are under a secure connection. Defaults to true.
	 *   @... {optional Boolean} isCluster Defines if the specified url is from a cluster server. Defaults to true.
	 *   @... {optional String} url The url of the storage server. Defaults to "storage.realtime.co".
	 *   @... {optional function()} onReconnect Function to run every time the notification system recovers from a disconnect.
	 *   @... {optional function()} onReconnecting Function to run every time the notification system tries to reconnect.
	 *   @param {optional function(StorageRef storageRef)} success Response from the server when the request was completed successfully.
	 *   @... {StorageRef} storageRef The resulting storage reference from the executed operation.
	 *   @param {optional function(String message) } error Response if client side validation failed or if an error was returned from the server.
	 *   @... {String} message The message detailing the error.
	 *   @see Storage\create.js
	 */
	create: function(args, success, error) {
	    if (!args.applicationKey) {
	        if (error) {
	            error("Cannot create a storage reference without specifying a valid application key.");
	        }
            return null;
	    }
	
	    if (!args.authenticationToken && !args.privateKey) {
	        if (error) {
	            error("Cannot create a storage reference without specifying an authentication token or a private key.");
	        }
            return null;
	    }
	    
	    return new Storage.StorageRef(args, success, error);
	},

	/**
	 * @object Realtime.Storage.ProvisionType Provides access to the available provision types.
	 */
	ProvisionType: {
	    /**
	     *   @property {Number} Light Id of the Light provision type (26 operations per second).
	     */
	    Light: 1,
	    /**
	     *   @property {Number} Medium Id of the Medium  provision type (50 operations per second).
	     */
	    Medium: 2,
	    /**
	     *   @property {Number} Intermediate  Id of the Intermediate provision type (100 operations per second).
	     */
	    Intermediate: 3,
	    /**
	     *   @property {Number} Heavy Id of the Heavy provision type (200 operations per second).
	     */
	    Heavy: 4,
	    /**
	     *   @property {Number} Custom Id of the Custom provision type (customized read and write throughput).
	     */
	    Custom: 5
	},

	/**
	 * @object Realtime.Storage.ProvisionLoad Provides access to the available provision load definitions.
	 */
	ProvisionLoad: {
	    /**
	     *   @property {Number} Read Id of the Read provision load (Assign more read capacity than write capacity).
	     */
	    Read: 1,
	    /**
	     *   @property {Number} Write Id of the Write provision load (Assign more write capacity than read capacity).
	     */
	    Write: 2,
	    /**
	     *   @property {Number} Balanced Id of the Balanced provision load (Assign similar read an write capacity).
	     */
	    Balanced: 3,
	    /**
	     *   @property {Number} Custom Id of the Custom provision load.
	     */            
	    Custom: 4
	},
	
    Operation: require("./Operation.js"),
    
    ItemSnapshot: require("./ItemSnapshot.js"),
    
    ItemRef: require("./ItemRef.js"),
    
    TableSnapshot: require("./TableSnapshot.js"),
    
    TableRef: require("./TableRef.js"),
    
    StorageRef: require("./StorageRef.js")	
};
module.exports = Storage;