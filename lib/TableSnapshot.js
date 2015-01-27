/**
 *   @class Realtime.Storage.TableSnapshot Class with the definition of a table snapshot.
 *   @see TableSnapshot\tableSnapshot.js
 */
var TableSnapshot = function(args) {
	/**
	 *   @function {TableRef} ref Creates and returns the corresponding TableRef object.
	 *   @return A reference to the table.
	 *   @see TableSnapshot\ref.js
	 */
	this.ref = function() { return new TableRef(args); };
	
	/**
	 *   @function {String} val Returns the name of the table.
	 *   @return The attributes this snapshot refers to.
	 *   @see TableSnapshot\val.js
	 */
	this.val = function() { return args.name; };
};

module.exports = TableSnapshot;

var TableRef = require("./TableRef.js");