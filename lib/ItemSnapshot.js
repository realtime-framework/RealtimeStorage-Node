/**
 *   @class Realtime.Storage.ItemSnapshot Class with the definition of an item snapshot.
 *   @see ItemSnapshot\itemSnapshot.js
 */
var ItemSnapshot = function(args) {
	/**
	 *   @function {ItemRef} ref Creates and returns the corresponding ItemRef object.
	 *   @return A reference to the item.
	 *   @see ItemSnapshot\ref.js
	 */
	this.ref = function() { return new ItemRef(args); };
	/**
	 *   @function {Object} val Returns the attributes of the item.
	 *   @return The attributes this snapshot refers to.
	 *   @see ItemSnapshot\val.js
	 */
	this.val = function() { return args.value; };
};

module.exports = ItemSnapshot;

var ItemRef = require("./ItemRef.js");