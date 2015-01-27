var urlParser = require("url");
var Request = require("./Request.js");
var Event = require("./Event.js");

var headers = {
    "content-type": "application/json; charset=utf-8"
};

function complete(response, retry, success, error) {
    if (response.error) {
        if (error) {
            if (!retry && typeof response.error == "string" && response.error.match("unavailable") && response.error.match("status") && response.error.match("0") && response.error.match("server")) {
                error({
                    error: response.error,
                    retry: true
                });
            } else {
                if(response.error.code >= 500 && response.error.code < 600 && !response.error.message) {
                    response.error.message = "Server Error: " + response.error.code;
                }
                error({
                    error: response.error,
                    retry: false
                });
            }
        }
    } else {
        if (success) {
            success(response.data);
        }
    }
};

var Operation = function(args) {
    Event.extend(this);
    
    this.applicationKey = args.applicationKey;
    this.authenticationToken = args.authenticationToken;
    this.privateKey = args.privateKey;
    
    this.isSecure = typeof args.isSecure == "boolean" ? args.isSecure : false;
    this.isCluster = typeof args.isCluster == "boolean" ? args.isCluster : true;
    
    // if cluster, the url will the retrieved from it.
    var url = !this.isCluster ? args.url : undefined;

    // get the url from the cluster
    this.url = this.isCluster ? (function() {
    	var clusterUrl = args.url || "storage-balancer.realtime.co";
    	var waiting = false;
    	// operations waiting for the url from cluster
    	var pending = [];
    	
        // if it's a cluster url, then 'url' is not valid the 1st time around;
        url = undefined;

    	function getUrlFromCluster(success, error) {
    		if(url) { 
            	if(success) success(url);
    			return url; 
			}
    		else {
    			// request the url from cluster (1st time or a refresh)
    			if(!waiting) {
    				waiting = true;
                    Request.get({
                        url: clusterUrl,
                        path: args.isSecure ? "/server/ssl/1.0" : "/server/1.0",
                        port: args.isSecure ? 443 : 80,
                        crossDomain: true,
                        callback: function(response) {   
                            if (response.url) {                    				
                            	url = response.url;
                            	// process pending operations (in order)
                            	for(var i = 0; i < pending.length; ++i) {
                            		pending[i].success(url);
                            	}
                            } else {                            	
                            	var err = "Could not retrieve the server url from the cluster. " + (typeof response.error == "object"  ? response.error.message : response.error);
                            	// process pending operations (in order)
                            	for(var i = 0; i < pending.length; ++i) {
                            		pending[i].error(err);
                            	}
                            }
                            
                        	pending = [];
                        	waiting = false;
                        }
                    }, Request.Serializers.queryString);
    			}
    			
    			// url request is still pending queue operation (review this line...)
				pending.push({ success: success || function() {}, error: error || function() {} })
    		}
    	}
    	
    	// retrieve the url from cluster at Operation creation
    	getUrlFromCluster();
    	
    	return getUrlFromCluster;
    })() : function(success) {
    	if(success) success(url);
    	return url; 
	};
	
	this.refreshUrl = function(callback) {
		if(this.isCluster) url = undefined;
		this.url(callback);
	};
	
	// renew server
	setInterval(function() { this.refreshUrl(function(url) { }) }.bind(this), 5*60*1000);
};

Operation.prototype = {		
    isAuthenticated: function(data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/isAuthenticated",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.isAuthenticated(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    authenticate: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;        
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/authenticate",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.authenticate(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    incr: function(data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/incr",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.authenticate(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },

    decr: function(data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/decr",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.authenticate(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    listItems: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/listItems",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.listItems(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    queryItems: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/queryItems",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.queryItems(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    getItem: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/getItem",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.getItem(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    putItem: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/putItem",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.putItem(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    updateItem: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/updateItem",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.updateItem(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    deleteItem: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/deleteItem",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.deleteItem(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    createTable: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/createTable",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.createTable(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    updateTable: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/updateTable",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.updateTable(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    deleteTable: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/deleteTable",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.deleteTable(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    listTables: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/listTables",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.listTables(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    describeTable: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {        	
            Request.post({
                url: url,
                path: "/describeTable",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {                	
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.describeTable(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    listRoles: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/listRoles",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.listRoles(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    getRoles: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/getRoles",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.getRoles(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    getRole: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/getRole",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.getRole(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    setRole: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/setRole",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.setRole(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    updateRole: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/updateRole",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.updateRole(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    },
    
    deleteRole: function (data, success, error, retry) {
        var self = this;
        data.applicationKey = this.applicationKey;
        data.privateKey = this.privateKey;
        data.authenticationToken = this.authenticationToken;
        
        this.url(function(url) {
            Request.post({
                url: url,
                path: "/deleteRole",
                port: self.isSecure ? 443 : 80,
                data: data,
                headers: headers,
                callback: function(response) {
                    complete(response, retry, success, function(err) {
                        if (err.retry) {
                        	self.refreshUrl(function() {
                                self.deleteRole(data, success, error, true);
                            }, error);
                        } else {
                            if (error) {
                                error(err.error.message);
                            }
                        }
                    });
                }
            }, Request.Serializers.json);
        }, error);
    }
};

module.exports = Operation;
