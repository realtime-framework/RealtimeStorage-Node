var querystring = require('querystring');
var url = require('url');
/*
 *   @class  Realtime.Request Contains methods for performing HTTP and HTTPS requests.
 */
/*
 *   Performs a POST request.
 *   @param {Object} args Contains the arguments for this function.
 *   @... {String} url The host for the request.
 *   @... {String} path The path for the request.
 *   @... {String} port The port for the request.
 *   @... {Object} data The data to send through the request.
 *   @... {optional Object} headers The headers to send in the request. The name of the properties are the header name and the value the header value.
 *   @... {Function} callback A function that will be called after the request returns a response.
 *   @function {public void} Realtime.Request.post
 */
this.post = function(postData, serializer) {
    if(!postData.data) postData.data = {};
    
    var postContent = !serializer ? this.Serializers.queryString(postData.data) : serializer(postData.data);

    var host = postData.url;

    if(host){
        var parsedHost = url.parse(host);
        host = parsedHost.hostname ? parsedHost.hostname : host;
    }

    var postOptions = {
        'host': host,
        'port': postData.port,
        'path': postData.path,
        'method': 'POST'
    };

    if (postData.headers) {
        postOptions.headers = postData.headers;
    } else {
        postOptions.headers = {
            "content-type": "application/x-www-form-urlencoded"
        };
    }

    postOptions.headers["content-length"] = postContent ? Buffer.byteLength(postContent, 'utf8') : 0;
    
    var httpClient = postData.port == 443 ? require('https') : require('http');

    var request = httpClient.request(postOptions, function(response) {
        var statusCode = response.statusCode;
        var data = '';

        response.addListener('data', function(chunk) {
            data += chunk;
        });

        response.addListener('end', function() {
            if (statusCode >= 400 && statusCode <= 599) {
                postData.callback({
                    data: null,
                    error: {
                        code: statusCode,
                        content: new Buffer(data, 'binary').toString()
                    }
                });
            } else {

                var res = new Buffer(data, 'binary').toString();

                try {
                    res = JSON.parse(res);
                } catch (e) {
                    res = JSON.parse(data);
                }

                postData.callback(res);
            }
        });
    });

    request.on('error', function(error) {
        postData.callback({
            data: null,
            error: {
                message: error.toString()
            }
        }, null);
    });

    request.write(postContent);
    request.end();
};
/*
 *   Performs a GET request.
 *   @param {Object} args Contains the arguments for this function.
 *   @... {String} url The host for the request.
 *   @... {String} path The path for the request.
 *   @... {String} port The port for the request.
 *   @... {Object} data The data to send through the request.
 *   @... {optional Object} headers The headers to send in the request. The name of the properties are the header name and the value the header value.
 *   @... {Function} success A function that will be called after the request returns a response.
 *   @function {public void} Realtime.Request.get
 */
this.get = function(getData, serializer) {

	if(!getData.data) getData.data = {};


    var getParameters = !getData.data ? "" : (serializer ? serializer(getData.data) : this.Serializers.queryString);
    var requestPath = getParameters ? getData.path + '?' + getParameters : getData.path;
    
    var host = getData.url;

   if(host){
        var parsedHost = url.parse(host);
        host = parsedHost.hostname ? parsedHost.hostname : host;
    }

    var headers = {
        'host': host,
        'port': getData.port,
        'path': requestPath,
        "user-agent": "realtime-framework-serverside-api"
    };
    
    var httpClient = getData.port == 443 ? require('https') : require('http');

    var request = httpClient.get(headers, function(response) {
        var statusCode = response.statusCode;
        var data = '';

        response.addListener('data', function(chunk) {
            data += chunk;
        });

        response.addListener('end', function() {
            if (statusCode >= 400 && statusCode <= 599) {
                getData.callback({
                    error: {
                        code: statusCode,
                        message: new Buffer(data, 'binary').toString()
                    },
                    data: null
                });
            } else {
            	
                var res = new Buffer(data, 'binary').toString();

                try {
                    res = JSON.parse(res);
                } catch (e) {
                    res = JSON.parse(data);
                }
            	
                getData.callback(res);
            }
        });
    });

    request.on('error', function(error) {
        getData.callback({
            error: {
                code: null,
                message: error.toString()
            },
            data: null
        });
    });

    request.end();
};
/*
 *   @property {Object} Serializers Object with pre-defined functions (queryString, json) to serialize data to send in the requests.
 */
this.Serializers = {
    queryString: function(data) {
        return !data ? null : (typeof data == 'string' ? data : querystring.stringify(data));
    },
    json: function(data) {
        return !data ? null : (typeof data == 'string' ? data : JSON.stringify(data));
    }
};