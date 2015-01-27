var Util = require("./Util.js");
var Event = require("./Event.js");
var Request = require("./Request.js");
var urlParser = require("url");
var ORTCClient = require('ibtrealtimesjnode').IbtRealTimeSJNode;

/**
 *	@namespace Realtime.Messaging Provides access to the Realtime Messaging functionalities.
 */
Event.extend(this);

/**
 *   @property {Boolean} isReady Tells if the Messaging is ready.
 */
this.isReady = true;

/**
 *	@function {public void} ready Call the function once the Messaging is ready.
 *	@param {Function} fn The function to call.
 */
this.ready = function(fn) {
    !this.isReady ? this.bind({
        ready: fn
    }) : fn();
};

/**
 *	@class Realtime.Messaging.Channel A logical path through which information is exchanged in an isolated way. Only users subscribing a certain channel will receive the information being broadcasted through it.
 */
var Channel = this.Channel = function Channel(args) {

    Event.extend(this);
    /**
     * @property {String} name Name of the channel.
     */
    this.name = args.name;
    /**
     * @property {Boolean} subscribeOnReconnect Indicates if the channels subscription is automatically made in case there's a reconnect. {@default true}
     */
    this.subscribeOnReconnect = typeof args.subscribeOnReconnect == "boolean" ? args.subscribeOnReconnect : true;
    /**
     *  @property {Boolean} subscribe Indicates if the channel is to be subscribed as soon as it's added to the connection. {@default true}
     */
    this.subscribe = typeof args.subscribe == "boolean" ? args.subscribe : true;
    /**
     *  @property {Function} messageAdapter Callback handler that allow changes to be made to the message. Called when a message arrives through the channel.
     */
    this.messageAdapter = args.messageAdapter;

    this.bind({
        /**
         *  @event evt_message Fired when a subscribed channel receives a message.
         *  @param {Object} e structure with the definition of the event's parameters.
         *  @... {String} message message that was received.
         */
        message: args.onMessage,
        /**
         *  @event evt_subscribe Fired when a channel subscribes to the connection. Starts listening for incoming messages.
         *  @param {Object} e structure with the definition of the event's parameters.
         *  @... {String} message message that was received.
         */
        subscribe: args.onSubscribe,
        /**
         *  @event evt_unsubscribe Fired when a channel unsubscribes from the connection. Stops listening for incoming messages.
         *  @param {Object} e structure with the definition of the event's parameters.
         *  @... {String} message message that was received.
         */
        unsubscribe: args.onUnsubscribe
    });
};
/**
 *   @class Realtime.Messaging.Connection Represents a connection to the Realtime server.
 */
var Connection = this.Connection = function Connection(args) {

    Event.extend(this);

    var client = new ORTCClient();

    /**
     *   @property {String} internalId Identification of the connection, generated automatically.
     */
    this.internalId = Util.guidGenerator();
    /**
     *   @property {String} id Identification of the connection assigned by the user.
     */
    this.id = args.id || this.internalId;
    /**
     *   @property {String} appKey Identifies the application using the Realtime API.
     */
    this.appKey = args.appKey;

    /**
     *   @property {String} appKey Identifies the application using the Realtime API.
     */
    this.privateKey = args.privateKey;

    /**
     *   @property {String} authToken Identifies a user belonging to the application using the Realtime API.
     */
    this.authToken = args.authToken;
    /**
     *   @property {Number} sendRetries Number of times a connection should try to send a message in case the first attempt fails. {@default 5}
     */
    this.sendRetries = parseInt(args.sendRetries) || 5;
    /**
     *   @property {Number} sendInterval Period of time, in miliseconds, between message send retries made by the connection. {@default 1000 ms}
     */
    this.sendInterval = parseInt(args.sendInterval) || 1000;
    /**
     *   @property {Number} timeout Period of time in miliseconds between connection attempts. {@default 10000ms}
     */
    this.timeout = parseInt(args.timeout) || 10000;
    /**
     *   @property {Number} connectAttempts Number of tries a connection should attempt to be established. {@default 5}
     */
    this.connectAttempts = args.connectAttempts || 5;
    /**
     *   @property {Number} connectionAttemptsCounter Number of connection attempts that have been made. Connection attemps stop after it reaches the value specified in connectionAttempts. {@default 0}
     */
    this.connectionAttemptsCounter = 0;
    /**
     *   @property {Boolean} autoConnect Indicates if a connection should be established implicitly after it's created. {@default true}
     */
    this.autoConnect = typeof args.autoConnect == 'boolean' ? args.autoConnect : true;
    /**
     *   @property {Object} metadata Provides information related with the connection itself.
     */
    this.metadata = args.metadata;
    /**
     *   @property {String} url Path to the location of the real-time comunication server is located.
     */
    this.url = args.url;
    /**
     *   @property {Boolean} isCluster Indicates if connection should be made to a cluster server. {@default true}
     */
    this.isCluster = typeof args.isCluster != 'undefined' ? args.isCluster : true;
    /**
     *   @property {String} announcementSubChannel The name of the announcement subchannel. Defining and subscribing to an announcement sub channel allows monitoring a single or set of users' ORTCs related activities.
     */
    this.announcementSubChannel = args.announcementSubChannel;
    /**
     *   @property {Function} messageAdapter Function that allows changes to be made to a message prior to it being processed by a tag. Called when a message arrives through any channel subscribed to this connection.
     */
    this.messageAdapter = args.messageAdapter;
    /**
     *  @property {Object} channels Contains all the existing channels in the connection.
     */
    this.channels = {};

    this.bind({
        /**
         *  @property {Function} onCreate Event handler raised when the connection is created.
         */
        /**
         *  @event evt_create Fired when the connection is created.
         *  @param {Object} e structure with the definition of the event's parameters.
         */
        create: args.onCreate,
        /**
         *  @property {Function} onConnect Event handler raised when the connection is established.
         */
        /**
         *  @event evt_connect Fired when the connection is established.
         *  @param {Object} e structure with the definition of the event's parameters.
         */
        connect: args.onConnect,
        /**
         *  @property {Function} onChannelCreate Event handler raised when a channel is added to the connection.
         */
        /**
         *  @event evt_channelCreate Fired when a channel is added to the connection.
         *  @param {Realtime.Messaging.Channel} channel structure with the channel's definition.
         */
        channelCreate: args.onChannelCreate,
        /**
         *  @property {Function} onDisconnect Event handler raised when there's a disconnection from the Realtime server.
         */
        /**
         *  @event evt_disconnect Fired when there's a disconnection from the Realtime server.
         *  @param {Object} e structure with the definition of the event's parameters.
         */
        disconnect: args.onDisconnect,
        /**
         *  @property {Function} onSubscribe Event handler raised when the connection subscribes a channel.
         */
        /**
         *  @event evt_subscribe Fired when the connection has subscribed to a channel.
         *  @param {Object} e structure with the definition of the event's parameters.
         *  @... {String} channel name of the subscribed channel.
         */
        subscribe: args.onSubscribe,
        /**
         *  @property {Function} onUnsubscribe Event handler raised when the connection unsubscribes a channel.
         */
        /**
         *  @event evt_unsubscribe Fired when the connection has unsubscribed a channel.
         *  @param {Object} e structure with the definition of the event's parameters.
         *  @... {String} channel name of the unsubscribed channel.
         */
        unsubscribe: args.onUnsubscribe,
        /**
         *  @property {Function} onException Event handler raised when an Realtime related exception has occurred.
         */
        /**
         *  @event evt_exception Fired when an Realtime related exception has occurred.
         *  @param {Object} e structure with the definition of the event's parameters.
         *  @... {String} message description of the raised exception.
         */
        exception: args.onException,
        /**
         *  @property {Function} onReconnect Event handler raised when a connection to an Realtime server is reestablished.
         */
        /**
         *  @event evt_reconnect Fired when a connection to an Realtime server is reestablished.
         *  @param {Object} e structure with the definition of the event's parameters.
         */
        reconnect: args.onReconnect,
        /**
         *  @property {Function} onReconnecting Event handler raised when a connection to an Realtime server is in the process of being reestablished.
         */
        /**
         *  @event evt_reconnecting Fired when a connection to an Realtime server is in the process of being reestablished.
         *  @param {Object} e structure with the definition of the event's parameters.
         */
        reconnecting: args.onReconnecting,
        /**
         *  @property {Function} onMessage Event handler raised when a connection receives a message through a subscribed channel.
         */
        /**
         *  @event evt_message Fired when a connection receives a message through a subscribed channel.
         *  @param {Object} e structure with the definition of the event's parameters.
         *  @... {String} channel name of the subscribed channel from where the message was received.
         *  @... {String} message message that was received.
         */
        message: args.onMessage,
        /**
         *  @property {Function} onDispose Event handler raised when a connection is disposed.
         */
        /**
         *  @event evt_dispose Fired when a connection is disposed.
         *  @param {Object} e structure with the definition of the event's parameters.
         */
        dispose: args.onDispose
    });

    /**
     *  @function {void} dispose Disconnects and removes references to this Connection.
     */
    this.dispose = function() {
        if (this.isConnected()) {
            this.disconnect();
        }
        this.fire({
            dispose: {}
        });
    };
    /**
     *  @function {Channel} createChannel Adds, but doesn't subscribe, a channel to the connection.
     *  @param {Channel} c The channel to be added.
     *  @returns The created channel.
     */
    this.createChannel = function(channelArgs) {

        if (!channelArgs) {
            this.fire({
                exception: {
                    message: "No channel to create."
                }
            });
            return null;
        }

        if (this.channels[channelArgs.name]) {
            return this.channels[channelArgs.name];
        }
        var channel = new Channel(channelArgs);
        this.channels[channel.name] = channel;
        this.fire({
            channelCreate: {
                channel: channel
            }
        });

        return channel;
    };

    var processConnection = function(message, channel) {
        var callback = function(messageAdapted) {

            if (!messageAdapted) {
                this.fire({
                    exception: "Caught an exception at connection message adapter. Cause: Callback called with no adapted message passed."
                });
                return;
            }

            this.fire({
                message: {
                    channel: channel,
                    message: messageAdapted
                }
            });

            if (this.channels[channel]) {
                process.nextTick(function() {
                    processChannel(this.channels[channel], messageAdapted);
                }.bind(this));
            }

        }.bind(this);

        if (this.messageAdapter) {
            process.nextTick(this.messageAdapter.bind(this, message, callback));
        } else {
            callback(message);
        }
    }.bind(this);

    var processChannel = function(channel, message) {
        var callback = function(channelMessageAdapted) {
            if (!channelMessageAdapted) {
                this.fire({
                    exception: "Caught an exception at channel message adapter. Cause: Callback called with no adapted message passed."
                });
                return;
            }

            channel.fire({
                message: {
                    message: channelMessageAdapted
                }
            });

        }.bind(this);

        if (channel.messageAdapter) {
            process.nextTick(function() {
                channel.messageAdapter(message, callback);
            });
        } else {
            callback(message);
        }
    }.bind(this);

    var processMessage = function(args) {
        processConnection(args.content, args.channel);
    }.bind(this);

    var retrySend = function(args, retries) {
        if (this.isCreated() && this.isConnected()) {
            this.send(args);
        } 
        else {
            if (++retries <= this.sendRetries) {
                setTimeout(function() {
                    retrySend(args, retries);
                }, this.sendInterval);
            }
        }
    }.bind(this);
    /**
     *   @function {void} send Transmits a message through a channel.
     *   @param {Object} args structure with the message attributes.
     *   @... {String} channel Name of the channel through which we're sending the message.
     *   @... {Object} content The message to be sent through the channel.
     *   @... {Boolean} sendOnly Identifies if the message should be sent and discarded by the connection that sends it.
     */
    this.send = function(args) {

        var channel = args.channel;
        var content = args.content;

        if (typeof content == "object") {
            content = content.stringify ? content.stringify() : JSON.stringify(content);
        }

        if (this.isCreated() && this.isConnected()) {
            client.send(channel, content.toString());
        } 
        else {
            setTimeout(function() {
                retrySend(args, 0);
            }, this.sendInterval);
        }
    };
    /**
     *   @function {void} connect Establishes the connection to the ORTC server.
     *   @param {Object} credentials structure with the credentials' attributes.
     *   @... {String} appKey ORTC's application key
     *   @... {String} authToken ORTC's authentication key. Identifies a user using the application.
     */
    this.connect = function(credentials) {
        if (this.isCreated()) {
            if (credentials) {
                this.appKey = credentials.appKey;
                this.authToken = credentials.authToken;
            }
            client.connect(this.appKey, this.authToken);
        }
    };
    /**
     *  @function {void} disconnect Closes the connection to the ORTC server.
     */
    this.disconnect = function() {
        if (this.isConnected()) {
            client.disconnect();
        } else {
            this.fire({
                exception: {
                    message: "Already disconnected."
                }
            });
        }
    };
    /**
     *  @function {void} subscribe Adds and subscribes a channel to the connection.
     *  @param {Channel} channel Channel the connection is going to subscribe.
     */
    this.subscribe = function(c) {
        if (!this.channels[c.name]) {
            c = this.createChannel(c);
        }

        client.subscribe(c.name, c.subscribeOnReconnect, function(ortc, channel, message) {
            processMessage({
                channel: channel,
                content: message
            });
        }.bind(this));
    };
    /**
     *  @function {void} unsubscribe Unsubscribes a channel from the connection.
     *  @param {String} name Name of the channel.
     */
    this.unsubscribe = function(name) {
        client.unsubscribe(name);
    };
    /**
     *  @function {Boolean} isCreated Checks if the connection is initialized.
     *  @returns A boolean stating if the connection is initialized.
     */
    this.isCreated = function() {
        return client != null;
    };
    /**
     *  @function {Boolean} isConnected Checks if the connection to the ORTC server is established.
     *  @returns A boolean stating if the connection is established.
     */
    this.isConnected = function() {
        return this.isCreated() ? (client.isConnected ? client.isConnected() : client.getIsConnected()) : false;
    };
    /**
     *  @function {Boolean} isSubscribed Checks if the connection has subscribed the channel.
     *  @param {String} channel Name of the channel.
     *  @returns A boolean stating if the channel is subscribed.
     */
    this.isSubscribed = function(name) {
        return this.isConnected() ? client.isSubscribed(name) : false;
    };
    /**
     *  @function {Boolean} getMetadata Gets the information related to the connection.
     *  @returns The information related to the connection
     */
    this.getMetadata = function() {
        if (this.isCreated()) {
            try {
                return JSON.parse(client.getConnectionMetadata());
            } catch (e) {
                return client.getConnectionMetadata();
            }
        }
        return null;
    };
    /**
     *  @function {void} setMetadata Associates information about the connection. The metadata is only set before the connection is established and updated after a reconnect.
     *  @param {String} metadata Information to store.
     */
    this.setMetadata = function(metadata) {
        if (this.isCreated()) {
            client.setConnectionMetadata(typeof metadata == 'object' ? JSON.stringify(metadata) : metadata);
            this.metadata = metadata;
        }
    };
    /**
     *  @function {String} getAnnouncementSubChannel Gets the client announcement subchannel.
     *  @returns The name of announcement subchannel
     */
    this.getAnnouncementSubChannel = function() {
        return this.isCreated() ? client.getAnnouncementSubChannel() : null;
    };
    /**
     *  @function {void} getAnnouncementSubChannel Sets the client announcement subchannel.
     *  @returns The name of announcement subchannel
     */
    this.setAnnouncementSubChannel = function(name) {
        if (this.isCreated()) {
            client.setAnnouncementSubChannel(name);
        }
    };

    if (args.channels) {
        var chs = args.channels;
        for (var i = 0; i < chs.length; ++i) {
            this.createChannel(chs[i]);
        }
    }

    this.setPresence = function(presence, channel, metadata, callback) {
        var args = { 
            channel: channel,
            metadata: metadata,
            privateKey: this.privateKey
        };

        if(!this.isCreated() || !this.isConnected()) {
            args.applicationKey = this.appKey;
            args.url = this.url;
            args.isCluster = this.isCluster;
        }

        presence ?
            client.enablePresence(args, callback) :
            client.disablePresence(args, callback);
    };

    this.presence = function(channel, callback) {
        var args = { channel: channel };

        if(!this.isCreated() || !this.isConnected()) {
            args.applicationKey = this.appKey;
            args.authenticationToken = this.authToken;
            args.isCluster = this.isCluster;
            args.url = this.url;                        
        }

        ortcClient.presence(args, callback);
    };

    client.setId(this.internalId);

    client.setConnectionTimeout(this.timeout);

    client.setConnectionMetadata(typeof this.metadata == 'object' ? JSON.stringify(this.metadata) : this.metadata);

    client.setAnnouncementSubChannel(this.announcementSubChannel);

    !this.isCluster ? client.setUrl(this.url) : client.setClusterUrl(this.url);

    client.onException = function(ortcClient, exception) {
        this.fire({
            exception: {
                message: exception
            }
        });
    }.bind(this);

    client.onConnected = function(ortcClient) {
        for (var ch in this.channels) {
            process.nextTick(function(channel) {
                if (channel.subscribe) {
                    client.subscribe(channel.name, channel.subscribeOnReconnect, function(ortc, channel, message) {
                        processMessage({
                            channel: channel,
                            content: message
                        });
                    }.bind(this));
                }
            }.bind(this, this.channels[ch]));
        }
        this.fire({
            connect: {}
        });
    }.bind(this);

    client.onDisconnected = function(ortcClient) {
        this.fire({
            disconnect: {}
        });
    }.bind(this);

    client.onSubscribed = function(ortcClient, channel) {
        this.channels[channel].fire({
            subscribe: {}
        });
        this.fire({
            subscribe: {
                channel: channel
            }
        });
    }.bind(this);

    client.onUnsubscribed = function(ortcClient, channel) {
        var removedChannel = this.channels[channel];
        delete this.channels[channel];
        removedChannel.fire({
            unsubscribe: {}
        });
        this.fire({
            unsubscribe: {
                channel: channel
            }
        });
    }.bind(this);

    client.onReconnected = function(ortcClient) {
        this.connectionAttemptsCounter = 0;
        this.fire({
            reconnect: {}
        });
    }.bind(this);

    client.onReconnecting = function(ortcClient) {
        if (this.connectionAttemptsCounter >= this.connectAttempts) {
            client.disconnect();
        } else {
            this.connectionAttemptsCounter++;
            this.fire({
                reconnecting: {}
            });
        }
    }.bind(this);

    process.nextTick(function() {
        this.fire({
            create: {}
        });

        if (this.autoConnect) {
            client.connect(this.appKey, this.authToken);
        }
    }.bind(this));
};
/**
 *   @class Realtime.Messaging.ConnectionManager Provides access to a data layer that manages the creation and dispose of connections, transmission and retrieval of information between the connections and the Realtime Framework servers.
 */
var ConnectionManager = function ConnectionManager() {

    var self = this;

    Event.extend(this);

    /**
     *   @property {Connection[]} Connections The array where the connections ids are stored.
     */
    this.Connections = [];

    var connections = {};

    var getById = function(id) {
        for (var key in connections) {
            if (connections[key].id === id) {
                return connections[key];
            }
        }
        return undefined;
    };

    var messageBuffer = {
        add: function(args) {
            var id = args.connection.id;
            if (!this.bConnections[id]) {
                this.bConnections[id] = [];
                args.connection.bind({
                    connect: this.send
                });
            }
            this.bConnections[id].push(args.message);
        },
        bConnections: {},
        send: function(e) {
            var messages = messageBuffer.bConnections[this.id];
            var con = this;

            for (var i = 0; i < messages.length; ++i) {
                con.send(messages[i]);
            }

            con.unbind({
                connect: messageBuffer.send
            });
            messageBuffer.bConnections[this.id] = undefined;
        }
    };
    /**
     *   @function {public Connection} create Creates a new connection.
     *   @param {Object} connection structure with the connection attributes.
     *   @... {optional String} id Connection's identification.
     *   @... {optional String} appKey Identifies the application using the Realtime API. Optional only if attribute 'autoConnect' is set to false.
     *   @... {optional String} authToken Identifies a user belonging to the application using the Realtime API. Optional only if attribute 'autoConnect' is set to false.
     *   @... {optional Number} sendRetries Number of times a connection should try to send a message in case the first attempt fails.
     *   @... {optional Number} sendInterval Period of time in miliseconds between message send retries by the connection.
     *   @... {optional Number} timeout Maximum amount of time (miliseconds) a connection tag should take to perform a connect.
     *   @... {optional Number} connectAttempts Number of times a connection should try to issue a connect.
     *   @... {optional Boolean} autoConnect Indicates if a connection should be established implicitly after it's created. Defaults to true.
     *   @... {optional String} metadata Provides information about one or more aspects of the data associated with the connection.
     *   @... {optional String} url Path to the location of the real-time comunication server is located. Optional if attribute 'autoConnect' is set to false.
     *   @... {optional Boolean} isCluster Indicates if connection should be made to a cluster server. Default is true.
     *   @... {optional Channel[]} channels Array of channels to be added to the connection.
     *   @... {optional Function} onCreate Event handler raised when the connection is created.
     *   @... {optional Function} onConnect Event handler raised when a connection is successfully established.
     *   @... {optional Function} onDisconnect Event handler raised when the connection has lost comunication with the Online Realtime Communication (Realtime) server.
     *   @... {optional Function} onSubscribe Event handler raised when a channel is subscribed.
     *   @... {optional Function} onUnsubscribe Event handler raised when a channel is unsubscribed.
     *   @... {optional Function} onException Event handler raised when there's an error performing Realtime Messaging Connection related operations.
     *   @... {optional Function} onReconnect Event handler raised when there's a reconnection to the Realtime servers.
     *   @... {optional Function} onReconnecting Event handler raised when a reconnection to the Realtime servers is under way.
     *   @... {optional Function} onMessage Event handler raised when a message arrives through any channel subscribed in this connection.
     *   @... {optional Function} messageAdapter Callback method that allow changes to be made to the message. Called when a message arrives through any channel subscribed in this connection.
     *   @returns The newly created xRTML Connection.
     *   @see ConnectionManager\create.js
     */
    this.create = function(connection) {
        return this.add(new Connection(connection));
    };
    /**
     *   @function {public Realtime.Messaging.Connection} add Adds a unique Realtime Connection to the Realtime Messaging platform.
     *   @param {Realtime.Messaging.Connection} connection Realtime Connection object.
     *   @returns The added Realtime Connection.
     */
    this.add = function(connection) {

        if (this.getById(connection.id)) {
            return null;
        }

        this.Connections.push(connection.id);
        connections[connection.internalId] = connection;

        this.fire({
            create: {
                connection: connection
            }
        });

        connection.bind({
            message: function(e) {
                self.fire({
                    message: {
                        connection: this,
                        channel: e.channel,
                        message: e.message
                    }
                });
            }
        });

        connection.once({
            dispose: function(e) {

                var connectionInternalId = this.internalId;
                var disposedConnection = connections[connectionInternalId];

                connections[connectionInternalId] = null;
                delete connections[connectionInternalId];

                for (var i = 0, cons = this.Connections, len = cons.length; i < len; i++) {
                    if (cons[i] === this.id) {
                        cons.splice(i, 1);
                        break;
                    }
                }

                self.fire({
                    dispose: {
                        connection: this
                    }
                });
            }
        });

        return connection;
    };
    
    /**
     *   @function {public Realtime.Messaging.Connection} getById Gets a connection by its internal or user given id.
     *   @param {String} id The id of the intended connection.
     *   @returns The connection with the given id.
     */
    this.getById = function(id) {
        return connections[id] || getById(id);
    };
    
    /**
     *   @function {public void} dispose Removes all references of the specified Connection.
     *   @param {String} id The id of the intended connection.
     */
    this.dispose = function(id) {
        var connection = this.getById(id);

        if (connection) {
            connection.dispose();
        }

        return !!connection;
    };
    
    /**
     *   @function {public void} addChannel Adds a channel to a connection.
     *   @param {Object} channel structure with the channel definition.
     *   @... {String} name The name of the channel.
     *   @... {optional Boolean} subscribeOnReconnect Defines if the channel is to be subscribed after a reconnect.
     *   @... {optional Boolean} subscribe Defines if the channel is to be subscribed as soon as the connection is established.
     *   @... {optional Function} messageAdapter Callback method that allow changes to be made to the message. Called when a message arrives through any channel subscribed in this connection.
     *   @... {optional Function} onMessage Event handler raised when a message arrives through the channel.
     *   @... {optional Function} onSubscribe Event handler raised when the channel is subscribed.
     *   @... {optional Function} onUnsubscribe Event handler raised when the channel is unsubscribed.
     */
    this.addChannel = function(channel) {
        var connection = this.getById(channel.connectionId);

        if ( !! connection) {
            connection.createChannel(channel);
        }

        return !!connection;
    };
    
    /**
     *   @function {public void} sendMessage Sends a message through the specified connections and channel.
     *   @param {Object} message structure with the definition of the message.
     *   @... {String[]} connections Ids of the connections.
     *   @... {String} channel Name of the channel.
     *   @... {Object} content Message to be sent.
     *   @... {optional Boolean} sendOnly Identifies if the message should be sent and discarded by the connection that sends it.
     *   @see ConnectionManager\sendMessage.js
     */
    this.sendMessage = function(message) {
        var connection;

        for (var i = 0; i < message.connections.length; ++i) {

            connection = this.getById(message.connections[i]);

            if (connection) {
                if (connection.isConnected()) {
                    connection.send(message);
                } else {
                    messageBuffer.add({
                        connection: connection,
                        message: {
                            channel: message.channel,
                            content: message.content,
                            sendOnly: message.sendOnly
                        }
                    });
                }
            }
        }
    };
    
    /**
     *   @function {void} saveAuthentication Saves authentication for the specified token with the specified authentication key.
     *   @param {Object} args structure with the arguments to perform the authentication.
     *   @... {String} url The url of the server.
     *   @... {Boolean} isCluster Flag that indicates if the server is cluster. {@default true}
     *   @... {String} appKey The application key.
     *   @... {String} authToken The authentication token.
     *   @... {String} privateKey The private key
     *   @... {Number} timeToLive The time to live in seconds.
     *   @... {Object} permissions The permissions for the channels. The channel name should be the property name and the permission ('r', 'W' and 'p') should be the property value. 'r' stands for read, 'w' for write and 'p' for presence service access.
     *   @... {Boolean} isPrivateToken Flag that indicates if the authentication token is private. If its private, only one connection can use the token (first connection that tries to connect).
     *   @... {Function} callback The callback to execute after the authentication. The callback will execute with two arguments: error and success. If the authentication succeeds the error is null.
     */
    this.saveAuthentication = function(args) {
        client.saveAuthentication(
            args.url,
            args.isCluster,
            args.authToken,
            args.isPrivateToken,
            args.appKey,
            args.timeToLive,
            args.privateKey,
            args.permissions,
            function() {
                args.callback({
                    error: arguments[0],
                    success: arguments[1]
                })
            }
        );
    };
    
    var sendRESTMessage = function(args) {
        var postArgs = {
            port: args.port ? args.port : (args.isHttps ? 443 : 80),
            path: "/send",
            parameters: {
                AT: args.authToken,
                AK: args.appKey,
                PK: args.privateKey,
                C: args.channel,
                M: args.message
            },
            callback: function(error, response) {
                if (typeof args.callback == "function") {
                    return args.callback({
                        error: error,
                        response: response
                    });
                }
            }
        };

        if (args.isCluster) {
            getServerFromCluster(args.host, args.appKey, function(error, serverUrl) {
                if (error == null) {
                    postArgs.host = serverUrl;
                    HttpRequest.post(postArgs, HttpRequest.Serializers.queryString);
                } else {
                    Console.error("Could not get server url from cluster.", "ConnectionManager - HTTP POST");
                }

            });
        } else {
            postArgs.host = args.host.replace("https://", "").replace("http://", "");
            HttpRequest.post(postArgs, HttpRequest.Serializers.queryString);
        }
    };
    
    /**
     *   @function {void} postMessage Sends a message through the specified connections and channel using a RESTful web service.
     *   @param {Object} args structure with the arguments to send a message.
     *   @... {String} host The url of the server.
     *   @... {Boolean} isCluster Flag that indicates if the server is cluster.
     *   @... {String} appKey The application key.
     *   @... {String} authToken The authentication token.
     *   @... {String} privateKey The private key.
     *   @... {String} channel The channel name to send the message.
     *   @... {String} message The message to send.
     *   @... {optional Function} callback The callback to execute after the post of the message. The callback will execute with an object as argument containing the properties error and response.
     */
    this.postMessage = function(args) {
        if (args.message.length <= 700) {
            sendRESTMessage(args);
        } else {

            var userCallback = args.callback;
            var fullMessage = args.message;
            var length = args.message.length;
            var numberOfParts = Math.ceil(args.message.length / 700);
            var part = 1;
            var guid = Util.idGenerator();

            var multiPartArguments = {
                host: args.host,
                isCluster: args.isCluster,
                appKey: args.appKey,
                authToken: args.authToken,
                privateKey: args.privateKey,
                channel: args.channel,
                callback: function(args) {

                    if ( !! args.error) {
                        if (typeof userCallback != "undefined") {
                            return userCallback(args);
                        }
                        return;
                    }

                    if (part == numberOfParts) {
                        // process callback
                        if (typeof userCallback != "undefined") {
                            return userCallback({
                                error: null,
                                response: {
                                    code: 201,
                                    content: 'Message " ' + args.message + '" sent to channel "' + args.channel + '"'
                                }
                            });
                        }
                        return;
                    }

                    // send nextMessage
                    part++;
                    var prefix = guid + "_" + part + "-" + numberOfParts + "_";
                    multiPartArguments.message = prefix + fullMessage.substring((part - 1) * 700, part * 700);
                    sendRESTMessage(multiPartArguments);

                }
            };

            var prefix = guid + "_" + part + "-" + numberOfParts + "_";
            multiPartArguments.message = prefix + fullMessage.substring((part - 1) * 700, part * 700);
            sendRESTMessage(multiPartArguments);

        }
    };

    var getServerFromCluster = function(url, appKey, callback) {
        var parsedUrl = urlParser.parse(url);

        Request.get({
            url: parsedUrl.host,
            path: parsedUrl.path,
            data: {
                "appkey": appKey
            },
            callback: function(error, responseData) {
                if (error != null) {
                    callback(error, null);
                } else {
                    var serverUrl = responseData.content.split('"')[1].replace("http://", "").replace("https://", "");
                    callback(null, serverUrl);
                }
            }
        }, Request.Serializers.queryString);
    };
};
this.ConnectionManager = new ConnectionManager();
